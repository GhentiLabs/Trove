// Package session drives a netio.Conn through the handshake state machine to an
// Active, keepalive-held, framed control channel and tears it down gracefully.
package session

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/netio"
	"github.com/GhentiLabs/Trove/client/internal/wire"
	"github.com/GhentiLabs/Trove/client/internal/wire/wirepb"
	"google.golang.org/protobuf/proto"
)

// DefaultKeepaliveInterval is how often an idle Active session sends a Ping.
const DefaultKeepaliveInterval = 15 * time.Second

const closeGraceTimeout = 2 * time.Second

// DefaultHandshakeTimeout bounds the pre-Active handshake.
const DefaultHandshakeTimeout = 15 * time.Second

var (
	// ErrVersionMismatch is returned when the peer speaks a different wire format.
	ErrVersionMismatch = errors.New("session: incompatible wire format version")
	// ErrUnauthorized is returned when the peer's node id is not authorized.
	ErrUnauthorized = errors.New("session: peer not authorized")
	// ErrFingerprintMismatch is returned when the peer's Hello node id does not match its certificate.
	ErrFingerprintMismatch = errors.New("session: hello node id does not match certificate")
	// ErrUnexpectedMessage is returned when a message arrives out of sequence.
	ErrUnexpectedMessage = errors.New("session: unexpected message")
)

// State is the connection lifecycle stage reached.
type State int32

const (
	StateHandshaking State = iota
	StateActive
	StateClosed
)

func (s State) String() string {
	switch s {
	case StateHandshaking:
		return "handshaking"
	case StateActive:
		return "active"
	case StateClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// Folder is a local folder offered to the peer in NetworkConfig.
type Folder struct {
	ShareID   string
	Encrypted bool
	// EncryptionVerifier is the non-secret key-mismatch token for an encrypted folder, empty
	// when this node holds no key for it yet.
	EncryptionVerifier []byte
	// Holder marks this node as serving the folder only as an untrusted holder (sealed blobs).
	Holder bool
}

// Local identifies this node for the Hello and the folders it offers.
type Local struct {
	NodeID        string
	ClientName    string
	ClientVersion string
	Folders       []Folder
}

// ControlHandler handles a post-Hello control message the session does not handle
// itself (anything beyond Ping and Close). Returning an error tears the session
// down; returning nil continues. Install it with SetControlHandler before Run.
type ControlHandler func(ctx context.Context, typ wire.MessageType, msg proto.Message) error

// Config drives a single Handshake.
type Config struct {
	Conn      netio.Conn
	Initiator bool
	Local     Local
	Authorize func(ctx context.Context, nodeID string) (granted []string, ok bool, err error)
	// ResponsiveOffer, when set, supplies the folders to offer a peer Authorize granted nothing,
	// computed from what the peer advertised (so a recovery peer can prove itself without this
	// node leaking its folder list). Consulted only on the responding side.
	ResponsiveOffer func(ctx context.Context, peerID string, peerOffered []Folder) ([]Folder, error)

	KeepaliveInterval time.Duration
	HandshakeTimeout  time.Duration
	Logger            *slog.Logger
}

// Session is an authenticated, Active, framed, multiplexed peer connection.
type Session struct {
	conn          netio.Conn
	ctrl          netio.Stream
	peerNodeID    string
	shared        []string
	peerVerifiers map[string][]byte
	peerHolders   map[string]bool
	keepalive     time.Duration
	started       time.Time
	log           *slog.Logger

	handler ControlHandler

	state   atomic.Int32
	closing atomic.Bool
	wmu     sync.Mutex
	once    sync.Once
	done    chan struct{}
}

// Handshake runs the state machine through Hello, authorization, and NetworkConfig
// to Active. A Hello is always exchanged before any rejection.
func Handshake(ctx context.Context, cfg Config) (*Session, error) {
	log := cfg.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}

	timeout := cfg.HandshakeTimeout
	if timeout <= 0 {
		timeout = DefaultHandshakeTimeout
	}
	hctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	stop := context.AfterFunc(hctx, func() { _ = cfg.Conn.Close() })
	defer stop()

	ctrl, err := openControl(hctx, cfg)
	if err != nil {
		_ = cfg.Conn.Close()
		return nil, err
	}

	peerHello, err := exchangeHello(cfg, ctrl)
	if err != nil {
		_ = cfg.Conn.Close()
		return nil, fmt.Errorf("session: hello: %w", err)
	}
	if v := peerHello.GetWireFormatVersion(); v != wire.WireFormatVersion {
		_ = cfg.Conn.Close()
		return nil, fmt.Errorf("%w: peer %d, ours %d", ErrVersionMismatch, v, wire.WireFormatVersion)
	}

	peerID := cfg.Conn.PeerNodeID()
	if peerHello.GetNodeId() != peerID {
		_ = cfg.Conn.Close()
		return nil, fmt.Errorf("%w: hello %q, cert %q", ErrFingerprintMismatch, peerHello.GetNodeId(), peerID)
	}

	authorize := cfg.Authorize
	if authorize == nil {
		authorize = func(context.Context, string) ([]string, bool, error) { return nil, false, nil }
	}
	granted, ok, err := authorize(ctx, peerID)
	if err != nil {
		_ = cfg.Conn.Close()
		return nil, fmt.Errorf("session: authorize: %w", err)
	}
	if !ok {
		_ = cfg.Conn.Close()
		return nil, fmt.Errorf("%w: %s", ErrUnauthorized, peerID)
	}

	offer := func(peer *wirepb.NetworkConfig) ([]Folder, error) {
		// A member (non-empty grant) or the initiator (peer == nil) offers from its own grant;
		// only a responder that granted nothing defers to the responsive offer.
		if len(granted) > 0 || cfg.ResponsiveOffer == nil || peer == nil {
			return offeredFolders(cfg.Local.Folders, granted), nil
		}
		return cfg.ResponsiveOffer(ctx, peerID, peerOfferedFolders(peer))
	}
	offered, peerCfg, err := exchangeConfig(cfg.Initiator, ctrl, offer)
	if err != nil {
		_ = cfg.Conn.Close()
		return nil, fmt.Errorf("session: config: %w", err)
	}

	keepalive := cfg.KeepaliveInterval
	if keepalive <= 0 {
		keepalive = DefaultKeepaliveInterval
	}
	shared, mismatched := intersectFolders(offered, peerCfg)
	if len(mismatched) > 0 {
		log.Warn("session: refusing folders on key mismatch", "peer", peerID, "folders", mismatched)
	}
	s := &Session{
		conn:          cfg.Conn,
		ctrl:          ctrl,
		peerNodeID:    peerID,
		shared:        shared,
		peerVerifiers: peerVerifiers(peerCfg),
		peerHolders:   peerHolders(peerCfg),
		keepalive:     keepalive,
		started:       time.Now(),
		log:           log,
		done:          make(chan struct{}),
	}
	s.state.Store(int32(StateActive))
	log.Info("session active", "peer", peerID, "shared_folders", len(s.shared))
	return s, nil
}

func openControl(ctx context.Context, cfg Config) (netio.Stream, error) {
	if cfg.Initiator {
		return cfg.Conn.OpenStream(ctx)
	}
	return cfg.Conn.AcceptStream(ctx)
}

// exchange runs one ordered request/response over the control stream: the
// initiator writes first then reads the peer's message; the responder reads
// first then writes its own.
func exchange[T any](initiator bool, write func() error, read func() (T, error)) (T, error) {
	var zero T
	if initiator {
		if err := write(); err != nil {
			return zero, err
		}
		return read()
	}
	peer, err := read()
	if err != nil {
		return zero, err
	}
	if err := write(); err != nil {
		return zero, err
	}
	return peer, nil
}

func exchangeHello(cfg Config, ctrl netio.Stream) (*wirepb.Hello, error) {
	mine := &wirepb.Hello{
		NodeId:            cfg.Local.NodeID,
		WireFormatVersion: wire.WireFormatVersion,
		ClientName:        cfg.Local.ClientName,
		ClientVersion:     cfg.Local.ClientVersion,
	}
	return exchange(cfg.Initiator,
		func() error { return wire.WriteHello(ctrl, mine) },
		func() (*wirepb.Hello, error) { return wire.ReadHello(ctrl) },
	)
}

// exchangeConfig swaps NetworkConfig. The initiator writes first (offer called with nil); the
// responder reads first, so its offer may depend on the peer. Returns our offer and the peer's.
func exchangeConfig(initiator bool, ctrl netio.Stream, offer func(*wirepb.NetworkConfig) ([]Folder, error)) ([]Folder, *wirepb.NetworkConfig, error) {
	if initiator {
		offered, err := offer(nil)
		if err != nil {
			return nil, nil, err
		}
		if err := wire.WriteMessage(ctrl, buildNetworkConfig(offered)); err != nil {
			return nil, nil, err
		}
		peer, err := readNetworkConfig(ctrl)
		return offered, peer, err
	}
	peer, err := readNetworkConfig(ctrl)
	if err != nil {
		return nil, nil, err
	}
	offered, err := offer(peer)
	if err != nil {
		return nil, nil, err
	}
	if err := wire.WriteMessage(ctrl, buildNetworkConfig(offered)); err != nil {
		return nil, nil, err
	}
	return offered, peer, nil
}

// peerOfferedFolders converts a peer's NetworkConfig into the folders it advertised.
func peerOfferedFolders(peer *wirepb.NetworkConfig) []Folder {
	out := make([]Folder, 0, len(peer.GetFolders()))
	for _, f := range peer.GetFolders() {
		if id := f.GetFolderId(); id != "" {
			out = append(out, Folder{ShareID: id, Encrypted: f.GetEncrypted(), EncryptionVerifier: f.GetEncryptionVerifier(), Holder: f.GetHolder()})
		}
	}
	return out
}

func buildNetworkConfig(offered []Folder) *wirepb.NetworkConfig {
	folders := make([]*wirepb.Folder, 0, len(offered))
	for _, f := range offered {
		folders = append(folders, &wirepb.Folder{
			FolderId:           f.ShareID,
			FolderType:         wirepb.FolderType_FOLDER_TYPE_SEND_RECEIVE,
			Encrypted:          f.Encrypted,
			EncryptionVerifier: f.EncryptionVerifier,
			Holder:             f.Holder,
		})
	}
	return &wirepb.NetworkConfig{Folders: folders}
}

func offeredFolders(local []Folder, granted []string) []Folder {
	grant := make(map[string]struct{}, len(granted))
	for _, g := range granted {
		if g != "" {
			grant[g] = struct{}{}
		}
	}
	var out []Folder
	for _, f := range local {
		if f.ShareID == "" {
			continue
		}
		if _, ok := grant[f.ShareID]; ok {
			out = append(out, f)
		}
	}
	return out
}

func readNetworkConfig(ctrl netio.Stream) (*wirepb.NetworkConfig, error) {
	typ, msg, err := wire.ReadControlMessage(ctrl)
	if err != nil {
		return nil, err
	}
	if typ != wire.TypeNetworkConfig {
		return nil, fmt.Errorf("%w: want NetworkConfig, got type %d", ErrUnexpectedMessage, typ)
	}
	return msg.(*wirepb.NetworkConfig), nil
}

// intersectFolders returns the folder ids both sides offer in shared, and any
// excluded for a proven encryption-key mismatch in mismatched.
func intersectFolders(offered []Folder, peer *wirepb.NetworkConfig) (shared, mismatched []string) {
	peerVerifier := make(map[string][]byte, len(peer.GetFolders()))
	peerSet := make(map[string]struct{}, len(peer.GetFolders()))
	for _, f := range peer.GetFolders() {
		if id := f.GetFolderId(); id != "" {
			peerSet[id] = struct{}{}
			peerVerifier[id] = f.GetEncryptionVerifier()
		}
	}
	for _, f := range offered {
		if _, ok := peerSet[f.ShareID]; !ok {
			continue
		}
		if pv := peerVerifier[f.ShareID]; len(f.EncryptionVerifier) > 0 && len(pv) > 0 && !bytes.Equal(f.EncryptionVerifier, pv) {
			mismatched = append(mismatched, f.ShareID)
			continue
		}
		shared = append(shared, f.ShareID)
	}
	sort.Strings(shared)
	sort.Strings(mismatched)
	return shared, mismatched
}

// peerVerifiers maps each folder the peer offered to the encryption verifier it announced.
func peerVerifiers(peer *wirepb.NetworkConfig) map[string][]byte {
	out := make(map[string][]byte, len(peer.GetFolders()))
	for _, f := range peer.GetFolders() {
		if id := f.GetFolderId(); id != "" {
			out[id] = f.GetEncryptionVerifier()
		}
	}
	return out
}

// PeerEncryptionVerifier returns the verifier the peer announced for a folder, or nil. It comes
// from the raw NetworkConfig before intersection, so it resolves even for a folder that ended up
// not shared (a recovery peer proving itself). Do not restrict it to SharedFolders.
func (s *Session) PeerEncryptionVerifier(folderID string) []byte { return s.peerVerifiers[folderID] }

// peerHolders maps each folder the peer offered to whether it serves the folder as a holder.
func peerHolders(peer *wirepb.NetworkConfig) map[string]bool {
	out := make(map[string]bool, len(peer.GetFolders()))
	for _, f := range peer.GetFolders() {
		if id := f.GetFolderId(); id != "" {
			out[id] = f.GetHolder()
		}
	}
	return out
}

// PeerServesAsHolder reports whether the peer advertised the folder as one it serves as a holder.
func (s *Session) PeerServesAsHolder(folderID string) bool { return s.peerHolders[folderID] }

// PeerNodeID is the authenticated peer identity.
func (s *Session) PeerNodeID() string { return s.peerNodeID }

// SharedFolders is the sorted intersection of both sides' offered folder ids.
func (s *Session) SharedFolders() []string { return s.shared }

// State reports the current lifecycle stage.
func (s *Session) State() State { return State(s.state.Load()) }

// Conn returns the underlying connection for opening data streams.
func (s *Session) Conn() netio.Conn { return s.conn }

// SetControlHandler installs the handler for control messages beyond Ping and
// Close. It must be called before Run.
func (s *Session) SetControlHandler(h ControlHandler) { s.handler = h }

// Send writes a control message on the control stream, serialized with keepalive
// and shutdown writes.
func (s *Session) Send(m proto.Message) error { return s.writeMessage(m) }

// Run holds the session open until the peer closes, ctx is cancelled, or it fails.
func (s *Session) Run(ctx context.Context) error {
	stop := context.AfterFunc(ctx, func() { _ = s.Close() })
	defer stop()

	var wg sync.WaitGroup
	wg.Go(func() { s.keepaliveLoop(ctx) })
	defer wg.Wait()
	defer s.shutdown(false)

	for {
		typ, msg, err := wire.ReadControlMessage(s.ctrl)
		if err != nil {
			if s.closing.Load() || ctx.Err() != nil || errors.Is(err, netio.ErrPeerClosed) {
				return nil
			}
			return fmt.Errorf("session: read: %w", err)
		}
		switch typ {
		case wire.TypePing:
		case wire.TypeClose:
			s.shutdown(false)
			return nil
		default:
			if s.handler == nil {
				return fmt.Errorf("%w: type %d", ErrUnexpectedMessage, typ)
			}
			if err := s.handler(ctx, typ, msg); err != nil {
				return fmt.Errorf("session: handle type %d: %w", typ, err)
			}
		}
	}
}

func (s *Session) keepaliveLoop(ctx context.Context) {
	t := time.NewTicker(s.keepalive)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.done:
			return
		case <-t.C:
			if err := s.writeMessage(&wirepb.Ping{}); err != nil {
				return
			}
		}
	}
}

func (s *Session) writeMessage(m proto.Message) error {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	return wire.WriteMessage(s.ctrl, m)
}

// Close sends a graceful Close to the peer and tears the connection down. It is idempotent.
func (s *Session) Close() error {
	s.shutdown(true)
	return nil
}

func (s *Session) shutdown(sendClose bool) {
	s.once.Do(func() {
		s.closing.Store(true)
		if sendClose {
			written := make(chan struct{})
			go func() {
				_ = s.writeMessage(&wirepb.Close{Reason: "shutdown"})
				close(written)
			}()
			select {
			case <-written:
			case <-time.After(closeGraceTimeout):
			}
		}
		s.state.Store(int32(StateClosed))
		close(s.done)
		_ = s.conn.Close()
		s.log.Info("session closed", "peer", s.peerNodeID, "duration", time.Since(s.started))
	})
}
