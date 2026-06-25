// Package session drives a netio.Conn through the handshake state machine to an
// Active, keepalive-held, framed control channel and tears it down gracefully.
package session

import (
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
}

// Local identifies this node for the Hello and the folders it offers.
type Local struct {
	NodeID        string
	ClientName    string
	ClientVersion string
	Folders       []Folder
}

// Config drives a single Handshake.
type Config struct {
	Conn      netio.Conn
	Initiator bool
	Local     Local
	Authorize func(nodeID string) (granted []string, ok bool, err error)

	KeepaliveInterval time.Duration
	HandshakeTimeout  time.Duration
	Logger            *slog.Logger
}

// Session is an authenticated, Active, framed, multiplexed peer connection.
type Session struct {
	conn       netio.Conn
	ctrl       netio.Stream
	peerNodeID string
	shared     []string
	keepalive  time.Duration
	started    time.Time
	log        *slog.Logger

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
		authorize = func(string) ([]string, bool, error) { return nil, false, nil }
	}
	granted, ok, err := authorize(peerID)
	if err != nil {
		_ = cfg.Conn.Close()
		return nil, fmt.Errorf("session: authorize: %w", err)
	}
	if !ok {
		_ = cfg.Conn.Close()
		return nil, fmt.Errorf("%w: %s", ErrUnauthorized, peerID)
	}

	offered := offeredFolders(cfg.Local.Folders, granted)
	peerCfg, err := exchangeConfig(cfg.Initiator, ctrl, offered)
	if err != nil {
		_ = cfg.Conn.Close()
		return nil, fmt.Errorf("session: config: %w", err)
	}

	keepalive := cfg.KeepaliveInterval
	if keepalive <= 0 {
		keepalive = DefaultKeepaliveInterval
	}
	s := &Session{
		conn:       cfg.Conn,
		ctrl:       ctrl,
		peerNodeID: peerID,
		shared:     intersectFolders(offered, peerCfg),
		keepalive:  keepalive,
		started:    time.Now(),
		log:        log,
		done:       make(chan struct{}),
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

func exchangeConfig(initiator bool, ctrl netio.Stream, offered []Folder) (*wirepb.NetworkConfig, error) {
	mine := buildNetworkConfig(offered)
	return exchange(initiator,
		func() error { return wire.WriteMessage(ctrl, mine) },
		func() (*wirepb.NetworkConfig, error) { return readNetworkConfig(ctrl) },
	)
}

func buildNetworkConfig(offered []Folder) *wirepb.NetworkConfig {
	folders := make([]*wirepb.Folder, 0, len(offered))
	for _, f := range offered {
		folders = append(folders, &wirepb.Folder{
			FolderId:   f.ShareID,
			FolderType: wirepb.FolderType_FOLDER_TYPE_SEND_RECEIVE,
			Encrypted:  f.Encrypted,
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

func intersectFolders(offered []Folder, peer *wirepb.NetworkConfig) []string {
	peerSet := make(map[string]struct{}, len(peer.GetFolders()))
	for _, f := range peer.GetFolders() {
		if id := f.GetFolderId(); id != "" {
			peerSet[id] = struct{}{}
		}
	}
	var out []string
	for _, f := range offered {
		if _, ok := peerSet[f.ShareID]; ok {
			out = append(out, f.ShareID)
		}
	}
	sort.Strings(out)
	return out
}

// PeerNodeID is the authenticated peer identity.
func (s *Session) PeerNodeID() string { return s.peerNodeID }

// SharedFolders is the sorted intersection of both sides' offered folder ids.
func (s *Session) SharedFolders() []string { return s.shared }

// State reports the current lifecycle stage.
func (s *Session) State() State { return State(s.state.Load()) }

// Conn exposes the underlying connection so M4 can open data streams.
func (s *Session) Conn() netio.Conn { return s.conn }

// Run holds the session open until the peer closes, ctx is cancelled, or it fails.
func (s *Session) Run(ctx context.Context) error {
	stop := context.AfterFunc(ctx, func() { _ = s.Close() })
	defer stop()

	var wg sync.WaitGroup
	wg.Go(func() { s.keepaliveLoop(ctx) })
	defer wg.Wait()
	defer s.shutdown(false)

	for {
		typ, _, err := wire.ReadControlMessage(s.ctrl)
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
			return fmt.Errorf("%w: type %d", ErrUnexpectedMessage, typ)
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
