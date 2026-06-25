// Package node composes the networking stack into one runnable peer.
package node

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/config"
	"github.com/GhentiLabs/Trove/client/internal/discovery"
	"github.com/GhentiLabs/Trove/client/internal/netio"
	"github.com/GhentiLabs/Trove/client/internal/peermgr"
	"github.com/GhentiLabs/Trove/client/internal/session"
	"github.com/GhentiLabs/Trove/client/internal/syncengine"
	"github.com/GhentiLabs/Trove/client/internal/transport"
	disco "github.com/GhentiLabs/Trove/pkg/discovery"
)

const (
	announceTTL           = 10 * time.Minute
	announceInterval      = 5 * time.Minute
	gatherTimeout         = 5 * time.Second
	stunKeepaliveInterval = 20 * time.Second
	maxInboundPunches     = 32
	signalReconnectMin    = 1 * time.Second
	signalReconnectMax    = 30 * time.Second
)

var errNoSignaler = errors.New("node: signaler not connected")

// Options configures a Service.
type Options struct {
	Cert     tls.Certificate
	NodeID   string
	Config   *config.Store
	TroveURL string
	UDPAddr  string
	Logger   *slog.Logger

	// StateDir enables one-way sync: per-folder model and chunk stores are opened
	// beneath it. When empty, the node runs transport and discovery only.
	StateDir string
	// SyncRole is this node's direction for its shared folders: owner (send-only,
	// scans and serves) or replica (receive-only, pulls and applies).
	SyncRole syncengine.Role
}

// Service is a composed, runnable Trove peer.
type Service struct {
	opts     Options
	log      *slog.Logger
	tr       *transport.Transport
	client   *discovery.Client
	cache    *discovery.Cache
	stunAddr string

	gatherMu sync.Mutex

	mu        sync.Mutex
	cands     []disco.Address
	reflexive disco.Address
	portMap   *discovery.PortMap

	sigMu sync.Mutex
	sig   *discovery.Signaler
}

// New binds the transport and builds the discovery client. Call Run to start.
func New(opts Options) (*Service, error) {
	log := opts.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	tr, err := transport.New(transport.Options{Cert: opts.Cert, UDPAddr: opts.UDPAddr})
	if err != nil {
		return nil, err
	}
	client, err := discovery.New(discovery.Options{Server: opts.TroveURL, Cert: opts.Cert, Logger: log})
	if err != nil {
		_ = tr.Close()
		return nil, err
	}
	return &Service{
		opts:     opts,
		log:      log,
		tr:       tr,
		client:   client,
		cache:    discovery.NewCache(),
		stunAddr: client.ServerAddr(),
	}, nil
}

// Run starts discovery, advertising, and the connection manager until ctx is cancelled.
func (s *Service) Run(ctx context.Context) error {
	defer s.close()

	local, err := s.localConfig(ctx)
	if err != nil {
		return err
	}
	peers, err := s.peerIDs(ctx)
	if err != nil {
		return err
	}

	var syncRT *syncRuntime
	if s.opts.StateDir != "" {
		syncRT, err = s.buildSyncRuntime(ctx)
		if err != nil {
			return err
		}
		defer syncRT.close()
	}

	// Connect the signaler before the manager starts so the first holepunch round
	// is not wasted on a not-yet-connected signaler; signalLoop then maintains it.
	if sig, err := s.client.Signal(ctx); err != nil {
		s.log.Warn("node: signal connect", "err", err)
	} else {
		s.setSignaler(sig)
	}

	mdns, err := discovery.Advertise(s.opts.NodeID, s.port())
	if err != nil {
		s.log.Warn("node: mdns advertise failed", "err", err)
	}
	defer func() {
		if mdns != nil {
			mdns.Close()
		}
	}()

	ladder := peermgr.NewLadder(peermgr.LadderConfig{
		Self:       s.opts.NodeID,
		Cache:      s.cache,
		Dial:       s.tr.Dial,
		Probe:      s.tr.Probe,
		Lookup:     s.lookup,
		Signal:     s.signal,
		Candidates: s.candidates,
		Logger:     s.log,
	})
	mgrOpts := peermgr.Options{
		Self:      s.opts.NodeID,
		Transport: s.tr,
		Local:     local,
		Authorize: s.authorize,
		Connect:   ladder.Connect,
		Peers:     peers,
		Logger:    s.log,
	}
	if syncRT != nil {
		mgrOpts.OnSession = syncRT.onSession(s.log)
	}
	mgr, err := peermgr.New(mgrOpts)
	if err != nil {
		return err
	}

	s.log.Info("node: started", "node_id", s.opts.NodeID, "listen", s.tr.LocalAddr().String(), "peers", len(peers))

	var wg sync.WaitGroup
	wg.Go(func() { s.announceLoop(ctx) })
	wg.Go(func() { s.stunKeepaliveLoop(ctx) })
	wg.Go(func() { s.browseLoop(ctx) })
	wg.Go(func() { s.signalLoop(ctx, &wg) })
	wg.Go(func() { _ = mgr.Run(ctx) })
	if syncRT != nil {
		wg.Go(func() { syncRT.runScanners(ctx, s.log) })
	}
	wg.Wait()
	return ctx.Err()
}

func (s *Service) announceLoop(ctx context.Context) {
	s.gather(ctx)
	t := time.NewTicker(announceInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.gather(ctx)
		}
	}
}

func (s *Service) browseLoop(ctx context.Context) {
	for peer := range discovery.BrowseLAN(ctx, s.opts.NodeID) {
		s.cache.Put(peer.NodeID, peer.Addr)
		s.log.Debug("node: mdns peer", "peer", peer.NodeID, "addr", peer.Addr)
	}
}

// signalLoop maintains the signaling connection, reconnecting with backoff so a
// dropped WebSocket does not permanently disable holepunching, and dispatches
// inbound punch requests while a connection is live.
func (s *Service) signalLoop(ctx context.Context, wg *sync.WaitGroup) {
	sem := make(chan struct{}, maxInboundPunches)
	backoff := signalReconnectMin
	for {
		if ctx.Err() != nil {
			return
		}
		sig := s.currentSignaler()
		if sig == nil {
			var err error
			sig, err = s.client.Signal(ctx)
			if err != nil {
				s.log.Warn("node: signal connect", "err", err)
				if !sleep(ctx, backoff) {
					return
				}
				backoff = min(backoff*2, signalReconnectMax)
				continue
			}
			s.setSignaler(sig)
		}
		backoff = signalReconnectMin
		s.drainIncoming(ctx, sig, sem, wg)
		s.setSignaler(nil)
		_ = sig.Close()
	}
}

func (s *Service) currentSignaler() *discovery.Signaler {
	s.sigMu.Lock()
	defer s.sigMu.Unlock()
	return s.sig
}

func (s *Service) drainIncoming(ctx context.Context, sig *discovery.Signaler, sem chan struct{}, wg *sync.WaitGroup) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-sig.Done():
			return
		case ir := <-sig.Incoming():
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			wg.Go(func() {
				defer func() { <-sem }()
				s.punchInbound(ctx, ir)
			})
		}
	}
}

func (s *Service) signal(ctx context.Context, nodeID string, cands []disco.Address) (disco.PeerCandidates, error) {
	s.sigMu.Lock()
	sig := s.sig
	s.sigMu.Unlock()
	if sig == nil {
		return disco.PeerCandidates{}, errNoSignaler
	}
	return sig.Connect(ctx, nodeID, cands)
}

func (s *Service) setSignaler(sig *discovery.Signaler) {
	s.sigMu.Lock()
	s.sig = sig
	s.sigMu.Unlock()
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (s *Service) punchInbound(ctx context.Context, ir disco.IncomingRequest) {
	d, ok := peermgr.PunchDelay(ir.PunchAtMillis)
	if !ok {
		s.log.Debug("node: ignoring implausible punch time", "from", ir.FromNodeID)
		return
	}
	if d > 0 {
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return
		}
	}
	addrs := make([]string, 0, len(ir.Candidates))
	for _, a := range ir.Candidates {
		addrs = append(addrs, a.String())
	}
	s.log.Debug("node: inbound punch probe", "from", ir.FromNodeID, "addrs", addrs)
	if err := s.tr.Probe(ctx, addrs); err != nil {
		s.log.Debug("node: inbound probe failed", "from", ir.FromNodeID, "err", err)
	}
}

func (s *Service) gather(ctx context.Context) {
	s.gatherMu.Lock()
	defer s.gatherMu.Unlock()

	cands, err := discovery.LocalCandidates(s.port())
	if err != nil {
		s.log.Warn("node: local candidates", "err", err)
	}

	ref, refOK := s.reflexiveCandidate(ctx)
	if refOK {
		cands = append(cands, ref)
	}

	cands = append(cands, s.mapPort(ctx)...)

	actx, cancel := context.WithTimeout(ctx, gatherTimeout)
	_, err = s.client.Announce(actx, cands, announceTTL)
	cancel()
	if err != nil {
		s.log.Warn("node: announce", "err", err)
	}

	s.mu.Lock()
	s.cands = cands
	if refOK {
		s.reflexive = ref
	}
	s.mu.Unlock()
	s.log.Debug("node: candidates gathered", "count", len(cands), "reflexive", refOK)
}

func (s *Service) mapPort(ctx context.Context) []disco.Address {
	if s.portMap == nil {
		mctx, cancel := context.WithTimeout(ctx, gatherTimeout)
		pm, err := discovery.MapPort(mctx, s.port())
		cancel()
		if err == nil {
			s.mu.Lock()
			s.portMap = pm
			s.mu.Unlock()
		}
	} else {
		rctx, cancel := context.WithTimeout(ctx, gatherTimeout)
		err := s.portMap.Refresh(rctx)
		cancel()
		if err != nil {
			s.log.Warn("node: upnp refresh", "err", err)
		}
	}
	if s.portMap != nil {
		return []disco.Address{s.portMap.External}
	}
	return nil
}

func (s *Service) reflexiveCandidate(ctx context.Context) (disco.Address, bool) {
	if s.stunAddr == "" {
		return disco.Address{}, false
	}
	rctx, cancel := context.WithTimeout(ctx, gatherTimeout)
	defer cancel()
	ap, err := s.tr.Reflexive(rctx, s.stunAddr)
	if err != nil {
		s.log.Debug("node: stun reflexive", "err", err)
		return disco.Address{}, false
	}
	a := disco.Address{IP: ap.Addr().Unmap().String(), Port: int(ap.Port()), Type: disco.AddressSTUN}
	if a.Validate() != nil {
		return disco.Address{}, false
	}
	s.log.Debug("node: reflexive", "addr", a.String(), "local_port", s.port())
	return a, true
}

func (s *Service) stunKeepaliveLoop(ctx context.Context) {
	if s.stunAddr == "" {
		return
	}
	t := time.NewTicker(stunKeepaliveInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			ref, ok := s.reflexiveCandidate(ctx)
			if !ok {
				continue
			}
			s.mu.Lock()
			changed := s.reflexive != ref
			s.mu.Unlock()
			if changed {
				s.log.Debug("node: reflexive address changed, re-announcing", "addr", ref.String())
				s.gather(ctx)
			}
		}
	}
}

func (s *Service) candidates() []disco.Address {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]disco.Address(nil), s.cands...)
}

func (s *Service) lookup(ctx context.Context, nodeID string) ([]string, error) {
	resp, err := s.client.Lookup(ctx, nodeID)
	if err != nil {
		return nil, err
	}
	local := localSubnets()
	out := make([]string, 0, len(resp.Addresses))
	for _, a := range resp.Addresses {
		if a.Type == disco.AddressSTUN {
			continue
		}
		if a.Validate() != nil || !dialable(a, local) {
			continue
		}
		out = append(out, a.String())
	}
	return out, nil
}

func dialable(a disco.Address, local []*net.IPNet) bool {
	ip := net.ParseIP(a.IP)
	if ip == nil {
		return false
	}
	if !ip.IsPrivate() {
		return true
	}
	for _, n := range local {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

func localSubnets() []*net.IPNet {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil
	}
	out := make([]*net.IPNet, 0, len(addrs))
	for _, a := range addrs {
		if n, ok := a.(*net.IPNet); ok {
			out = append(out, n)
		}
	}
	return out
}

func (s *Service) authorize(nodeID string) ([]string, bool, error) {
	peer, err := s.opts.Config.GetPeer(context.Background(), nodeID)
	switch {
	case err == nil:
		return peer.Folders, true, nil
	case isNotFound(err):
		return nil, false, nil
	default:
		return nil, false, err
	}
}

func (s *Service) localConfig(ctx context.Context) (session.Local, error) {
	folders, err := s.opts.Config.ListFolders(ctx)
	if err != nil {
		return session.Local{}, err
	}
	var offered []session.Folder
	for _, f := range folders {
		if f.ShareID != "" {
			offered = append(offered, session.Folder{ShareID: f.ShareID, Encrypted: f.Encrypted})
		}
	}
	return session.Local{NodeID: s.opts.NodeID, Folders: offered, ClientName: "trove", ClientVersion: "m3"}, nil
}

func (s *Service) peerIDs(ctx context.Context) ([]string, error) {
	peers, err := s.opts.Config.ListPeers(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(peers))
	for _, p := range peers {
		out = append(out, p.NodeID)
	}
	return out, nil
}

func (s *Service) port() int {
	if ua, ok := s.tr.LocalAddr().(*net.UDPAddr); ok {
		return ua.Port
	}
	return 0
}

func (s *Service) close() {
	s.mu.Lock()
	pm := s.portMap
	s.portMap = nil
	s.mu.Unlock()
	if pm != nil {
		_ = pm.Release()
	}
	_ = s.tr.Close()
}

func isNotFound(err error) bool {
	return errors.Is(err, config.ErrPeerNotFound)
}

var _ netio.Transport = (*transport.Transport)(nil)
