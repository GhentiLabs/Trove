// Package node composes the M3 networking stack into one runnable peer: it binds a
// QUIC transport, talks to Trove (announce/lookup/signal), advertises and browses
// the LAN over mDNS, maps a port via UPnP, and runs the connection manager over the
// reachability ladder. It is the thin integration layer the live two-machine
// acceptance gate drives; the daemon control surface (L10) is out of M3 scope.
package node

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/config"
	"github.com/GhentiLabs/Trove/client/internal/discovery"
	"github.com/GhentiLabs/Trove/client/internal/netio"
	"github.com/GhentiLabs/Trove/client/internal/peermgr"
	"github.com/GhentiLabs/Trove/client/internal/session"
	"github.com/GhentiLabs/Trove/client/internal/transport"
	disco "github.com/GhentiLabs/Trove/pkg/discovery"
)

const (
	announceTTL      = 10 * time.Minute
	announceInterval = 5 * time.Minute
	gatherTimeout    = 5 * time.Second
)

// Options configures a Service.
type Options struct {
	// Cert and NodeID are this node's identity (from pkg/identity).
	Cert   tls.Certificate
	NodeID string
	// Config is the opened config store (peer registry + folders).
	Config *config.Store
	// TroveURL is the trove://host:port?id=<fp> discovery server string.
	TroveURL string
	// UDPAddr is the local QUIC bind address, e.g. "0.0.0.0:0".
	UDPAddr string
	// Logger receives node events; nil discards them.
	Logger *slog.Logger
}

// Service is a composed, runnable Trove peer.
type Service struct {
	opts   Options
	log    *slog.Logger
	tr     *transport.Transport
	client *discovery.Client
	cache  *discovery.Cache

	mu      sync.Mutex
	cands   []disco.Address
	portMap *discovery.PortMap
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
	return &Service{opts: opts, log: log, tr: tr, client: client, cache: discovery.NewCache()}, nil
}

// Run starts discovery, advertising, and the connection manager, blocking until ctx
// is cancelled.
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

	s.gather(ctx)

	sig, err := s.client.Signal(ctx)
	if err != nil {
		return fmt.Errorf("node: signal: %w", err)
	}
	defer func() { _ = sig.Close() }()

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
		Signal:     sig.Connect,
		Candidates: s.candidates,
		Logger:     s.log,
	})
	mgr, err := peermgr.New(peermgr.Options{
		Self:      s.opts.NodeID,
		Transport: s.tr,
		Local:     local,
		Authorize: s.authorize,
		Connect:   ladder.Connect,
		Peers:     peers,
		Logger:    s.log,
	})
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	wg.Go(func() { s.announceLoop(ctx) })
	wg.Go(func() { s.browseLoop(ctx) })
	wg.Go(func() { s.incomingLoop(ctx, sig) })
	wg.Go(func() { _ = mgr.Run(ctx) })
	wg.Wait()
	return ctx.Err()
}

// announceLoop refreshes this node's registration before its TTL lapses.
func (s *Service) announceLoop(ctx context.Context) {
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

// browseLoop feeds mDNS-discovered LAN peers into the address cache so the ladder's
// first tier reaches them without Trove.
func (s *Service) browseLoop(ctx context.Context) {
	for peer := range discovery.BrowseLAN(ctx, s.opts.NodeID) {
		s.cache.Put(peer.NodeID, peer.Addr)
	}
}

// incomingLoop opens this node's NAT mapping when a peer punches toward it, so the
// peer's simultaneous dial reaches the accept loop.
func (s *Service) incomingLoop(ctx context.Context, sig *discovery.Signaler) {
	for {
		select {
		case <-ctx.Done():
			return
		case ir := <-sig.Incoming():
			go s.punchInbound(ctx, ir)
		}
	}
}

func (s *Service) punchInbound(ctx context.Context, ir disco.IncomingRequest) {
	if d := time.Until(time.UnixMilli(ir.PunchAtMillis)); d > 0 {
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
	if err := s.tr.Probe(ctx, addrs); err != nil {
		s.log.Debug("node: inbound probe failed", "from", ir.FromNodeID, "err", err)
	}
}

// gather rebuilds the candidate set: local interface addresses, the Trove-observed
// external address, and a UPnP/NAT-PMP mapping (best-effort).
func (s *Service) gather(ctx context.Context) {
	gctx, cancel := context.WithTimeout(ctx, gatherTimeout)
	defer cancel()

	cands, err := discovery.LocalCandidates(s.port())
	if err != nil {
		s.log.Warn("node: local candidates", "err", err)
	}
	if resp, err := s.client.Announce(gctx, cands, announceTTL); err == nil {
		if obs := observedAddress(resp.ObservedAddr); obs != nil {
			cands = append(cands, *obs)
		}
	} else {
		s.log.Warn("node: announce", "err", err)
	}
	if s.portMap == nil {
		if pm, err := discovery.MapPort(gctx, s.port()); err == nil {
			s.mu.Lock()
			s.portMap = pm
			s.mu.Unlock()
			cands = append(cands, pm.External)
		}
	} else {
		cands = append(cands, s.portMap.External)
	}

	s.mu.Lock()
	s.cands = cands
	s.mu.Unlock()
}

func (s *Service) candidates() []disco.Address {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cands
}

func (s *Service) lookup(ctx context.Context, nodeID string) ([]string, error) {
	resp, err := s.client.Lookup(ctx, nodeID)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(resp.Addresses))
	for _, a := range resp.Addresses {
		out = append(out, a.String())
	}
	return out, nil
}

func (s *Service) authorize(nodeID string) (bool, error) {
	_, err := s.opts.Config.GetPeer(context.Background(), nodeID)
	switch {
	case err == nil:
		return true, nil
	case isNotFound(err):
		return false, nil
	default:
		return false, err
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

func observedAddress(addr string) *disco.Address {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return nil
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil
	}
	a := disco.Address{IP: host, Port: port, Type: disco.AddressPublic}
	if a.Validate() != nil {
		return nil
	}
	return &a
}

var _ netio.Transport = (*transport.Transport)(nil)
