// Package node composes the networking stack into one runnable peer.
package node

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/config"
	"github.com/GhentiLabs/Trove/client/internal/control"
	"github.com/GhentiLabs/Trove/client/internal/crypto"
	"github.com/GhentiLabs/Trove/client/internal/discovery"
	"github.com/GhentiLabs/Trove/client/internal/membership"
	"github.com/GhentiLabs/Trove/client/internal/netio"
	"github.com/GhentiLabs/Trove/client/internal/peermgr"
	"github.com/GhentiLabs/Trove/client/internal/session"
	"github.com/GhentiLabs/Trove/client/internal/storage"
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
	gossipInterval        = 30 * time.Second
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

	// StateDir enables sync and membership: per-folder stores and the roster database
	// are opened beneath it. When empty, the node runs transport and discovery only.
	StateDir string
}

// Service is a composed, runnable Trove peer.
type Service struct {
	opts     Options
	log      *slog.Logger
	tr       *transport.Transport
	client   *discovery.Client
	cache    *discovery.Cache
	stunAddr string

	members *membership.Store
	gossip  *gossiper
	// serves is true if this node stores any folder; it gates accepting a non-member's session.
	serves bool

	gatherMu sync.Mutex

	mu        sync.Mutex
	cands     []disco.Address
	reflexive disco.Address
	portMap   *discovery.PortMap

	sigMu sync.Mutex
	sig   *discovery.Signaler

	rtMu   sync.Mutex
	rt     *syncRuntime
	mgr    *peermgr.Manager
	reload chan reloadRequest

	// cfgMu serializes control-plane folder mutations, so an invariant checked
	// against config (the holder/sync mix) cannot be broken by a concurrent
	// mutation between its check and its commit.
	cfgMu sync.Mutex
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
		reload:   make(chan reloadRequest),
	}, nil
}

// Run starts discovery, advertising, and the connection manager until ctx is cancelled.
// The folder-set-scoped stack runs in cycles: requestReload tears the current cycle
// down and builds the next from config, so folder changes take effect live.
func (s *Service) Run(ctx context.Context) error {
	defer s.close()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// The control socket binds first: it is the single-instance lock, so it must
	// be held before anything opens shared state or external connections.
	var ctrl *http.Server
	var ctrlLn net.Listener
	var sockPath string
	if s.opts.StateDir != "" {
		var err error
		ctrlLn, sockPath, err = s.listenControl()
		if err != nil {
			return err
		}
		ctrl = &http.Server{Handler: control.New(controlBackend{s}, s.log).Handler()}
		closeMembers, err := s.openMembership()
		if err != nil {
			_ = ctrlLn.Close()
			_ = os.Remove(sockPath)
			return err
		}
		defer func() { _ = closeMembers() }()
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

	s.log.Info("node: started", "node_id", s.opts.NodeID, "listen", s.tr.LocalAddr().String())

	var wg sync.WaitGroup
	if ctrl != nil {
		wg.Go(func() { _ = ctrl.Serve(ctrlLn) })
		wg.Go(func() {
			<-ctx.Done()
			sctx, cancel := context.WithTimeout(context.Background(), controlShutdownTimeout)
			defer cancel()
			// Close hard on timeout so a handler blocked on a dying run loop
			// cannot strand its client.
			if err := ctrl.Shutdown(sctx); err != nil {
				_ = ctrl.Close()
			}
			_ = os.Remove(sockPath)
		})
	}
	wg.Go(func() { s.announceLoop(ctx) })
	wg.Go(func() { s.stunKeepaliveLoop(ctx) })
	wg.Go(func() { s.browseLoop(ctx) })
	wg.Go(func() { s.signalLoop(ctx, &wg) })
	if s.gossip != nil {
		wg.Go(func() { s.gossipLoop(ctx) })
	}

	var runErr error
	for runErr == nil {
		runErr = s.runSyncCycle(ctx)
	}
	cancel()
	wg.Wait()
	return runErr
}

// runSyncCycle builds the folder-set-scoped stack, runs it until ctx ends or a
// reload is requested, then tears it down in order: cancel sessions and loops,
// wait for them, close the per-folder stores. Returns nil only on reload.
func (s *Service) runSyncCycle(ctx context.Context) error {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var syncRT *syncRuntime
	if s.opts.StateDir != "" {
		var err error
		syncRT, err = s.buildSyncRuntime(cctx)
		if err != nil {
			return err
		}
		syncRT.repairFolders(cctx, s.log)
	}
	closeRT := func() {
		if syncRT != nil {
			syncRT.close()
		}
	}

	local, err := s.localConfig(cctx)
	if err != nil {
		closeRT()
		return err
	}
	peers, err := s.peerIDs(cctx)
	if err != nil {
		closeRT()
		return err
	}

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
		mgrOpts.OnSession = syncRT.onSession(s.log, s.gossip)
		mgrOpts.ResponsiveOffer = syncRT.responsiveOffer
	}
	mgr, err := peermgr.New(mgrOpts)
	if err != nil {
		closeRT()
		return err
	}
	s.setRuntime(syncRT, mgr)
	s.log.Info("node: sync cycle started", "peers", len(peers))

	var wg sync.WaitGroup
	wg.Go(func() { _ = mgr.Run(cctx) })
	if syncRT != nil {
		wg.Go(func() { syncRT.runScanners(cctx, s.log) })
		wg.Go(func() { syncRT.runTombstoneSweeper(cctx, s.log) })
		wg.Go(func() { syncRT.runGCSweeper(cctx, s.log) })
	}

	var req reloadRequest
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case req = <-s.reload:
		s.log.Info("node: reloading folder config")
	}
	s.setRuntime(nil, nil)
	cancel()
	wg.Wait()
	closeRT()
	if req.done != nil {
		close(req.done)
	}
	return err
}

// reloadRequest asks the run loop to rebuild the sync stack; done closes when the
// old stack is fully torn down (per-folder stores closed).
type reloadRequest struct{ done chan struct{} }

// requestReload rebuilds the sync stack from config. It returns once the previous
// stack is torn down; the caller observes the new stack through Status.
func (s *Service) requestReload(ctx context.Context) error {
	req := reloadRequest{done: make(chan struct{})}
	select {
	case s.reload <- req:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-req.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// triggerReload requests a rebuild without waiting for it, for callers inside
// the sync stack where a synchronous wait would deadlock the teardown. An
// aborted send is fine: ctx ending means a teardown (and rebuild from fresh
// config) is already underway.
func (s *Service) triggerReload(ctx context.Context) {
	go func() {
		select {
		case s.reload <- reloadRequest{}:
		case <-ctx.Done():
		}
	}()
}

func (s *Service) setRuntime(rt *syncRuntime, mgr *peermgr.Manager) {
	s.rtMu.Lock()
	s.rt, s.mgr = rt, mgr
	s.rtMu.Unlock()
}

// withRuntime runs fn with the current cycle's runtime (nil while rebuilding or
// when StateDir is empty), holding the lock so a teardown cannot close the
// per-folder stores mid-query. fn must not call requestReload: the cycle needs
// this lock to clear the runtime before it acks.
func (s *Service) withRuntime(fn func(rt *syncRuntime, mgr *peermgr.Manager) error) error {
	s.rtMu.Lock()
	defer s.rtMu.Unlock()
	return fn(s.rt, s.mgr)
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

// openMembership opens the roster store under StateDir and builds the gossiper. The
// signing key is the node's own Ed25519 key, carried by its certificate.
func (s *Service) openMembership() (func() error, error) {
	key, ok := s.opts.Cert.PrivateKey.(ed25519.PrivateKey)
	if !ok {
		return nil, errors.New("node: certificate key is not Ed25519")
	}
	db, err := storage.Open(storage.Options{Path: filepath.Join(s.opts.StateDir, "membership.db"), MaxOpenConns: 4})
	if err != nil {
		return nil, fmt.Errorf("node: open membership db: %w", err)
	}
	store, err := membership.Open(membership.Options{DB: db, NodeID: s.opts.NodeID, Key: key})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	s.members = store
	s.gossip = newGossiper(store, s.log)
	return db.Close, nil
}

func (s *Service) gossipLoop(ctx context.Context) {
	t := time.NewTicker(gossipInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.gossip.resync(ctx)
		}
	}
}

// authorize grants a peer the groups (by group id, which is the folder share id) it is a
// verified member of, plus any group it founded — a member can always reach its founder
// straight from the group id, which bootstraps the roster.
func (s *Service) authorize(ctx context.Context, nodeID string) ([]string, bool, error) {
	if s.members == nil {
		return nil, false, nil
	}
	groups, err := s.members.Groups(ctx)
	if err != nil {
		return nil, false, err
	}
	var granted []string
	for _, g := range groups {
		if founder, ok := membership.Founder(g); ok && founder == nodeID {
			granted = append(granted, g)
			continue
		}
		member, err := s.members.IsMember(ctx, g, nodeID)
		if err != nil {
			return nil, false, err
		}
		if member {
			granted = append(granted, g)
		}
	}
	if len(granted) == 0 {
		// A non-member is accepted but granted nothing, so this node advertises no folder ids to
		// a stranger; the responsive offer shares a folder only on a verifier match, and the
		// member engine / holder blob path serves it read-only. A node that serves nothing rejects.
		return nil, s.serves, nil
	}
	return granted, true, nil
}

func (s *Service) localConfig(ctx context.Context) (session.Local, error) {
	folders, err := s.opts.Config.ListFolders(ctx)
	if err != nil {
		return session.Local{}, err
	}
	var offered []session.Folder
	for _, f := range folders {
		if f.ShareID == "" {
			continue
		}
		sf := session.Folder{ShareID: f.ShareID, Encrypted: f.Encrypted}
		if f.Encrypted {
			switch key, _, err := s.opts.Config.GetFolderKey(ctx, f.ID); {
			case err == nil:
				sf.EncryptionVerifier = crypto.FolderVerifier(key, f.ShareID)
			case errors.Is(err, config.ErrNoKey):
			default:
				return session.Local{}, err
			}
		} else {
			// Advertise the recovery verifier once the secret is known (members never mint their
			// own, so two members never disagree), to validate a delivered recovery secret.
			switch secret, err := s.opts.Config.FolderSecret(ctx, f.ID); {
			case err == nil:
				sf.EncryptionVerifier = crypto.FolderVerifier(secret, f.ShareID)
			case errors.Is(err, config.ErrNoSecret):
			default:
				return session.Local{}, err
			}
		}
		offered = append(offered, sf)
	}
	return session.Local{NodeID: s.opts.NodeID, Folders: offered, ClientName: "trove", ClientVersion: "m6"}, nil
}

// peerIDs is the set of nodes to proactively connect to: every co-member across all
// groups this node belongs to, plus the founder of each group, minus self.
func (s *Service) peerIDs(ctx context.Context) ([]string, error) {
	if s.members == nil {
		return nil, nil
	}
	groups, err := s.members.Groups(ctx)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{})
	for _, g := range groups {
		if founder, ok := membership.Founder(g); ok && founder != s.opts.NodeID {
			seen[founder] = struct{}{}
		}
		roster, err := s.members.Roster(ctx, g)
		if err != nil {
			return nil, err
		}
		for _, e := range roster {
			if e.NodeID != s.opts.NodeID {
				seen[e.NodeID] = struct{}{}
			}
		}
	}
	out := make([]string, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	slices.Sort(out)
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

var _ netio.Transport = (*transport.Transport)(nil)
