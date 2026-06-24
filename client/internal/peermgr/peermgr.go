// Package peermgr is the connection manager: it holds N authenticated peer sessions
// at once, drives the reachability ladder to dial authorized peers (the ladder is
// injected as Connect so the holepunch/discovery wiring stays out of the core),
// accepts inbound connections, deduplicates the two connections formed when both
// sides dial at once, and reconnects with backoff. Session lifecycle is the testable
// core; the live ladder is glued on at construction.
package peermgr

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/netio"
	"github.com/GhentiLabs/Trove/client/internal/session"
)

// serveResult distinguishes why a connection attempt ended, so the dial loop only
// grows backoff on a genuine failure, not on a healthy connection that lost dedup.
type serveResult int

const (
	serveFailed serveResult = iota // handshake failed
	serveDedup                     // connection was healthy but lost the tiebreak
	serveRan                       // session ran to completion
)

const (
	defaultMinBackoff = 1 * time.Second
	defaultMaxBackoff = 1 * time.Minute
	// maxInboundHandshakes bounds concurrent inbound handshakes so a flood of
	// connecting-but-stalling peers cannot grow handshake goroutines without limit.
	maxInboundHandshakes = 64
)

// Options configures New.
type Options struct {
	// Self is this node's id, used for the duplicate-connection tiebreak.
	Self string
	// Transport accepts inbound peer connections.
	Transport netio.Transport
	// Local is this node's advertised identity and folders for the handshake.
	Local session.Local
	// Authorize gates a peer's certificate-derived node id and returns the shared
	// folder ids granted to it (passed through to the session).
	Authorize func(nodeID string) (granted []string, ok bool, err error)
	// Connect is the reachability ladder: it returns an authenticated transport
	// connection to nodeID (LAN, direct, or holepunch) or an error to retry.
	Connect func(ctx context.Context, nodeID string) (netio.Conn, error)
	// Peers are the node ids to actively maintain connections to.
	Peers []string
	// MinBackoff/MaxBackoff bound per-peer reconnect backoff.
	MinBackoff, MaxBackoff time.Duration
	// Logger receives manager events; nil discards them.
	Logger *slog.Logger
}

// Manager holds and maintains the set of active peer sessions.
type Manager struct {
	self       string
	transport  netio.Transport
	local      session.Local
	authorize  func(string) ([]string, bool, error)
	connect    func(context.Context, string) (netio.Conn, error)
	peers      []string
	minBackoff time.Duration
	maxBackoff time.Duration
	log        *slog.Logger

	mu       sync.Mutex
	sessions map[string]*session.Session
}

// New validates options and returns a Manager. Call Run to start it.
func New(opts Options) (*Manager, error) {
	log := opts.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	minB := opts.MinBackoff
	if minB <= 0 {
		minB = defaultMinBackoff
	}
	maxB := opts.MaxBackoff
	if maxB < minB {
		maxB = defaultMaxBackoff
	}
	return &Manager{
		self:       opts.Self,
		transport:  opts.Transport,
		local:      opts.Local,
		authorize:  opts.Authorize,
		connect:    opts.Connect,
		peers:      opts.Peers,
		minBackoff: minB,
		maxBackoff: maxB,
		log:        log,
		sessions:   make(map[string]*session.Session),
	}, nil
}

// Run accepts inbound connections and maintains an outbound connection to each
// configured peer until ctx is cancelled, then tears all sessions down.
func (m *Manager) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	if m.transport != nil {
		wg.Go(func() { m.acceptLoop(ctx, &wg) })
	}
	for _, p := range m.peers {
		wg.Go(func() { m.dialLoop(ctx, p) })
	}
	wg.Wait()
	m.closeAll()
	return ctx.Err()
}

// acceptLoop tracks each inbound session goroutine in wg so Run does not return
// while a serve goroutine is still using the transport.
func (m *Manager) acceptLoop(ctx context.Context, wg *sync.WaitGroup) {
	sem := make(chan struct{}, maxInboundHandshakes)
	for {
		conn, err := m.transport.Accept(ctx)
		if err != nil {
			return
		}
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			_ = conn.Close()
			return
		}
		wg.Go(func() {
			defer func() { <-sem }()
			m.serve(ctx, conn, false)
		})
	}
}

func (m *Manager) dialLoop(ctx context.Context, peerID string) {
	backoff := m.minBackoff
	for {
		if ctx.Err() != nil {
			return
		}
		if m.has(peerID) {
			if !sleep(ctx, m.minBackoff) {
				return
			}
			continue
		}
		conn, err := m.connect(ctx, peerID)
		if err != nil {
			// errAwaitInbound is the holepunch acceptor's normal outcome: the NAT
			// probe fired and the connection arrives via the accept loop. Retry at the
			// base interval to keep the mapping fresh rather than backing off.
			if errors.Is(err, errAwaitInbound) {
				backoff = m.minBackoff
				if !sleep(ctx, m.minBackoff) {
					return
				}
				continue
			}
			m.log.Debug("peermgr: dial failed", "peer", peerID, "err", err)
			if !sleep(ctx, backoff) {
				return
			}
			backoff = min(backoff*2, m.maxBackoff)
			continue
		}
		switch m.serve(ctx, conn, true) {
		case serveRan, serveDedup:
			backoff = m.minBackoff
		case serveFailed:
			if !sleep(ctx, backoff) {
				return
			}
			backoff = min(backoff*2, m.maxBackoff)
		}
	}
}

// serve completes the handshake on conn and, if it wins deduplication, runs the
// session until it ends.
func (m *Manager) serve(ctx context.Context, conn netio.Conn, initiator bool) serveResult {
	sess, err := session.Handshake(ctx, session.Config{
		Conn:      conn,
		Initiator: initiator,
		Local:     m.local,
		Authorize: m.authorize,
		Logger:    m.log,
	})
	if err != nil {
		m.log.Debug("peermgr: handshake failed", "initiator", initiator, "err", err)
		return serveFailed
	}
	peerID := sess.PeerNodeID()
	if !m.add(peerID, sess, initiator) {
		_ = sess.Close()
		return serveDedup
	}
	_ = sess.Run(ctx)
	m.remove(peerID, sess)
	return serveRan
}

// add registers sess for peerID, resolving a duplicate by keeping the session in
// which the lexicographically smaller node id is the initiator. Both peers compute
// the same winner, so they converge on a single connection.
func (m *Manager) add(peerID string, sess *session.Session, initiator bool) bool {
	m.mu.Lock()
	existing, ok := m.sessions[peerID]
	if ok && initiator != (m.self < peerID) {
		m.mu.Unlock()
		return false
	}
	m.sessions[peerID] = sess
	m.mu.Unlock()
	// Evict the losing duplicate outside the lock: its graceful Close can block for
	// up to the session's close-grace timeout, which must not stall the manager.
	if ok {
		_ = existing.Close()
	}
	return true
}

func (m *Manager) remove(peerID string, sess *session.Session) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cur, ok := m.sessions[peerID]; ok && cur == sess {
		delete(m.sessions, peerID)
	}
}

func (m *Manager) has(peerID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.sessions[peerID]
	return ok
}

func (m *Manager) closeAll() {
	m.mu.Lock()
	sessions := make([]*session.Session, 0, len(m.sessions))
	for _, s := range m.sessions {
		sessions = append(sessions, s)
	}
	m.mu.Unlock()
	for _, s := range sessions {
		_ = s.Close()
	}
}

// Session returns the active session for nodeID, if any.
func (m *Manager) Session(nodeID string) (*session.Session, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[nodeID]
	return s, ok
}

// ActiveCount is the number of held sessions.
func (m *Manager) ActiveCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sessions)
}

// sleep waits for d or until ctx is cancelled, reporting whether it slept fully.
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
