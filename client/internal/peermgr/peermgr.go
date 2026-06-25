// Package peermgr is the connection manager: it holds and maintains authenticated
// peer sessions, deduplicating and reconnecting as needed.
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

type serveResult int

const (
	serveFailed serveResult = iota
	serveDedup
	serveRan
)

const (
	defaultMinBackoff    = 1 * time.Second
	defaultMaxBackoff    = 1 * time.Minute
	holepunchRetry       = 2 * time.Second
	maxInboundHandshakes = 64
)

// Options configures New.
type Options struct {
	Self                   string
	Transport              netio.Transport
	Local                  session.Local
	Authorize              func(nodeID string) (granted []string, ok bool, err error)
	Connect                func(ctx context.Context, nodeID string) (netio.Conn, error)
	Peers                  []string
	MinBackoff, MaxBackoff time.Duration
	Logger                 *slog.Logger
	// OnSession, if set, is called for each Active session before its Run loop and
	// must return a stop func invoked when the session ends.
	OnSession func(ctx context.Context, s *session.Session) (stop func())
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
	onSession  func(context.Context, *session.Session) func()

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
		onSession:  opts.OnSession,
		sessions:   make(map[string]*session.Session),
	}, nil
}

// Run maintains connections to all configured peers until ctx is cancelled.
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
			if errors.Is(err, errAwaitInbound) || errors.Is(err, errPunchMissed) {
				backoff = m.minBackoff
				if !sleep(ctx, holepunchRetry) {
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
	if m.onSession != nil {
		stop := m.onSession(ctx, sess)
		defer stop()
	}
	_ = sess.Run(ctx)
	m.remove(peerID, sess)
	return serveRan
}

func (m *Manager) add(peerID string, sess *session.Session, initiator bool) bool {
	m.mu.Lock()
	existing, ok := m.sessions[peerID]
	if ok && initiator != (m.self < peerID) {
		m.mu.Unlock()
		return false
	}
	m.sessions[peerID] = sess
	m.mu.Unlock()
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
