package peermgr

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/netio"
	"github.com/GhentiLabs/Trove/client/internal/session"
)

const mgrID = "mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm"

func allow(string) (bool, error) { return true, nil }

// peerNode is a stub remote: it accepts connections on the MemNet and completes the
// responder handshake, holding each session so keepalive/close work.
func peerNode(t *testing.T, ctx context.Context, mn *netio.MemNet, id string) {
	t.Helper()
	tr := mn.Transport(id, id)
	go func() {
		for {
			conn, err := tr.Accept(ctx)
			if err != nil {
				return
			}
			go func() {
				s, err := session.Handshake(ctx, session.Config{
					Conn: conn, Initiator: false, Authorize: allow,
					Local: session.Local{NodeID: id},
				})
				if err != nil {
					return
				}
				_ = s.Run(ctx)
			}()
		}
	}()
}

func newManager(t *testing.T, mn *netio.MemNet, opts Options) *Manager {
	t.Helper()
	if opts.Transport == nil {
		opts.Transport = mn.Transport(mgrID, mgrID)
	}
	if opts.Connect == nil {
		mt := mn.Transport(mgrID+"-dial", mgrID)
		opts.Connect = func(ctx context.Context, nodeID string) (netio.Conn, error) {
			return mt.Dial(ctx, nodeID, nodeID)
		}
	}
	if opts.Authorize == nil {
		opts.Authorize = allow
	}
	opts.Self = mgrID
	opts.Local = session.Local{NodeID: mgrID}
	m, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return m
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition not met within deadline")
}

func TestManagerHoldsAuthorizedSessions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mn := netio.NewMemNet()
	peers := []string{
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		"cccccccccccccccccccccccccccccccccccccccccccccccccccc",
	}
	for _, p := range peers {
		peerNode(t, ctx, mn, p)
	}
	m := newManager(t, mn, Options{Peers: peers})

	var wg sync.WaitGroup
	wg.Go(func() { _ = m.Run(ctx) })

	waitFor(t, func() bool { return m.ActiveCount() == 3 })
	for _, p := range peers {
		if _, ok := m.Session(p); !ok {
			t.Fatalf("no session for peer %s", p)
		}
	}
	cancel()
	wg.Wait()
}

func TestManagerRejectsUnauthorized(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mn := netio.NewMemNet()
	peer := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	peerNode(t, ctx, mn, peer)

	deny := func(string) (bool, error) { return false, nil }
	m := newManager(t, mn, Options{Peers: []string{peer}, Authorize: deny})

	var wg sync.WaitGroup
	wg.Go(func() { _ = m.Run(ctx) })

	// Give the dial loop time to attempt and be rejected repeatedly.
	time.Sleep(100 * time.Millisecond)
	if m.ActiveCount() != 0 {
		t.Fatalf("unauthorized peer reached an active session: count=%d", m.ActiveCount())
	}
	cancel()
	wg.Wait()
}

func TestManagerRetriesOnDialFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mn := netio.NewMemNet()
	peer := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	peerNode(t, ctx, mn, peer)

	mt := mn.Transport(mgrID+"-dial", mgrID)
	var attempts atomic.Int32
	connect := func(ctx context.Context, nodeID string) (netio.Conn, error) {
		if attempts.Add(1) <= 2 {
			return nil, errors.New("transient dial failure")
		}
		return mt.Dial(ctx, nodeID, nodeID)
	}
	m := newManager(t, mn, Options{Peers: []string{peer}, Connect: connect, MinBackoff: 5 * time.Millisecond})

	var wg sync.WaitGroup
	wg.Go(func() { _ = m.Run(ctx) })

	waitFor(t, func() bool { return m.ActiveCount() == 1 })
	if attempts.Load() < 3 {
		t.Fatalf("connected without retrying: attempts=%d", attempts.Load())
	}
	cancel()
	wg.Wait()
}

func TestManagerReconnectsAfterDrop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mn := netio.NewMemNet()
	peer := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	peerNode(t, ctx, mn, peer)
	m := newManager(t, mn, Options{Peers: []string{peer}, MinBackoff: 5 * time.Millisecond})

	var wg sync.WaitGroup
	wg.Go(func() { _ = m.Run(ctx) })

	waitFor(t, func() bool { return m.ActiveCount() == 1 })
	first, _ := m.Session(peer)
	_ = first.Close() // simulate a dropped connection

	waitFor(t, func() bool {
		s, ok := m.Session(peer)
		return ok && s != first
	})
	cancel()
	wg.Wait()
}

func TestManagerDedupConverges(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mn := netio.NewMemNet()
	idA := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	idB := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

	ta := mn.Transport("A", idA)
	tb := mn.Transport("B", idB)
	ma, err := New(Options{
		Self: idA, Transport: ta, Local: session.Local{NodeID: idA}, Authorize: allow,
		Peers: []string{idB}, MinBackoff: 5 * time.Millisecond,
		Connect: func(ctx context.Context, id string) (netio.Conn, error) { return ta.Dial(ctx, "B", id) },
	})
	if err != nil {
		t.Fatal(err)
	}
	mb, err := New(Options{
		Self: idB, Transport: tb, Local: session.Local{NodeID: idB}, Authorize: allow,
		Peers: []string{idA}, MinBackoff: 5 * time.Millisecond,
		Connect: func(ctx context.Context, id string) (netio.Conn, error) { return tb.Dial(ctx, "A", id) },
	})
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Go(func() { _ = ma.Run(ctx) })
	wg.Go(func() { _ = mb.Run(ctx) })

	// Both dial each other; deduplication must converge each side to exactly one.
	waitFor(t, func() bool { return ma.ActiveCount() == 1 && mb.ActiveCount() == 1 })
	// And it must stay converged, not oscillate.
	time.Sleep(60 * time.Millisecond)
	if ma.ActiveCount() != 1 || mb.ActiveCount() != 1 {
		t.Fatalf("dedup did not stay converged: a=%d b=%d", ma.ActiveCount(), mb.ActiveCount())
	}
	cancel()
	wg.Wait()
}
