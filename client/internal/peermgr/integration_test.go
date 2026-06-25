package peermgr

import (
	"context"
	"crypto/tls"
	"sync"
	"testing"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/discovery"
	"github.com/GhentiLabs/Trove/client/internal/session"
	"github.com/GhentiLabs/Trove/client/internal/transport"
	"github.com/GhentiLabs/Trove/pkg/identity"
)

// These tests wire the real QUIC transport, the reachability ladder, the
// connection manager, and the session handshake together on loopback — the join
// that the MemNet-backed unit tests cannot exercise (real buffered QUIC streams,
// the graceful close frame, dedup over real connections). No network beyond
// 127.0.0.1 and no holepunch: the cache tier supplies each peer's loopback addr.

func realIdentity(t *testing.T) (tls.Certificate, string) {
	t.Helper()
	_, key, err := identity.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	cert, err := identity.NewCertificate(key)
	if err != nil {
		t.Fatalf("NewCertificate: %v", err)
	}
	return cert, identity.FingerprintCert(cert.Leaf)
}

func realTransport(t *testing.T) (*transport.Transport, string) {
	t.Helper()
	cert, id := realIdentity(t)
	tr, err := transport.New(transport.Options{Cert: cert, UDPAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("transport.New: %v", err)
	}
	t.Cleanup(func() { _ = tr.Close() })
	return tr, id
}

// realResponder accepts inbound connections on tr and runs the responder side of
// the handshake, holding each session until ctx ends.
func realResponder(ctx context.Context, tr *transport.Transport, id string) {
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

func TestLoopbackManagerHoldsAndReconnects(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgrTr, mgrID := realTransport(t)
	cache := discovery.NewCache()
	var peers []string
	for range 3 {
		ptr, pid := realTransport(t)
		realResponder(ctx, ptr, pid)
		cache.Put(pid, ptr.LocalAddr().String())
		peers = append(peers, pid)
	}

	ladder := NewLadder(LadderConfig{Self: mgrID, Cache: cache, Dial: mgrTr.Dial})
	m, err := New(Options{
		Self: mgrID, Transport: mgrTr, Local: session.Local{NodeID: mgrID},
		Authorize: allow, Connect: ladder.Connect, Peers: peers,
		MinBackoff: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Go(func() { _ = m.Run(ctx) })

	waitFor(t, func() bool { return m.ActiveCount() == 3 })

	first, _ := m.Session(peers[0]) // drop one; the manager must re-establish it
	_ = first.Close()
	waitFor(t, func() bool {
		s, ok := m.Session(peers[0])
		return ok && s != first
	})

	cancel()
	wg.Wait()
	if m.ActiveCount() != 0 {
		t.Fatalf("sessions not torn down after shutdown: %d", m.ActiveCount())
	}
}

func TestLoopbackGracefulCloseOverQUIC(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	srvTr, srvID := realTransport(t)
	cliTr, cliID := realTransport(t)

	done := make(chan error, 1)
	go func() {
		conn, err := srvTr.Accept(ctx)
		if err != nil {
			done <- err
			return
		}
		s, err := session.Handshake(ctx, session.Config{
			Conn: conn, Initiator: false, Authorize: allow, Local: session.Local{NodeID: srvID},
		})
		if err != nil {
			done <- err
			return
		}
		done <- s.Run(ctx) // returns nil only on a graceful peer Close
	}()

	conn, err := cliTr.Dial(ctx, srvTr.LocalAddr().String(), srvID)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	cs, err := session.Handshake(ctx, session.Config{
		Conn: conn, Initiator: true, Authorize: allow, Local: session.Local{NodeID: cliID},
	})
	if err != nil {
		t.Fatalf("Handshake: %v", err)
	}

	// Graceful close; the responder's Run must return nil over a real QUIC stream.
	if err := cs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("responder Run = %v, want nil after graceful close", err)
		}
	case <-ctx.Done():
		t.Fatal("responder did not observe the graceful close")
	}
}
