package session

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/netio"
	"github.com/GhentiLabs/Trove/client/internal/wire"
	"github.com/GhentiLabs/Trove/client/internal/wire/wirepb"
)

const (
	idA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	idB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func allow(context.Context, string) ([]string, bool, error) { return nil, true, nil }

func grant(ids ...string) func(context.Context, string) ([]string, bool, error) {
	return func(context.Context, string) ([]string, bool, error) { return ids, true, nil }
}

// connPair returns two connected netio.Conns whose PeerNodeIDs are each other's id.
func connPair(t *testing.T, aID, bID string) (netio.Conn, netio.Conn) {
	t.Helper()
	ctx := context.Background()
	mn := netio.NewMemNet()
	a := mn.Transport("a", aID)
	b := mn.Transport("b", bID)
	ch := make(chan netio.Conn, 1)
	go func() { c, _ := b.Accept(ctx); ch <- c }()
	ac, err := a.Dial(ctx, "b", bID)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	return ac, <-ch
}

// handshakePair runs both sides concurrently and returns their results.
func handshakePair(t *testing.T, ctx context.Context, aCfg, bCfg Config) (*Session, error, *Session, error) {
	t.Helper()
	var bs *Session
	var bErr error
	var wg sync.WaitGroup
	wg.Go(func() { bs, bErr = Handshake(ctx, bCfg) })
	as, aErr := Handshake(ctx, aCfg)
	wg.Wait()
	return as, aErr, bs, bErr
}

func TestHandshakeReachesActiveAndIntersectsFolders(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ac, bc := connPair(t, idA, idB)

	aCfg := Config{Conn: ac, Initiator: true, Authorize: grant("shared", "only-a"),
		Local: Local{NodeID: idA, Folders: []Folder{{ShareID: "shared"}, {ShareID: "only-a"}}}}
	bCfg := Config{Conn: bc, Initiator: false, Authorize: grant("shared", "only-b"),
		Local: Local{NodeID: idB, Folders: []Folder{{ShareID: "shared"}, {ShareID: "only-b"}}}}

	as, aErr, bs, bErr := handshakePair(t, ctx, aCfg, bCfg)
	if aErr != nil || bErr != nil {
		t.Fatalf("handshake errors: a=%v b=%v", aErr, bErr)
	}
	if as.State() != StateActive || bs.State() != StateActive {
		t.Fatalf("states: a=%v b=%v", as.State(), bs.State())
	}
	if as.PeerNodeID() != idB || bs.PeerNodeID() != idA {
		t.Fatalf("peer ids: a=%q b=%q", as.PeerNodeID(), bs.PeerNodeID())
	}
	for _, s := range []*Session{as, bs} {
		got := s.SharedFolders()
		if len(got) != 1 || got[0] != "shared" {
			t.Fatalf("shared folders = %v, want [shared]", got)
		}
	}
	// Never Run, so no peer is reading; Close's graceful frame would block on the
	// unbuffered fake stream. Tear down at the transport level instead.
	_ = as.Conn().Close()
	_ = bs.Conn().Close()
}

func TestHandshakeOffersOnlyGrantedFolders(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ac, bc := connPair(t, idA, idB)

	// B offers fb, but A granted B only fa, so A must not include fb.
	aCfg := Config{Conn: ac, Initiator: true, Authorize: grant("fa"),
		Local: Local{NodeID: idA, Folders: []Folder{{ShareID: "fa"}, {ShareID: "fb"}}}}
	bCfg := Config{Conn: bc, Initiator: false, Authorize: grant("fa", "fb"),
		Local: Local{NodeID: idB, Folders: []Folder{{ShareID: "fa"}, {ShareID: "fb"}}}}

	as, aErr, bs, bErr := handshakePair(t, ctx, aCfg, bCfg)
	if aErr != nil || bErr != nil {
		t.Fatalf("handshake: a=%v b=%v", aErr, bErr)
	}
	for _, s := range []*Session{as, bs} {
		got := s.SharedFolders()
		if len(got) != 1 || got[0] != "fa" {
			t.Fatalf("shared folders = %v, want [fa] (fb must not leak past the grant)", got)
		}
	}
	_ = as.Conn().Close()
	_ = bs.Conn().Close()
}

func TestHandshakeRefusesKeyMismatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	match := []byte("same-verifier")
	cases := []struct {
		name       string
		aVer, bVer []byte
		wantShared bool
	}{
		{"matching verifiers sync", match, match, true},
		{"mismatched verifiers refused", []byte("verifier-a"), []byte("verifier-b"), false},
		{"missing peer verifier tolerated", match, nil, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ac, bc := connPair(t, idA, idB)
			aCfg := Config{Conn: ac, Initiator: true, Authorize: grant("enc"),
				Local: Local{NodeID: idA, Folders: []Folder{{ShareID: "enc", Encrypted: true, Verifier: tc.aVer}}}}
			bCfg := Config{Conn: bc, Initiator: false, Authorize: grant("enc"),
				Local: Local{NodeID: idB, Folders: []Folder{{ShareID: "enc", Encrypted: true, Verifier: tc.bVer}}}}

			as, aErr, bs, bErr := handshakePair(t, ctx, aCfg, bCfg)
			if aErr != nil || bErr != nil {
				t.Fatalf("handshake: a=%v b=%v", aErr, bErr)
			}
			for _, s := range []*Session{as, bs} {
				shared := len(s.SharedFolders()) == 1
				if shared != tc.wantShared {
					t.Fatalf("shared = %v, want %v (folders %v)", shared, tc.wantShared, s.SharedFolders())
				}
			}
			_ = as.Conn().Close()
			_ = bs.Conn().Close()
		})
	}
}

func TestHandshakeUnauthorizedNeverActive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ac, bc := connPair(t, idA, idB)

	deny := func(context.Context, string) ([]string, bool, error) { return nil, false, nil }
	aCfg := Config{Conn: ac, Initiator: true, Authorize: deny, Local: Local{NodeID: idA}}
	bCfg := Config{Conn: bc, Initiator: false, Authorize: allow, Local: Local{NodeID: idB}}

	as, aErr, bs, bErr := handshakePair(t, ctx, aCfg, bCfg)
	if !errors.Is(aErr, ErrUnauthorized) {
		t.Fatalf("authorize=false Handshake err = %v, want ErrUnauthorized", aErr)
	}
	if as != nil {
		t.Fatal("unauthorized peer must not yield a session")
	}
	if bs != nil || bErr == nil {
		t.Fatalf("peer must fail cleanly when A rejects: bs=%v bErr=%v", bs, bErr)
	}
}

func TestHandshakeFingerprintMismatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ac, bc := connPair(t, idA, idB)

	// B claims a different node id in its Hello than its certificate fingerprint.
	aCfg := Config{Conn: ac, Initiator: true, Authorize: allow, Local: Local{NodeID: idA}}
	bCfg := Config{Conn: bc, Initiator: false, Authorize: allow, Local: Local{NodeID: "cccccccccccccccccccccccccccccccccccccccccccccccccccc"}}

	as, aErr, bs, bErr := handshakePair(t, ctx, aCfg, bCfg)
	if !errors.Is(aErr, ErrFingerprintMismatch) {
		t.Fatalf("fingerprint mismatch Handshake err = %v, want ErrFingerprintMismatch", aErr)
	}
	if as != nil {
		t.Fatal("fingerprint mismatch must not yield a session")
	}
	if bs != nil || bErr == nil {
		t.Fatalf("peer must fail cleanly on mismatch: bs=%v bErr=%v", bs, bErr)
	}
}

func TestHandshakeVersionMismatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ac, bc := connPair(t, idA, idB)

	done := make(chan struct{})
	go func() {
		defer close(done)
		ctrl, err := bc.AcceptStream(ctx)
		if err != nil {
			return
		}
		if _, err := wire.ReadHello(ctrl); err != nil {
			return
		}
		_ = wire.WriteHello(ctrl, &wirepb.Hello{NodeId: idB, WireFormatVersion: wire.WireFormatVersion + 1})
	}()

	as, err := Handshake(ctx, Config{Conn: ac, Initiator: true, Authorize: allow, Local: Local{NodeID: idA}})
	<-done
	if !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("version mismatch Handshake err = %v, want ErrVersionMismatch", err)
	}
	if as != nil {
		t.Fatal("version mismatch must not yield a session")
	}
}

func TestDuplicateNetworkConfigRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ac, bc := connPair(t, idA, idB)

	// Manual peer: handshake, then send a second NetworkConfig the session must reject.
	go func() {
		ctrl, err := bc.AcceptStream(ctx)
		if err != nil {
			return
		}
		if _, err := wire.ReadHello(ctrl); err != nil {
			return
		}
		_ = wire.WriteHello(ctrl, &wirepb.Hello{NodeId: idB, WireFormatVersion: wire.WireFormatVersion})
		if _, _, err := wire.ReadMessage(ctrl); err != nil {
			return
		}
		_ = wire.WriteMessage(ctrl, &wirepb.NetworkConfig{})
		_ = wire.WriteMessage(ctrl, &wirepb.NetworkConfig{}) // illegal second config
	}()

	as, err := Handshake(ctx, Config{Conn: ac, Initiator: true, Authorize: allow, Local: Local{NodeID: idA}})
	if err != nil {
		t.Fatalf("Handshake: %v", err)
	}
	if err := as.Run(ctx); !errors.Is(err, ErrUnexpectedMessage) {
		t.Fatalf("Run err = %v, want ErrUnexpectedMessage on duplicate NetworkConfig", err)
	}
}

func TestRunReturnsErrorOnAbruptDrop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ac, bc := connPair(t, idA, idB)

	aCfg := Config{Conn: ac, Initiator: true, Authorize: allow, Local: Local{NodeID: idA}}
	bCfg := Config{Conn: bc, Initiator: false, Authorize: allow, Local: Local{NodeID: idB}}
	as, aErr, bs, bErr := handshakePair(t, ctx, aCfg, bCfg)
	if aErr != nil || bErr != nil {
		t.Fatalf("handshake: a=%v b=%v", aErr, bErr)
	}

	bRun := make(chan error, 1)
	go func() { bRun <- bs.Run(ctx) }()
	go func() { _ = as.Run(ctx) }()

	_ = as.Conn().Close() // abrupt drop, no graceful Close

	select {
	case err := <-bRun:
		if err == nil {
			t.Fatal("peer Run returned nil on an abrupt drop, want an error")
		}
	case <-ctx.Done():
		t.Fatal("peer Run did not return after the drop")
	}
}

func TestRunClosePropagatesToPeer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ac, bc := connPair(t, idA, idB)

	aCfg := Config{Conn: ac, Initiator: true, Authorize: allow, Local: Local{NodeID: idA}}
	bCfg := Config{Conn: bc, Initiator: false, Authorize: allow, Local: Local{NodeID: idB}}
	as, aErr, bs, bErr := handshakePair(t, ctx, aCfg, bCfg)
	if aErr != nil || bErr != nil {
		t.Fatalf("handshake: a=%v b=%v", aErr, bErr)
	}

	bDone := make(chan error, 1)
	go func() { bDone <- bs.Run(ctx) }()
	go func() { _ = as.Run(ctx) }()

	if err := as.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-bDone:
		if err != nil {
			t.Fatalf("peer Run returned %v, want nil after graceful close", err)
		}
	case <-ctx.Done():
		t.Fatal("peer Run did not return after graceful close")
	}
	if bs.State() != StateClosed {
		t.Fatalf("peer state = %v, want Closed", bs.State())
	}
}

// An Active session emits a Ping on its keepalive interval. A manual peer reads the
// control stream and blocks until the Ping arrives, so the test is deterministic
// (no wall-clock sleep) and actually verifies a Ping was sent.
func TestKeepaliveSendsPing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ac, bc := connPair(t, idA, idB)
	short := 20 * time.Millisecond

	ctrlc := make(chan netio.Stream, 1)
	go func() {
		ctrl, err := bc.AcceptStream(ctx)
		if err != nil {
			return
		}
		if _, err := wire.ReadHello(ctrl); err != nil {
			return
		}
		if err := wire.WriteHello(ctrl, &wirepb.Hello{NodeId: idB, WireFormatVersion: wire.WireFormatVersion}); err != nil {
			return
		}
		if _, _, err := wire.ReadMessage(ctrl); err != nil {
			return
		}
		if err := wire.WriteMessage(ctrl, &wirepb.NetworkConfig{}); err != nil {
			return
		}
		ctrlc <- ctrl
	}()

	as, err := Handshake(ctx, Config{Conn: ac, Initiator: true, Authorize: allow, Local: Local{NodeID: idA}, KeepaliveInterval: short})
	if err != nil {
		t.Fatalf("Handshake: %v", err)
	}
	go func() { _ = as.Run(ctx) }()

	ctrl := <-ctrlc
	typ, _, err := wire.ReadMessage(ctrl)
	if err != nil {
		t.Fatalf("read keepalive: %v", err)
	}
	if typ != wire.TypePing {
		t.Fatalf("first keepalive message type = %d, want Ping", typ)
	}
}
