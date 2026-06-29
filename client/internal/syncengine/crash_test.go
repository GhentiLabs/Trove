package syncengine

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/gc"
	"github.com/GhentiLabs/Trove/client/internal/netio"
)

// faultyConn wraps a replica connection to inject data-stream failures. The first
// OpenStream (the control stream) always succeeds; later opens (chunk pulls) obey
// the flags, so the handshake is never disturbed.
type faultyConn struct {
	netio.Conn
	opens    atomic.Int64
	failOpen atomic.Bool
	corrupt  atomic.Bool
}

func (c *faultyConn) OpenStream(ctx context.Context) (netio.Stream, error) {
	n := c.opens.Add(1)
	if n > 1 && c.failOpen.Load() {
		return nil, errors.New("injected: data stream open failure")
	}
	s, err := c.Conn.OpenStream(ctx)
	if err != nil {
		return nil, err
	}
	if n > 1 && c.corrupt.Load() {
		return &corruptStream{Stream: s}, nil
	}
	return s, nil
}

// corruptStream flips every payload byte (those past the 12-byte response header),
// so a pulled chunk fails its hash check.
type corruptStream struct {
	netio.Stream
	read int
}

func (s *corruptStream) Read(p []byte) (int, error) {
	n, err := s.Stream.Read(p)
	for i := 0; i < n; i++ {
		if s.read+i >= 12 {
			p[i] ^= 0xFF
		}
	}
	s.read += n
	return n, err
}

// waitForPullAttempt blocks until the replica has opened at least one data stream
// beyond the control stream, confirming a chunk pull was attempted.
func waitForPullAttempt(t *testing.T, fc *faultyConn) {
	t.Helper()
	waitFor(t, convergeTimeout, "a data-stream pull attempt", func() bool {
		return fc.opens.Load() >= 2
	})
}

func assertReplicaEmpty(t *testing.T, replica, owner peer) {
	t.Helper()
	recs, err := replica.model.ListManifests(context.Background())
	if err != nil {
		t.Fatalf("ListManifests: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("replica committed %d manifests despite a failed pull", len(recs))
	}
	if _, _, ok, _ := replica.model.LoadCursor(context.Background(), folderID, owner.id); ok {
		t.Fatal("cursor advanced despite a failed pull")
	}
}

func TestCrashMidPullLeavesNoPartialThenResumes(t *testing.T) {
	t.Parallel()
	owner := newPeer(t, ownerID)
	replica := newPeer(t, replicaID)
	writeFile(t, owner.root, "a.txt", []byte("alpha"))
	writeFile(t, owner.root, "big.bin", pseudoRandom(2<<20, 3))
	owner.scan(t)

	fc := &faultyConn{}
	fc.failOpen.Store(true)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ownerEng, _ := startSync(t, ctx, owner, replica, func(c netio.Conn) netio.Conn {
		fc.Conn = c
		return fc
	})

	waitForPullAttempt(t, fc)
	assertReplicaEmpty(t, replica, owner)
	if entries := walk(t, replica.root); len(entries) != 0 {
		t.Fatalf("partial destination files present: %v", keys(entries))
	}

	fc.failOpen.Store(false)
	ownerEng.Announce(ctx)
	waitConverged(t, owner, replica)
	assertTreesEqual(t, owner.root, replica.root)
}

func TestCorruptChunkRejectedThenResumes(t *testing.T) {
	t.Parallel()
	owner := newPeer(t, ownerID)
	replica := newPeer(t, replicaID)
	writeFile(t, owner.root, "big.bin", pseudoRandom(2<<20, 4))
	owner.scan(t)

	fc := &faultyConn{}
	fc.corrupt.Store(true)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ownerEng, _ := startSync(t, ctx, owner, replica, func(c netio.Conn) netio.Conn {
		fc.Conn = c
		return fc
	})

	waitForPullAttempt(t, fc)
	assertReplicaEmpty(t, replica, owner)
	if entries := walk(t, replica.root); len(entries) != 0 {
		t.Fatalf("destination files present after corrupt pull: %v", keys(entries))
	}

	fc.corrupt.Store(false)
	ownerEng.Announce(ctx)
	waitConverged(t, owner, replica)
	assertTreesEqual(t, owner.root, replica.root)
}

// TestInFlightChunksSurviveGraceSweep covers the GC interaction: a chunk the puller
// has stored but no manifest yet references survives a sweep because its last_seen is
// within the grace window; once grace elapses and it stays unreferenced, it is swept.
func TestInFlightChunksSurviveGraceSweep(t *testing.T) {
	t.Parallel()
	replica := newPeer(t, replicaID)
	ctx := context.Background()

	id, err := replica.chunks.Put(ctx, replica.fc, pseudoRandom(300<<10, 5))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	if _, err := gc.Sweep(ctx, replica.model, replica.chunks, time.Hour, time.Now()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if has, _ := replica.chunks.Has(ctx, id); !has {
		t.Fatal("in-flight chunk swept inside the grace window")
	}

	if _, err := gc.Sweep(ctx, replica.model, replica.chunks, 0, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("Sweep past grace: %v", err)
	}
	if has, _ := replica.chunks.Has(ctx, id); has {
		t.Fatal("unreferenced chunk past grace was not swept")
	}
}
