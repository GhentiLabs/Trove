package syncengine

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/netio"
	"github.com/GhentiLabs/Trove/client/internal/session"
)

// engineOn builds and drives one engine for a session over p's stores.
func engineOn(t *testing.T, ctx context.Context, sess *session.Session, p peer, role Role, coord *Coordinator) *Engine {
	t.Helper()
	e, err := New(Options{Session: sess, Folders: []FolderConfig{{
		FolderID: folderID, Role: role, Root: p.root, FolderCtx: p.fc, Model: p.model, Chunks: p.chunks, Coord: coord,
	}}})
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	sess.SetControlHandler(e.Handle)
	go func() { _ = sess.Run(ctx) }()
	go func() { _ = e.Drive(ctx) }()
	return e
}

func waitSources(t *testing.T, c *Coordinator, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c.sourceCount() >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("coordinator has %d sources, want %d", c.sourceCount(), n)
}

// TestMultiSourcePullsFromOwnerAndReplica proves a replica fetches distinct chunks from
// two peers at once: chunks a peer-replica already holds come from that peer, chunks
// only the owner has come from the owner.
func TestMultiSourcePullsFromOwnerAndReplica(t *testing.T) {
	owner := newPeer(t, ownerID)
	a := newPeer(t, strings.Repeat("a", 52))
	b := newPeer(t, replicaID)

	writeFile(t, owner.root, "a.bin", pseudoRandom(2<<20, 11))
	owner.scan(t)

	// 1) Replica A fully syncs a.bin from the owner, then we freeze A by cutting its
	//    owner session — A now holds a.bin's chunks and nothing newer.
	ctxA, cancelA := context.WithCancel(context.Background())
	oaO, oaA := memSessionPair(t, ctxA, owner, a)
	engineOn(t, ctxA, oaO, owner, RoleOwner, nil)
	ca := NewCoordinator(folderID, a.fc, a.chunks, 0, nil)
	engineOn(t, ctxA, oaA, a, RoleReplica, ca)
	waitConverged(t, owner, a)
	cancelA()

	// 2) The owner adds b.bin (distinct chunks) that A does not have.
	writeFile(t, owner.root, "b.bin", pseudoRandom(2<<20, 22))
	owner.scan(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cb := NewCoordinator(folderID, b.fc, b.chunks, 0, nil)

	// 3) B connects to A first and registers it as a source (A serves what it holds).
	abA, abB := memSessionPair(t, ctx, a, b)
	aServe := engineOn(t, ctx, abA, a, RoleReplica, nil)
	engineOn(t, ctx, abB, b, RoleReplica, cb)
	waitSources(t, cb, 1)

	// 4) B connects to the owner: register the owner as a source, then start the owner
	//    engine so its announce drives B's reconcile with both sources present.
	obO, obB := memSessionPair(t, ctx, owner, b)
	engineOn(t, ctx, obB, b, RoleReplica, cb)
	waitSources(t, cb, 2)
	ownerServe := engineOn(t, ctx, obO, owner, RoleOwner, nil)

	waitConverged(t, owner, b)
	assertTreesEqual(t, owner.root, b.root)
	assertLeafSetsEqual(t, owner, b)

	if n := aServe.ServedChunks(); n == 0 {
		t.Fatal("peer-replica A served no chunks; pull was not multi-source")
	}
	if n := ownerServe.ServedChunks(); n == 0 {
		t.Fatal("owner served no chunks; b.bin (owner-only) was not fetched from the owner")
	}
}

// TestMultiSourceCorruptSourceRefetched proves a corrupt chunk from one source is
// rejected and transparently refetched from another, with no operator intervention.
func TestMultiSourceCorruptSourceRefetched(t *testing.T) {
	owner := newPeer(t, ownerID)
	a := newPeer(t, strings.Repeat("a", 52))
	b := newPeer(t, replicaID)

	writeFile(t, owner.root, "a.bin", pseudoRandom(2<<20, 33))
	owner.scan(t)

	// A fully syncs the file, then is frozen with a good copy.
	ctxA, cancelA := context.WithCancel(context.Background())
	oaO, oaA := memSessionPair(t, ctxA, owner, a)
	engineOn(t, ctxA, oaO, owner, RoleOwner, nil)
	ca := NewCoordinator(folderID, a.fc, a.chunks, 0, nil)
	engineOn(t, ctxA, oaA, a, RoleReplica, ca)
	waitConverged(t, owner, a)
	cancelA()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cb := NewCoordinator(folderID, b.fc, b.chunks, 0, nil)

	// B connects to A through a corrupting link: every chunk A serves is mangled in
	// flight, so B must verify-fail and refetch from the owner.
	fc := &faultyConn{}
	fc.corrupt.Store(true)
	abA, abB := memSessionPair(t, ctx, a, b, func(c netio.Conn) netio.Conn { fc.Conn = c; return fc })
	engineOn(t, ctx, abA, a, RoleReplica, nil)
	engineOn(t, ctx, abB, b, RoleReplica, cb)
	waitSources(t, cb, 1)

	obO, obB := memSessionPair(t, ctx, owner, b)
	engineOn(t, ctx, obB, b, RoleReplica, cb)
	waitSources(t, cb, 2)
	engineOn(t, ctx, obO, owner, RoleOwner, nil)

	waitConverged(t, owner, b)
	assertTreesEqual(t, owner.root, b.root)
}
