package syncengine

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/GhentiLabs/Trove/client/internal/netio"
)

// simPeer is a writer with its own per-node coordinator, the unit of the multi-node
// simulation harness.
type simPeer struct {
	peer
	coord *Coordinator
}

func newSimPeer(t *testing.T, idChar byte) simPeer {
	t.Helper()
	p := newPeer(t, strings.Repeat(string(idChar), 52))
	return simPeer{peer: p, coord: NewCoordinator(folderID, p.fc, p.chunks, 0, nil)}
}

func simPeers(t *testing.T, n int) []simPeer {
	t.Helper()
	out := make([]simPeer, n)
	for i := range out {
		out[i] = newSimPeer(t, byte('a'+i))
	}
	return out
}

func bare(peers []simPeer) []peer {
	out := make([]peer, len(peers))
	for i, p := range peers {
		out[i] = p.peer
	}
	return out
}

// connect wires writers i and j as a bidirectional session over a fresh MemNet, driven
// until ctx ends.
func connect(t *testing.T, ctx context.Context, a, b simPeer) {
	t.Helper()
	sa, sb := memSessionPair(t, ctx, a.peer, b.peer)
	engineOn(t, ctx, sa, a.peer, RoleWriter, a.coord)
	engineOn(t, ctx, sb, b.peer, RoleWriter, b.coord)
}

func connectMesh(t *testing.T, ctx context.Context, peers []simPeer) {
	t.Helper()
	for i := range peers {
		for j := i + 1; j < len(peers); j++ {
			connect(t, ctx, peers[i], peers[j])
		}
	}
}

// randomEdits applies count random edits and deletes to a shared path set, scanning after
// each so the writer originates them, deterministically from rng.
func randomEdits(t *testing.T, p simPeer, idx int, paths []string, count int, rng *rand.Rand) {
	t.Helper()
	for k := 0; k < count; k++ {
		path := paths[rng.IntN(len(paths))]
		abs := filepath.Join(p.root, filepath.FromSlash(path))
		if rng.IntN(5) == 0 {
			_ = os.Remove(abs)
		} else {
			writeFile(t, p.root, path, []byte(fmt.Sprintf("p%d-k%d-%d", idx, k, rng.Uint64())))
		}
		p.scan(t)
	}
}

// assertAllEqual checks every peer holds a byte-identical tree.
func assertAllEqual(t *testing.T, peers []simPeer) {
	t.Helper()
	for i := 1; i < len(peers); i++ {
		assertTreesEqual(t, peers[0].root, peers[i].root)
	}
}

// TestSimManyWritersConverge has three writers each originate a burst of random edits and
// deletes offline, then connect as a full mesh and converge to one byte-identical tree.
func TestSimManyWritersConverge(t *testing.T) {
	t.Parallel()
	peers := simPeers(t, 3)
	paths := []string{"a.txt", "b.txt", "c.txt", "dir/d.txt", "dir/e.txt"}
	rng := rand.New(rand.NewPCG(0x5eed, 0xface))
	for i := range peers {
		randomEdits(t, peers[i], i, paths, 8, rng)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connectMesh(t, ctx, peers)

	waitSameRoot(t, bare(peers)...)
	assertAllEqual(t, peers)

	// Convergence is a fixpoint: re-scanning a converged writer (its conflict copies are
	// real files on disk now) must originate nothing and keep every tree identical.
	want := peers[0].currentRoot(t)
	for i := range peers {
		peers[i].scan(t)
	}
	waitSameRoot(t, bare(peers)...)
	if got := peers[0].currentRoot(t); got != want {
		t.Fatalf("re-scan of a converged tree changed the root: %s -> %s", want, got)
	}
	assertAllEqual(t, peers)
}

// TestSimPartitionHealConverges runs rounds where the mesh is torn down, each writer edits
// offline, then the mesh heals; after the final heal all writers converge identically.
func TestSimPartitionHealConverges(t *testing.T) {
	t.Parallel()
	peers := simPeers(t, 3)
	paths := []string{"shared.txt", "notes.txt", "data/x.bin"}
	rng := rand.New(rand.NewPCG(7, 11))

	for range 4 {
		ctx, cancel := context.WithCancel(context.Background())
		edited := false
		for i := range peers {
			if rng.IntN(2) == 0 {
				randomEdits(t, peers[i], i, paths, 3, rng)
				edited = true
			}
		}
		if !edited {
			randomEdits(t, peers[0], 0, paths, 3, rng)
		}
		connectMesh(t, ctx, peers)
		waitSameRoot(t, bare(peers)...)
		assertAllEqual(t, peers)
		cancel()
	}
}

// TestSimConcurrentEditsOverCorruptLink converges two writers that edit the same path
// while the link between them corrupts every chunk in flight: verify-by-hash rejects the
// bad bytes and the data is refetched, so keep-both still converges bit-exact.
func TestSimConcurrentEditsOverCorruptLink(t *testing.T) {
	t.Parallel()
	a := newSimPeer(t, 'a')
	b := newSimPeer(t, 'b')
	writeFile(t, a.root, "doc.txt", []byte(strings.Repeat("A", 4096)))
	writeFile(t, b.root, "doc.txt", []byte(strings.Repeat("B", 4096)))
	a.scan(t)
	b.scan(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fc := &faultyConn{}
	fc.corrupt.Store(true)
	sa, sb := memSessionPair(t, ctx, a.peer, b.peer, func(c netio.Conn) netio.Conn { fc.Conn = c; return fc })
	engineOn(t, ctx, sa, a.peer, RoleWriter, a.coord)
	engineOn(t, ctx, sb, b.peer, RoleWriter, b.coord)

	waitForPullAttempt(t, fc)
	fc.corrupt.Store(false) // the swarm heals; the rejected chunks are refetched cleanly

	waitSameRoot(t, a.peer, b.peer)
	assertTreesEqual(t, a.root, b.root)
	got := fileContents(t, a.root)
	if !got[strings.Repeat("A", 4096)] || !got[strings.Repeat("B", 4096)] {
		t.Fatalf("keep-both lost an edit over a corrupt link: %d survivors", len(got))
	}
}

// TestSimConflictStormPreservesEveryEdit hammers a single path from three writers offline,
// then converges; every distinct edit must survive somewhere (winner or conflict copy).
func TestSimConflictStormPreservesEveryEdit(t *testing.T) {
	t.Parallel()
	peers := simPeers(t, 3)
	contents := []string{"alpha version", "bravo version", "charlie version"}
	for i := range peers {
		writeFile(t, peers[i].root, "hot.txt", []byte(contents[i]))
		peers[i].scan(t)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connectMesh(t, ctx, peers)
	waitSameRoot(t, bare(peers)...)
	assertAllEqual(t, peers)

	got := fileContents(t, peers[0].root)
	for _, want := range contents {
		if !got[want] {
			t.Fatalf("conflict storm lost an edit %q; survivors %v", want, got)
		}
	}
}
