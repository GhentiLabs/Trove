package syncengine

import (
	"context"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"
)

const (
	ownerID   = "oooooooooooooooooooooooooooooooooooooooooooooooooooo"
	replicaID = "rrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr"
)

// pseudoRandom returns deterministic, poorly-compressible bytes so a large file
// splits into multiple content-defined chunks.
func pseudoRandom(n int, seed uint64) []byte {
	r := rand.New(rand.NewPCG(seed, 0x9e3779b97f4a7c15))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.Uint64())
	}
	return b
}

func TestConvergeBasicTree(t *testing.T) {
	t.Parallel()
	owner := newPeer(t, ownerID)
	replica := newPeer(t, replicaID)

	writeFile(t, owner.root, "a.txt", []byte("hello world"))
	writeFile(t, owner.root, "dir/b.txt", []byte("nested file contents"))
	writeFile(t, owner.root, "empty.txt", nil)
	if err := os.MkdirAll(filepath.Join(owner.root, "emptydir"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.Symlink("a.txt", filepath.Join(owner.root, "link")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	owner.scan(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startSync(t, ctx, owner, replica)

	waitConverged(t, owner, replica)
	assertTreesEqual(t, owner.root, replica.root)
	assertLeafSetsEqual(t, owner, replica)
}

func TestConvergeMultiChunkFile(t *testing.T) {
	t.Parallel()
	owner := newPeer(t, ownerID)
	replica := newPeer(t, replicaID)

	writeFile(t, owner.root, "big.bin", pseudoRandom(5<<20, 1)) // ~5 MiB → several chunks
	owner.scan(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startSync(t, ctx, owner, replica)

	waitConverged(t, owner, replica)
	assertTreesEqual(t, owner.root, replica.root)
	assertLeafSetsEqual(t, owner, replica)
}

func TestConvergeEmptyFolder(t *testing.T) {
	t.Parallel()
	owner := newPeer(t, ownerID)
	replica := newPeer(t, replicaID)
	owner.scan(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startSync(t, ctx, owner, replica)

	waitConverged(t, owner, replica)
	assertTreesEqual(t, owner.root, replica.root)
}

func TestConvergeDeletePropagates(t *testing.T) {
	t.Parallel()
	owner := newPeer(t, ownerID)
	replica := newPeer(t, replicaID)
	writeFile(t, owner.root, "keep.txt", []byte("stays"))
	writeFile(t, owner.root, "gone.txt", []byte("removed later"))
	owner.scan(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ownerEng, _ := startSync(t, ctx, owner, replica)
	waitConverged(t, owner, replica)

	if err := os.Remove(filepath.Join(owner.root, "gone.txt")); err != nil {
		t.Fatalf("remove: %v", err)
	}
	owner.scan(t)
	ownerEng.Announce(ctx)

	waitConverged(t, owner, replica)
	if _, err := os.Lstat(filepath.Join(replica.root, "gone.txt")); !os.IsNotExist(err) {
		t.Fatalf("replica still has deleted file: err=%v", err)
	}
	assertTreesEqual(t, owner.root, replica.root)
	assertLeafSetsEqual(t, owner, replica)
}

// TestAntiEntropyTransfersOnlyDelta verifies that a small change after an initial
// sync moves only the new chunk (the since-cursor delta), and that the replica, being
// receive-only, never serves a chunk.
func TestAntiEntropyTransfersOnlyDelta(t *testing.T) {
	t.Parallel()
	owner := newPeer(t, ownerID)
	replica := newPeer(t, replicaID)
	writeFile(t, owner.root, "a.txt", []byte("first"))
	owner.scan(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ownerEng, replicaEng := startSync(t, ctx, owner, replica)
	waitConverged(t, owner, replica)
	base := ownerEng.ServedChunks()

	writeFile(t, owner.root, "b.txt", []byte("second"))
	owner.scan(t)
	ownerEng.Announce(ctx)
	waitConverged(t, owner, replica)

	if got := ownerEng.ServedChunks() - base; got != 1 {
		t.Fatalf("anti-entropy served %d chunks for a one-file delta, want 1", got)
	}
	if n := replicaEng.ServedChunks(); n != 0 {
		t.Fatalf("replica served %d chunks; replicas are receive-only", n)
	}
	assertTreesEqual(t, owner.root, replica.root)
	assertLeafSetsEqual(t, owner, replica)
}

func assertLeafSetsEqual(t *testing.T, owner, replica peer) {
	t.Helper()
	if owner.currentRoot(t) != replica.currentRoot(t) {
		t.Fatalf("roots differ: owner %s, replica %s", owner.currentRoot(t), replica.currentRoot(t))
	}
	or, err := owner.model.ListManifests(context.Background())
	if err != nil {
		t.Fatalf("owner ListManifests: %v", err)
	}
	rr, err := replica.model.ListManifests(context.Background())
	if err != nil {
		t.Fatalf("replica ListManifests: %v", err)
	}
	if len(or) != len(rr) {
		t.Fatalf("manifest count: owner %d, replica %d", len(or), len(rr))
	}
	for i := range or {
		if or[i].Manifest.Path != rr[i].Manifest.Path || or[i].ID != rr[i].ID || or[i].Deleted != rr[i].Deleted {
			t.Fatalf("leaf %d mismatch: owner {%s %s del=%v}, replica {%s %s del=%v}",
				i, or[i].Manifest.Path, or[i].ID, or[i].Deleted, rr[i].Manifest.Path, rr[i].ID, rr[i].Deleted)
		}
	}
}
