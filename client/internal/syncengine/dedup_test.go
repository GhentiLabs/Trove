package syncengine

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// TestRenameTransfersNoChunkData verifies the dedup/rename invariant: after a large
// file converges, renaming it on the owner moves no chunk data — the replica already
// holds every chunk, so the owner serves none during the rename's convergence.
func TestRenameTransfersNoChunkData(t *testing.T) {
	t.Parallel()
	owner := newPeer(t, ownerID)
	replica := newPeer(t, replicaID)
	writeFile(t, owner.root, "big.bin", pseudoRandom(3<<20, 2))
	owner.scan(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ownerEng, _ := startSync(t, ctx, owner, replica)
	waitConverged(t, owner, replica)

	served := ownerEng.ServedChunks()
	if served == 0 {
		t.Fatal("expected chunks served during the initial sync")
	}

	if err := os.Rename(filepath.Join(owner.root, "big.bin"), filepath.Join(owner.root, "renamed.bin")); err != nil {
		t.Fatalf("rename: %v", err)
	}
	owner.scan(t)
	ownerEng.Announce(ctx)
	waitConverged(t, owner, replica)

	if got := ownerEng.ServedChunks(); got != served {
		t.Fatalf("rename served %d chunks, want 0 (dedup should reuse local chunks)", got-served)
	}
	assertTreesEqual(t, owner.root, replica.root)
	if _, err := os.Lstat(filepath.Join(replica.root, "big.bin")); !os.IsNotExist(err) {
		t.Fatalf("old path still present on replica: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(replica.root, "renamed.bin")); err != nil {
		t.Fatalf("renamed path missing on replica: %v", err)
	}
}
