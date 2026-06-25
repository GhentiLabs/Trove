package syncengine

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/model"
)

// waitReceipt polls until p holds a receipt for peerID whose root matches wantRoot.
func waitReceipt(t *testing.T, p peer, peerID, wantRoot string) model.Receipt {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		r, ok, err := p.model.Receipt(context.Background(), peerID)
		if err != nil {
			t.Fatalf("Receipt(%s): %v", peerID, err)
		}
		if ok && r.Root.String() == wantRoot {
			return r
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no receipt for %s at root %s within deadline", peerID, wantRoot)
	return model.Receipt{}
}

// TestConvergenceReceiptExchanged proves both ends record a receipt once a folder
// converges: the replica acknowledges the owner's root, and the owner learns it.
func TestConvergenceReceiptExchanged(t *testing.T) {
	owner := newPeer(t, ownerID)
	replica := newPeer(t, replicaID)
	writeFile(t, owner.root, "a.txt", []byte("hello"))
	writeFile(t, owner.root, "dir/b.txt", []byte("world"))
	owner.scan(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startSync(t, ctx, owner, replica)
	waitConverged(t, owner, replica)

	want := owner.currentRoot(t)
	rr := waitReceipt(t, replica, ownerID, want)
	if rr.HighWater == 0 {
		t.Fatalf("replica receipt high-water is zero")
	}
	or := waitReceipt(t, owner, replicaID, want)
	if or.SyncedAt.IsZero() {
		t.Fatalf("owner receipt has no timestamp")
	}
	if or.HighWater != rr.HighWater {
		t.Fatalf("receipt high-water mismatch: owner %d, replica %d", or.HighWater, rr.HighWater)
	}
}

// TestStartupRepairRematerializesDeletedFile proves a replica restores a file deleted
// out-of-band under it, sourcing bytes from its local chunk store with no owner.
func TestStartupRepairRematerializesDeletedFile(t *testing.T) {
	owner := newPeer(t, ownerID)
	replica := newPeer(t, replicaID)
	writeFile(t, owner.root, "keep.txt", []byte("keep me"))
	writeFile(t, owner.root, "deep/nested.bin", pseudoRandom(3<<20, 7)) // multi-chunk
	owner.scan(t)

	ctx, cancel := context.WithCancel(context.Background())
	startSync(t, ctx, owner, replica)
	waitConverged(t, owner, replica)
	cancel() // repair must work with the owner detached

	victim := filepath.Join(replica.root, "deep/nested.bin")
	if err := os.Remove(victim); err != nil {
		t.Fatalf("remove victim: %v", err)
	}
	if _, err := os.Lstat(victim); !os.IsNotExist(err) {
		t.Fatalf("victim not actually removed: %v", err)
	}

	cfg := FolderConfig{
		FolderID: folderID, Role: RoleReplica, Root: replica.root,
		FolderCtx: replica.fc, Model: replica.model, Chunks: replica.chunks,
	}
	if err := RepairFolder(context.Background(), cfg, nil); err != nil {
		t.Fatalf("RepairFolder: %v", err)
	}
	assertTreesEqual(t, owner.root, replica.root)
}

// TestOfflineCatchUpThroughEditDeleteRename runs the M4 gate's core convergence shape
// in-process: a replica goes offline, the owner edits, deletes, and renames files, and
// the replica converges bit-exact on reconnect without resurrecting the deletion.
func TestOfflineCatchUpThroughEditDeleteRename(t *testing.T) {
	owner := newPeer(t, ownerID)
	replica := newPeer(t, replicaID)
	writeFile(t, owner.root, "edit.txt", []byte("v1"))
	writeFile(t, owner.root, "delete.txt", []byte("doomed"))
	writeFile(t, owner.root, "move/src.bin", pseudoRandom(2<<20, 3))
	owner.scan(t)

	ctx1, cancel1 := context.WithCancel(context.Background())
	startSync(t, ctx1, owner, replica)
	waitConverged(t, owner, replica)
	firstRoot := owner.currentRoot(t)
	waitReceipt(t, owner, replicaID, firstRoot)
	cancel1() // replica goes offline

	// Owner mutates the folder while the replica is disconnected.
	writeFile(t, owner.root, "edit.txt", []byte("v2-changed"))
	if err := os.Remove(filepath.Join(owner.root, "delete.txt")); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if err := os.Rename(filepath.Join(owner.root, "move/src.bin"), filepath.Join(owner.root, "move/dst.bin")); err != nil {
		t.Fatalf("rename: %v", err)
	}
	owner.scan(t)
	secondRoot := owner.currentRoot(t)
	if secondRoot == firstRoot {
		t.Fatal("owner root did not change after edits")
	}

	// Reconnect on a fresh session and converge.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	startSync(t, ctx2, owner, replica)
	waitConverged(t, owner, replica)

	if _, err := os.Lstat(filepath.Join(replica.root, "delete.txt")); !os.IsNotExist(err) {
		t.Fatalf("deletion resurrected on catch-up: err=%v", err)
	}
	assertTreesEqual(t, owner.root, replica.root)
	assertLeafSetsEqual(t, owner, replica)

	// Receipts reflect the caught-up state on both ends.
	rr := waitReceipt(t, replica, ownerID, secondRoot)
	or := waitReceipt(t, owner, replicaID, secondRoot)
	if rr.HighWater != or.HighWater {
		t.Fatalf("post-catch-up receipt mismatch: owner %d, replica %d", or.HighWater, rr.HighWater)
	}
}
