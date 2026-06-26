package syncengine

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/model"
	"github.com/GhentiLabs/Trove/client/internal/wire/wirepb"
)

// waitReceipt polls until p holds a receipt of kind for peerID whose root matches wantRoot.
func waitReceipt(t *testing.T, p peer, kind model.ReceiptKind, peerID, wantRoot string) model.Receipt {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		r, ok, err := p.model.Receipt(context.Background(), kind, peerID)
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
	rr := waitReceipt(t, replica, model.LocalSync, ownerID, want)
	if rr.HighWater == 0 {
		t.Fatalf("replica receipt high-water is zero")
	}
	or := waitReceipt(t, owner, model.InboundAck, replicaID, want)
	if or.SyncedAt.IsZero() {
		t.Fatalf("owner receipt has no timestamp")
	}
	if or.HighWater != rr.HighWater {
		t.Fatalf("receipt high-water mismatch: owner %d, replica %d", or.HighWater, rr.HighWater)
	}
}

// TestReceiptValidationRejectsBadEpochAndCapsHighWater proves the owner trusts a
// receipt only as far as it can check it: a wrong epoch is rejected outright and an
// inflated high-water is capped to what the owner has produced, so a hostile replica
// cannot move the tombstone-reaping gate past a sequence it never reached.
func TestReceiptValidationRejectsBadEpochAndCapsHighWater(t *testing.T) {
	owner := newPeer(t, ownerID)
	replica := newPeer(t, replicaID)
	writeFile(t, owner.root, "a.txt", []byte("hello"))
	writeFile(t, owner.root, "b.txt", []byte("world"))
	owner.scan(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ownerEng, _ := startSync(t, ctx, owner, replica)
	waitConverged(t, owner, replica)

	want := owner.currentRoot(t)
	base := waitReceipt(t, owner, model.InboundAck, replicaID, want)
	bg := context.Background()
	epoch, err := owner.model.FolderEpoch(bg)
	if err != nil {
		t.Fatalf("FolderEpoch: %v", err)
	}
	curRoot, err := owner.model.CurrentRoot(bg)
	if err != nil {
		t.Fatalf("CurrentRoot: %v", err)
	}
	rootBytes := curRoot.Bytes()

	// Wrong epoch: rejected; the genuine receipt is untouched.
	ownerEng.recordReceipt(bg, &wirepb.SyncReceipt{
		FolderId: folderID, SnapshotRoot: rootBytes, IndexEpochId: epoch + 999, HighWaterSequence: base.HighWater + 5,
	})
	got, _, err := owner.model.Receipt(bg, model.InboundAck, replicaID)
	if err != nil {
		t.Fatalf("Receipt: %v", err)
	}
	if got.HighWater != base.HighWater || got.Epoch != epoch {
		t.Fatalf("wrong-epoch receipt mutated state: %+v (base hw %d, epoch %d)", got, base.HighWater, epoch)
	}

	// Inflated high-water: capped to the owner's max sequence.
	ownerMax, err := owner.model.HighWater(bg)
	if err != nil {
		t.Fatalf("HighWater: %v", err)
	}
	ownerEng.recordReceipt(bg, &wirepb.SyncReceipt{
		FolderId: folderID, SnapshotRoot: rootBytes, IndexEpochId: epoch, HighWaterSequence: ownerMax + 1_000_000,
	})
	got, _, err = owner.model.Receipt(bg, model.InboundAck, replicaID)
	if err != nil {
		t.Fatalf("Receipt: %v", err)
	}
	if got.HighWater != ownerMax {
		t.Fatalf("inflated high-water not capped: got %d, want %d", got.HighWater, ownerMax)
	}
}

// TestStartupRepairRestoresDirAndSymlink covers the directory and symlink repair
// branches: both are recreated when removed out-of-band under the replica.
func TestStartupRepairRestoresDirAndSymlink(t *testing.T) {
	owner := newPeer(t, ownerID)
	replica := newPeer(t, replicaID)
	if err := os.MkdirAll(filepath.Join(owner.root, "d/sub"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	writeFile(t, owner.root, "d/sub/f.txt", []byte("inside"))
	if err := os.Symlink("d/sub/f.txt", filepath.Join(owner.root, "link")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	owner.scan(t)

	ctx, cancel := context.WithCancel(context.Background())
	startSync(t, ctx, owner, replica)
	waitConverged(t, owner, replica)
	cancel()

	if err := os.RemoveAll(filepath.Join(replica.root, "d")); err != nil {
		t.Fatalf("rm dir: %v", err)
	}
	if err := os.Remove(filepath.Join(replica.root, "link")); err != nil {
		t.Fatalf("rm link: %v", err)
	}

	cfg := FolderConfig{
		FolderID: folderID, Role: RoleReader, Root: replica.root,
		FolderCtx: replica.fc, Model: replica.model, Chunks: replica.chunks,
	}
	if err := RepairFolder(context.Background(), cfg, nil); err != nil {
		t.Fatalf("RepairFolder: %v", err)
	}
	assertTreesEqual(t, owner.root, replica.root)
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
		FolderID: folderID, Role: RoleReader, Root: replica.root,
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
	waitReceipt(t, owner, model.InboundAck, replicaID, firstRoot)
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
	rr := waitReceipt(t, replica, model.LocalSync, ownerID, secondRoot)
	or := waitReceipt(t, owner, model.InboundAck, replicaID, secondRoot)
	if rr.HighWater != or.HighWater {
		t.Fatalf("post-catch-up receipt mismatch: owner %d, replica %d", or.HighWater, rr.HighWater)
	}
}
