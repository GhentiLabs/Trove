package model

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/snapshot"
)

func leafFor(snap Snapshot, path string) (snapshot.Leaf, bool) {
	i := slices.IndexFunc(snap.Leaves, func(l snapshot.Leaf) bool { return l.Path == path })
	if i < 0 {
		return snapshot.Leaf{}, false
	}
	return snap.Leaves[i], true
}

func TestCutEmptyIsEmptyRoot(t *testing.T) {
	s := newStore(t)
	root, err := s.Cut(context.Background())
	if err != nil {
		t.Fatalf("Cut: %v", err)
	}
	if want := (snapshot.Set{}).Root(); root != want {
		t.Fatalf("empty cut root = %s, want %s", root, want)
	}
}

func TestCutAndGetSnapshot(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustPut(t, s, regular("a", "1"), Metadata{Size: 1})
	mustPut(t, s, regular("b", "2"), Metadata{Size: 1})

	root, err := s.Cut(ctx)
	if err != nil {
		t.Fatalf("Cut: %v", err)
	}
	snap, err := s.GetSnapshot(ctx, root)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if snap.Root != root || len(snap.Leaves) != 2 || snap.CreatedBy != nodeA {
		t.Fatalf("snapshot = %+v", snap)
	}
	if snap.Parent != (snapshot.Root{}) {
		t.Fatalf("first snapshot has a parent: %s", snap.Parent)
	}
}

func TestCutSkipsUnchanged(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustPut(t, s, regular("a", "1"), Metadata{Size: 1})
	r1, _ := s.Cut(ctx)
	r2, err := s.Cut(ctx)
	if err != nil {
		t.Fatalf("Cut: %v", err)
	}
	if r1 != r2 {
		t.Fatalf("unchanged re-cut changed root: %s -> %s", r1, r2)
	}
	if n := countSnapshots(t, s); n != 1 {
		t.Fatalf("unchanged re-cut created a duplicate snapshot: %d total", n)
	}
}

func TestCutStructuralSharingAndParentChain(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustPut(t, s, regular("a", "1"), Metadata{Size: 1})
	mustPut(t, s, regular("b", "2"), Metadata{Size: 1})
	r1, _ := s.Cut(ctx)
	aID := mustGet(t, s, "a").ID

	mustPut(t, s, regular("b", "two-changed"), Metadata{Size: 11})
	r2, _ := s.Cut(ctx)

	if r1 == r2 {
		t.Fatal("editing a file did not change the root")
	}
	if mustGet(t, s, "a").ID != aID {
		t.Fatal("unchanged file's manifest id was not preserved byte-for-byte")
	}
	snap2, err := s.GetSnapshot(ctx, r2)
	if err != nil {
		t.Fatalf("GetSnapshot r2: %v", err)
	}
	if snap2.Parent != r1 {
		t.Fatalf("parent = %s, want %s", snap2.Parent, r1)
	}
	snap1, _ := s.GetSnapshot(ctx, r1)
	if snap1.Parent != (snapshot.Root{}) {
		t.Fatalf("root snapshot has a parent: %s", snap1.Parent)
	}
}

func TestSnapshotRecordsTombstone(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustPut(t, s, regular("a", "1"), Metadata{Size: 1})
	if _, err := s.DeleteManifest(ctx, "a"); err != nil {
		t.Fatalf("DeleteManifest: %v", err)
	}
	root, _ := s.Cut(ctx)
	snap, _ := s.GetSnapshot(ctx, root)
	l, ok := leafFor(snap, "a")
	if !ok || !l.Deleted {
		t.Fatalf("tombstone not present as deleted leaf: %+v", snap.Leaves)
	}
}

func TestDiffSnapshots(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustPut(t, s, regular("a", "1"), Metadata{Size: 1})
	mustPut(t, s, regular("b", "2"), Metadata{Size: 1})
	r1, _ := s.Cut(ctx)

	mustPut(t, s, regular("a", "1-edited"), Metadata{Size: 8})
	mustPut(t, s, regular("c", "3"), Metadata{Size: 1})
	if _, err := s.DeleteManifest(ctx, "b"); err != nil {
		t.Fatalf("DeleteManifest: %v", err)
	}
	r2, _ := s.Cut(ctx)

	d, err := s.DiffSnapshots(ctx, r1, r2)
	if err != nil {
		t.Fatalf("DiffSnapshots: %v", err)
	}
	if len(d.Added) != 1 || d.Added[0].Path != "c" {
		t.Fatalf("added = %+v", d.Added)
	}
	changed := map[string]bool{}
	for _, c := range d.Changed {
		changed[c.After.Path] = true
	}
	if !changed["a"] || !changed["b"] || len(d.Changed) != 2 {
		t.Fatalf("changed = %+v, want a (edit) and b (deletion)", d.Changed)
	}
}

func TestDiffSnapshotMissingRoot(t *testing.T) {
	s := newStore(t)
	var bogus snapshot.Root
	bogus[0] = 0xFF
	if _, err := s.DiffSnapshots(context.Background(), bogus, bogus); !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("got %v, want ErrSnapshotNotFound", err)
	}
}

func TestSweepTombstones(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustPut(t, s, regular("a", "1"), Metadata{Size: 1})
	if _, err := s.DeleteManifest(ctx, "a"); err != nil {
		t.Fatalf("DeleteManifest: %v", err)
	}
	root, _ := s.Cut(ctx)

	if n, err := s.SweepTombstones(ctx, time.Now(), math.MaxInt64); err != nil || n != 0 {
		t.Fatalf("premature sweep removed %d (err %v), want 0", n, err)
	}
	if _, err := s.GetManifest(ctx, "a"); err != nil {
		t.Fatalf("tombstone gone before expiry: %v", err)
	}

	future := time.Now().Add(TombstoneLifetime + time.Hour)
	if n, err := s.SweepTombstones(ctx, future, math.MaxInt64); err != nil || n != 1 {
		t.Fatalf("expiry sweep removed %d (err %v), want 1", n, err)
	}
	if _, err := s.GetManifest(ctx, "a"); !errors.Is(err, ErrManifestNotFound) {
		t.Fatalf("swept tombstone still present: %v", err)
	}

	snap, _ := s.GetSnapshot(ctx, root)
	if l, ok := leafFor(snap, "a"); !ok || !l.Deleted {
		t.Fatal("sweep destroyed historical tombstone in retained snapshot")
	}
}

// TestSweepTombstonesConvergenceGate proves a deletion is reaped only after every
// known replica has converged past it, even once its retention has expired.
func TestSweepTombstonesConvergenceGate(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustPut(t, s, regular("a", "1"), Metadata{Size: 1})
	id, err := s.DeleteManifest(ctx, "a")
	if err != nil {
		t.Fatalf("DeleteManifest: %v", err)
	}
	_ = id
	rec, err := s.GetManifest(ctx, "a")
	if err != nil {
		t.Fatalf("GetManifest: %v", err)
	}
	tombSeq := rec.Seq
	future := time.Now().Add(TombstoneLifetime + time.Hour)

	// Expired, but the only replica has not yet reached the tombstone's sequence.
	if n, err := s.SweepTombstones(ctx, future, tombSeq-1); err != nil || n != 0 {
		t.Fatalf("sweep removed %d (err %v) while a replica was behind, want 0", n, err)
	}
	if _, err := s.GetManifest(ctx, "a"); err != nil {
		t.Fatalf("tombstone reaped before convergence: %v", err)
	}

	// Once the replica converges past it, the expired tombstone is reaped.
	if n, err := s.SweepTombstones(ctx, future, tombSeq); err != nil || n != 1 {
		t.Fatalf("sweep removed %d (err %v) after convergence, want 1", n, err)
	}
	if _, err := s.GetManifest(ctx, "a"); !errors.Is(err, ErrManifestNotFound) {
		t.Fatalf("converged tombstone still present: %v", err)
	}
}

// TestConvergedHighWater checks the per-epoch minimum receipt high-water that gates
// tombstone reaping.
func TestConvergedHighWater(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	root, err := s.CurrentRoot(ctx)
	if err != nil {
		t.Fatalf("CurrentRoot: %v", err)
	}

	if _, ok, err := s.ConvergedHighWater(ctx, 7); err != nil || ok {
		t.Fatalf("empty receipts: ok=%v err=%v, want ok=false", ok, err)
	}

	now := time.Now()
	must := func(peer string, epoch uint64, hw int64) {
		if err := s.RecordReceipt(ctx, Receipt{PeerID: peer, Root: root, Epoch: epoch, HighWater: hw, SyncedAt: now}); err != nil {
			t.Fatalf("RecordReceipt: %v", err)
		}
	}
	must("p1", 7, 40)
	must("p2", 7, 25)
	must("p3", 9, 5) // different epoch, must be ignored

	hw, ok, err := s.ConvergedHighWater(ctx, 7)
	if err != nil || !ok || hw != 25 {
		t.Fatalf("ConvergedHighWater(7) = (%d, %v, %v), want (25, true, nil)", hw, ok, err)
	}

	got, ok, err := s.Receipt(ctx, "p2")
	if err != nil || !ok || got.HighWater != 25 || got.Root != root {
		t.Fatalf("Receipt(p2) = (%+v, %v, %v), want hw=25 root match", got, ok, err)
	}
	all, err := s.Receipts(ctx)
	if err != nil || len(all) != 3 {
		t.Fatalf("Receipts len=%d err=%v, want 3", len(all), err)
	}
}

func TestHistoryIsStructurallyShared(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	const files, edits = 50, 10
	for i := range files {
		mustPut(t, s, regular(fmt.Sprintf("f%02d", i), fmt.Sprintf("content-%d", i)), Metadata{Size: 1})
	}
	if _, err := s.Cut(ctx); err != nil {
		t.Fatalf("Cut: %v", err)
	}
	for k := range edits {
		mustPut(t, s, regular("f00", fmt.Sprintf("edit-%d", k)), Metadata{Size: 1})
		if _, err := s.Cut(ctx); err != nil {
			t.Fatalf("Cut: %v", err)
		}
	}

	var distinct int
	if err := s.db.QueryRow(ctx, `SELECT COUNT(DISTINCT manifest_id) FROM snapshot_manifests`).Scan(&distinct); err != nil {
		t.Fatalf("count distinct: %v", err)
	}
	if want := files + edits; distinct != want {
		t.Fatalf("distinct file-versions across %d snapshots = %d, want %d (one new version per edit, the rest shared)", edits+1, distinct, want)
	}
}

func countSnapshots(t *testing.T, s *Store) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow(context.Background(), `SELECT COUNT(*) FROM snapshots`).Scan(&n); err != nil {
		t.Fatalf("count snapshots: %v", err)
	}
	return n
}
