package model

import (
	"context"
	"errors"
	"testing"
)

func TestReachableLogicalBytes(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	mustPut(t, s, regular("a.txt", "0123456789"), Metadata{Size: 10})
	if got := reachable(t, s); got != 10 {
		t.Fatalf("reachable after put = %d, want 10", got)
	}
	if _, err := s.Cut(ctx); err != nil {
		t.Fatalf("Cut: %v", err)
	}
	mustPut(t, s, regular("a.txt", "abcdefghij"), Metadata{Size: 10})
	if got := reachable(t, s); got != 20 {
		t.Fatalf("reachable after edit = %d, want 20 (current + snapshot history)", got)
	}
}

func TestPruneHistoryToFitNoop(t *testing.T) {
	s := newStore(t)
	s.quota = 100
	ctx := context.Background()

	mustPut(t, s, regular("a.txt", "0123456789"), Metadata{Size: 10})
	if _, err := s.Cut(ctx); err != nil {
		t.Fatalf("Cut: %v", err)
	}
	if err := s.PruneHistoryToFit(ctx); err != nil {
		t.Fatalf("PruneHistoryToFit: %v", err)
	}
	if snaps := listSnaps(t, s); len(snaps) != 1 {
		t.Fatalf("snapshots = %d, want 1 (nothing pruned)", len(snaps))
	}
}

func TestPruneHistoryToFitPrunesOldest(t *testing.T) {
	s := newStore(t)
	s.quota = 15
	ctx := context.Background()

	mustPut(t, s, regular("a.txt", "AAAAAAAAAA"), Metadata{Size: 10})
	if _, err := s.Cut(ctx); err != nil {
		t.Fatalf("Cut snap1: %v", err)
	}
	mustPut(t, s, regular("a.txt", "BBBBBBBBBB"), Metadata{Size: 10})
	snap2, err := s.Cut(ctx)
	if err != nil {
		t.Fatalf("Cut snap2: %v", err)
	}
	if err := s.PruneHistoryToFit(ctx); err != nil {
		t.Fatalf("PruneHistoryToFit: %v", err)
	}
	snaps := listSnaps(t, s)
	if len(snaps) != 1 || snaps[0].Root != snap2 {
		t.Fatalf("snapshots = %+v, want only snap2 retained", snaps)
	}
	if got := reachable(t, s); got != 10 {
		t.Fatalf("reachable after prune = %d, want 10", got)
	}
}

func TestPruneHistoryToFitRejectsAndKeepsCurrent(t *testing.T) {
	s := newStore(t)
	s.quota = 15
	ctx := context.Background()

	mustPut(t, s, regular("a.txt", "AAAAAAAAAA"), Metadata{Size: 10})
	mustPut(t, s, regular("b.txt", "BBBBBBBBBB"), Metadata{Size: 10})
	if err := s.PruneHistoryToFit(ctx); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("PruneHistoryToFit err = %v, want ErrQuotaExceeded", err)
	}
	if _, err := s.GetManifest(ctx, "a.txt"); err != nil {
		t.Fatalf("current file evicted: %v", err)
	}
	if _, err := s.GetManifest(ctx, "b.txt"); err != nil {
		t.Fatalf("current file evicted: %v", err)
	}
}

func TestPruneHistoryToFitUnlimited(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	mustPut(t, s, regular("a.txt", "0123456789"), Metadata{Size: 10})
	mustPut(t, s, regular("b.txt", "abcdefghij"), Metadata{Size: 10})
	if _, err := s.Cut(ctx); err != nil {
		t.Fatalf("Cut: %v", err)
	}
	if err := s.PruneHistoryToFit(ctx); err != nil {
		t.Fatalf("PruneHistoryToFit unlimited: %v", err)
	}
	if snaps := listSnaps(t, s); len(snaps) != 1 {
		t.Fatalf("snapshots = %d, want 1 (unlimited never prunes)", len(snaps))
	}
}

// TestPruneHistoryToFitToleratesRecurringRoot exercises a repeated snapshot root, where
// forgetting one occurrence clears the other the loop later revisits.
func TestPruneHistoryToFitToleratesRecurringRoot(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	mustPut(t, s, regular("a.txt", "AAAAAAAAAA"), Metadata{Size: 10})
	if _, err := s.Cut(ctx); err != nil {
		t.Fatalf("cut A1: %v", err)
	}
	mustPut(t, s, regular("a.txt", "BBBBBBBBBB"), Metadata{Size: 10})
	if _, err := s.Cut(ctx); err != nil {
		t.Fatalf("cut B: %v", err)
	}
	mustPut(t, s, regular("a.txt", "AAAAAAAAAA"), Metadata{Size: 10})
	if _, err := s.Cut(ctx); err != nil {
		t.Fatalf("cut A2: %v", err)
	}
	if snaps := listSnaps(t, s); len(snaps) != 3 {
		t.Fatalf("want 3 snapshots including a recurring root, got %d", len(snaps))
	}

	s.quota = 5
	if err := s.PruneHistoryToFit(ctx); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("PruneHistoryToFit err = %v, want ErrQuotaExceeded", err)
	}
	if _, err := s.GetManifest(ctx, "a.txt"); err != nil {
		t.Fatalf("current file evicted: %v", err)
	}
}

func reachable(t *testing.T, s *Store) int64 {
	t.Helper()
	n, err := s.ReachableLogicalBytes(context.Background())
	if err != nil {
		t.Fatalf("ReachableLogicalBytes: %v", err)
	}
	return n
}

func listSnaps(t *testing.T, s *Store) []Snapshot {
	t.Helper()
	snaps, err := s.ListSnapshots(context.Background())
	if err != nil {
		t.Fatalf("ListSnapshots: %v", err)
	}
	return snaps
}
