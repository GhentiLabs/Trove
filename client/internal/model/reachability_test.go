package model

import (
	"context"
	"testing"

	"github.com/GhentiLabs/Trove/client/internal/hasher"
)

// TestSupersededHistory checks that the query returns exactly the candidate chunks
// a retained snapshot keeps but the current state no longer references, and nothing
// once that snapshot is forgotten.
func TestSupersededHistory(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	mustPut(t, s, regular("a.txt", "v1"), Metadata{Size: 2})
	root, err := s.Cut(ctx)
	if err != nil {
		t.Fatalf("Cut: %v", err)
	}
	mustPut(t, s, regular("a.txt", "v2"), Metadata{Size: 2})

	v1 := hasher.Sum([]byte("v1"))
	v2 := hasher.Sum([]byte("v2"))

	got, err := s.SupersededHistory(ctx, []hasher.ChunkID{v1, v2})
	if err != nil {
		t.Fatalf("SupersededHistory: %v", err)
	}
	if len(got) != 1 || got[0] != v1 {
		t.Fatalf("got %v, want only the snapshot-pinned superseded chunk v1", got)
	}

	if err := s.Forget(ctx, root); err != nil {
		t.Fatalf("Forget: %v", err)
	}
	got, err = s.SupersededHistory(ctx, []hasher.ChunkID{v1, v2})
	if err != nil {
		t.Fatalf("SupersededHistory after forget: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("got %v, want none once the snapshot is forgotten", got)
	}
}

// TestSupersededHistoryIgnoresCurrentChunks guards the anti-join: a chunk a snapshot
// keeps but a different current file still references must not be reported (promoting
// it would strand the live file).
func TestSupersededHistoryIgnoresCurrentChunks(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	mustPut(t, s, regular("a.txt", "shared body", "old unique"), Metadata{Size: 20})
	mustPut(t, s, regular("b.txt", "shared body"), Metadata{Size: 11})
	if _, err := s.Cut(ctx); err != nil {
		t.Fatalf("Cut: %v", err)
	}
	mustPut(t, s, regular("a.txt", "entirely new"), Metadata{Size: 12})

	shared := hasher.Sum([]byte("shared body"))
	old := hasher.Sum([]byte("old unique"))
	got, err := s.SupersededHistory(ctx, []hasher.ChunkID{shared, old})
	if err != nil {
		t.Fatalf("SupersededHistory: %v", err)
	}
	if len(got) != 1 || got[0] != old {
		t.Fatalf("got %v, want only the unique old chunk (shared is still live in b.txt)", got)
	}
}
