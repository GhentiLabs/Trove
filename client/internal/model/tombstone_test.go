package model

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/manifest"
)

func TestDeleteCreatesTombstone(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustPut(t, s, regular("a.txt", "hello"), Metadata{Mtime: time.Unix(1, 0), Size: 5})
	before := mustGet(t, s, "a.txt")

	if _, err := s.DeleteManifest(ctx, "a.txt"); err != nil {
		t.Fatalf("DeleteManifest: %v", err)
	}
	rec := mustGet(t, s, "a.txt")
	if !rec.Deleted {
		t.Fatal("manifest not marked deleted")
	}
	if rec.Version[nodeA] != before.Version[nodeA]+1 {
		t.Fatalf("delete did not bump version: %v -> %v", before.Version, rec.Version)
	}
	if rec.Seq <= before.Seq {
		t.Fatalf("delete did not bump sequence: %d -> %d", before.Seq, rec.Seq)
	}
	if rec.DeletedAt.IsZero() {
		t.Fatal("tombstone has no deletion time")
	}
}

func TestDeleteMissingIsError(t *testing.T) {
	s := newStore(t)
	if _, err := s.DeleteManifest(context.Background(), "ghost"); err == nil {
		t.Fatal("expected error deleting missing path")
	}
}

func TestDeleteAlreadyDeletedIsNoOp(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustPut(t, s, regular("a.txt", "hello"), Metadata{Size: 5})
	id1, err := s.DeleteManifest(ctx, "a.txt")
	if err != nil {
		t.Fatalf("first delete: %v", err)
	}
	id2, err := s.DeleteManifest(ctx, "a.txt")
	if err != nil {
		t.Fatalf("second delete: %v", err)
	}
	if id1 != id2 {
		t.Fatalf("double delete changed id: %s -> %s", id1, id2)
	}
	if rec := mustGet(t, s, "a.txt"); rec.Version[nodeA] != 2 {
		t.Fatalf("double delete re-bumped version: %v (want this node at 2)", rec.Version)
	}
}

func TestResurrectAfterDelete(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	m := regular("a.txt", "hello")
	mustPut(t, s, m, Metadata{Mtime: time.Unix(1, 0), Size: 5})
	if _, err := s.DeleteManifest(ctx, "a.txt"); err != nil {
		t.Fatalf("DeleteManifest: %v", err)
	}
	tomb := mustGet(t, s, "a.txt")

	mustPut(t, s, m, Metadata{Mtime: time.Unix(2, 0), Size: 5})
	rec := mustGet(t, s, "a.txt")
	if rec.Deleted {
		t.Fatal("resurrected manifest still marked deleted")
	}
	if rec.ID != m.ID() {
		t.Fatal("resurrected id should equal the original content id")
	}
	if rec.Version[nodeA] != tomb.Version[nodeA]+1 {
		t.Fatalf("resurrect did not bump version: %v -> %v", tomb.Version, rec.Version)
	}
}

func TestRenameMovesNoChunks(t *testing.T) {
	s := newStore(t)
	old := regular("old/name.txt", "alpha", "beta")
	mustPut(t, s, old, Metadata{Mtime: time.Unix(1, 0), Size: 9})

	renamed := old
	renamed.Path = "new/name.txt"
	mustPut(t, s, renamed, Metadata{Mtime: time.Unix(1, 0), Size: 9})

	if old.ID() == renamed.ID() {
		t.Fatal("rename should change the manifest id (path is part of identity)")
	}
	oldChunks := chunkIDs(mustGet(t, s, "old/name.txt").Manifest.Chunks)
	newChunks := chunkIDs(mustGet(t, s, "new/name.txt").Manifest.Chunks)
	if !slices.Equal(oldChunks, newChunks) {
		t.Fatalf("rename changed chunk set:\n old %v\n new %v", oldChunks, newChunks)
	}
}

func chunkIDs(refs []manifest.ChunkRef) []string {
	out := make([]string, len(refs))
	for i, r := range refs {
		out[i] = r.ID.String()
	}
	return out
}

func TestListAndManifestsSince(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustPut(t, s, regular("a", "1"), Metadata{Size: 1})
	cut := mustGet(t, s, "a").Seq
	mustPut(t, s, regular("b", "2"), Metadata{Size: 1})
	mustPut(t, s, regular("c", "3"), Metadata{Size: 1})

	all, err := s.ListManifests(ctx)
	if err != nil {
		t.Fatalf("ListManifests: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("ListManifests returned %d, want 3", len(all))
	}

	since, err := s.ManifestsSince(ctx, cut)
	if err != nil {
		t.Fatalf("ManifestsSince: %v", err)
	}
	if len(since) != 2 {
		t.Fatalf("ManifestsSince(%d) returned %d, want 2 (b,c)", cut, len(since))
	}
	for _, r := range since {
		if r.Seq <= cut {
			t.Fatalf("ManifestsSince returned seq %d <= cursor %d", r.Seq, cut)
		}
	}
}
