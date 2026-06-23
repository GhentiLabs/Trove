package model

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/hasher"
	"github.com/GhentiLabs/Trove/client/internal/manifest"
)

func regular(path string, chunks ...string) manifest.Manifest {
	var refs []manifest.ChunkRef
	for _, c := range chunks {
		refs = append(refs, manifest.ChunkRef{ID: hasher.Sum([]byte(c)), Length: int64(len(c))})
	}
	return manifest.Manifest{Kind: manifest.KindRegular, Path: path, Mode: 0o644, Chunks: refs}
}

func mustPut(t *testing.T, s *Store, m manifest.Manifest, md Metadata) manifest.ID {
	t.Helper()
	id, err := s.PutManifest(context.Background(), m, md)
	if err != nil {
		t.Fatalf("PutManifest %q: %v", m.Path, err)
	}
	return id
}

func mustGet(t *testing.T, s *Store, path string) Record {
	t.Helper()
	rec, err := s.GetManifest(context.Background(), path)
	if err != nil {
		t.Fatalf("GetManifest %q: %v", path, err)
	}
	return rec
}

func TestPutGetRoundTrip(t *testing.T) {
	s := newStore(t)
	m := regular("a.txt", "hello", "world")
	id := mustPut(t, s, m, Metadata{Mtime: time.Unix(100, 0), Size: 10})

	rec := mustGet(t, s, "a.txt")
	if rec.ID != id || rec.ID != m.ID() {
		t.Fatalf("id mismatch: rec=%s put=%s computed=%s", rec.ID, id, m.ID())
	}
	if !slices.Equal(rec.Manifest.Chunks, m.Chunks) {
		t.Fatalf("chunks not round-tripped: %+v", rec.Manifest.Chunks)
	}
	if rec.Version[nodeA] != 1 || len(rec.Version) != 1 {
		t.Fatalf("version vector = %v, want {%s:1}", rec.Version, nodeA)
	}
	if rec.Seq != 1 {
		t.Fatalf("seq = %d, want 1", rec.Seq)
	}
	if rec.Deleted {
		t.Fatal("new manifest marked deleted")
	}
	if rec.Metadata.Size != 10 {
		t.Fatalf("metadata size = %d, want 10", rec.Metadata.Size)
	}
}

func TestPutIsIdentityIdempotent(t *testing.T) {
	s := newStore(t)
	m := regular("a.txt", "hello")
	id1 := mustPut(t, s, m, Metadata{Mtime: time.Unix(100, 0), Size: 5})
	rec1 := mustGet(t, s, "a.txt")

	id2 := mustPut(t, s, m, Metadata{Mtime: time.Unix(200, 0), Size: 5})
	rec2 := mustGet(t, s, "a.txt")

	if id2 != id1 {
		t.Fatalf("identical content produced different id: %s vs %s", id1, id2)
	}
	if rec2.Seq != rec1.Seq {
		t.Fatalf("touch bumped sequence: %d -> %d", rec1.Seq, rec2.Seq)
	}
	if rec2.Version[nodeA] != rec1.Version[nodeA] {
		t.Fatalf("touch bumped version vector: %v -> %v", rec1.Version, rec2.Version)
	}
	if !rec2.Metadata.Mtime.Equal(time.Unix(200, 0)) {
		t.Fatalf("stat not refreshed: mtime = %v", rec2.Metadata.Mtime)
	}
}

func TestEditBumpsVersionAndSequence(t *testing.T) {
	s := newStore(t)
	md := Metadata{Mtime: time.Unix(100, 0), Size: 5}
	mustPut(t, s, regular("a.txt", "hello"), md)
	rec1 := mustGet(t, s, "a.txt")

	mustPut(t, s, regular("a.txt", "hello there, world"), md)
	rec2 := mustGet(t, s, "a.txt")

	if rec2.ID == rec1.ID {
		t.Fatal("content change did not change id")
	}
	if rec2.Version[nodeA] != 2 {
		t.Fatalf("version = %v, want this node at 2", rec2.Version)
	}
	if rec2.Seq <= rec1.Seq {
		t.Fatalf("sequence not strictly greater: %d -> %d", rec1.Seq, rec2.Seq)
	}
}

func TestSequenceStrictlyIncreasesAcrossPaths(t *testing.T) {
	s := newStore(t)
	md := Metadata{Mtime: time.Unix(100, 0), Size: 1}
	mustPut(t, s, regular("a", "x"), md)
	mustPut(t, s, regular("b", "y"), md)
	if a, b := mustGet(t, s, "a").Seq, mustGet(t, s, "b").Seq; b <= a {
		t.Fatalf("sequence not increasing across paths: a=%d b=%d", a, b)
	}
}

func TestGetMissingManifest(t *testing.T) {
	s := newStore(t)
	if _, err := s.GetManifest(context.Background(), "nope"); err == nil {
		t.Fatal("expected error for missing manifest")
	}
}

func TestGetManifestDetectsCorruption(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustPut(t, s, regular("a.txt", "hello"), Metadata{Size: 5})
	if _, err := s.db.Exec(ctx,
		`UPDATE manifests SET manifest_id = X'0000000000000000000000000000000000000000000000000000000000000000' WHERE path = 'a.txt'`); err != nil {
		t.Fatalf("corrupt db: %v", err)
	}
	if _, err := s.GetManifest(ctx, "a.txt"); !errors.Is(err, ErrCorruptModel) {
		t.Fatalf("got %v, want ErrCorruptModel", err)
	}
}
