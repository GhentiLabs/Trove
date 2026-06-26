package model

import (
	"context"
	"errors"
	"maps"
	"testing"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/hasher"
	"github.com/GhentiLabs/Trove/client/internal/manifest"
	"github.com/GhentiLabs/Trove/client/internal/snapshot"
)

func chunkID(t *testing.T, s string) hasher.ChunkID {
	t.Helper()
	return hasher.Sum([]byte(s))
}

func remoteFile(t *testing.T, path string, ownerSeq int64, chunks []manifest.ChunkRef) RemoteManifest {
	t.Helper()
	m := manifest.Manifest{Kind: manifest.KindRegular, Path: path, Mode: 0o644, Chunks: chunks}
	return RemoteManifest{
		Manifest: m,
		ID:       m.ID(),
		Version:  manifest.VersionVector{nodeB: uint64(ownerSeq)},
		OwnerSeq: ownerSeq,
	}
}

func TestApplyRemoteStoresVerbatim(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	rm := remoteFile(t, "a.txt", 5, []manifest.ChunkRef{{ID: chunkID(t, "x"), Length: 1024}})

	if err := s.ApplyRemoteAndAdvance(ctx, []RemoteManifest{rm}, "fld", nodeB, 9, 5); err != nil {
		t.Fatalf("ApplyRemoteAndAdvance: %v", err)
	}

	rec, err := s.GetManifest(ctx, "a.txt")
	if err != nil {
		t.Fatalf("GetManifest: %v", err)
	}
	if rec.ID != rm.ID {
		t.Fatalf("manifest id not preserved")
	}
	if rec.Seq != 5 {
		t.Fatalf("seq = %d, want owner seq 5", rec.Seq)
	}
	if !maps.Equal(rec.Version, rm.Version) {
		t.Fatalf("version vector = %v, want verbatim %v", rec.Version, rm.Version)
	}
	if _, ok := rec.Version[s.NodeID()]; ok {
		t.Fatalf("replica node %q must not appear in the version vector", s.NodeID())
	}

	epoch, hw, ok, err := s.LoadCursor(ctx, "fld", nodeB)
	if err != nil || !ok {
		t.Fatalf("LoadCursor: ok=%v err=%v", ok, err)
	}
	if epoch != 9 || hw != 5 {
		t.Fatalf("cursor = (%d,%d), want (9,5)", epoch, hw)
	}
}

func TestApplyRemotePreservesAuthor(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	rm := remoteFile(t, "a.txt", 5, []manifest.ChunkRef{{ID: chunkID(t, "x"), Length: 1024}})
	rm.Author = nodeB
	rm.AuthoredAt = time.UnixMilli(1700000000123)

	if err := s.ApplyRemoteAndAdvance(ctx, []RemoteManifest{rm}, "fld", nodeB, 9, 5); err != nil {
		t.Fatalf("ApplyRemoteAndAdvance: %v", err)
	}
	rec, err := s.GetManifest(ctx, "a.txt")
	if err != nil {
		t.Fatalf("GetManifest: %v", err)
	}
	if rec.Author != nodeB {
		t.Fatalf("author = %q, want verbatim %q", rec.Author, nodeB)
	}
	if !rec.AuthoredAt.Equal(rm.AuthoredAt) {
		t.Fatalf("authored-at = %v, want verbatim %v", rec.AuthoredAt, rm.AuthoredAt)
	}
}

func TestApplyRemoteConvergesRoot(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	batch := []RemoteManifest{
		remoteFile(t, "a.txt", 1, []manifest.ChunkRef{{ID: chunkID(t, "a"), Length: 10}}),
		remoteFile(t, "dir/b.txt", 2, []manifest.ChunkRef{{ID: chunkID(t, "b"), Length: 20}}),
		remoteFile(t, "c.txt", 3, []manifest.ChunkRef{{ID: chunkID(t, "c"), Length: 30}}),
	}
	if err := s.ApplyRemoteAndAdvance(ctx, batch, "fld", nodeB, 1, 3); err != nil {
		t.Fatalf("ApplyRemoteAndAdvance: %v", err)
	}

	want := snapshot.Set{
		{Path: "a.txt", ManifestID: batch[0].ID},
		{Path: "c.txt", ManifestID: batch[2].ID},
		{Path: "dir/b.txt", ManifestID: batch[1].ID},
	}.Root()

	got, err := s.CurrentRoot(ctx)
	if err != nil {
		t.Fatalf("CurrentRoot: %v", err)
	}
	if got != want {
		t.Fatalf("CurrentRoot = %s, want %s", got, want)
	}
}

func TestApplyRemoteAtomicRollback(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	good := remoteFile(t, "good.txt", 1, []manifest.ChunkRef{{ID: chunkID(t, "g"), Length: 10}})
	bad := remoteFile(t, "bad.txt", 2, []manifest.ChunkRef{{ID: chunkID(t, "b"), Length: 10}})
	bad.ID = good.ID // identity no longer matches the manifest content

	err := s.ApplyRemoteAndAdvance(ctx, []RemoteManifest{good, bad}, "fld", nodeB, 1, 2)
	if !errors.Is(err, ErrCorruptModel) {
		t.Fatalf("err = %v, want ErrCorruptModel", err)
	}
	if _, err := s.GetManifest(ctx, "good.txt"); !errors.Is(err, ErrManifestNotFound) {
		t.Fatalf("good.txt committed despite rollback: %v", err)
	}
	if _, _, ok, _ := s.LoadCursor(ctx, "fld", nodeB); ok {
		t.Fatalf("cursor advanced despite rollback")
	}
}

func TestApplyRemoteIdempotent(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	batch := []RemoteManifest{remoteFile(t, "a.txt", 1, []manifest.ChunkRef{{ID: chunkID(t, "a"), Length: 10}})}
	for range 2 {
		if err := s.ApplyRemoteAndAdvance(ctx, batch, "fld", nodeB, 1, 1); err != nil {
			t.Fatalf("ApplyRemoteAndAdvance: %v", err)
		}
	}
	recs, err := s.ListManifests(ctx)
	if err != nil {
		t.Fatalf("ListManifests: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("manifest count = %d, want 1", len(recs))
	}
}

func TestApplyRemoteTombstoneRemovesFromCurrentRoot(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	live := remoteFile(t, "a.txt", 1, []manifest.ChunkRef{{ID: chunkID(t, "a"), Length: 10}})
	if err := s.ApplyRemoteAndAdvance(ctx, []RemoteManifest{live}, "fld", nodeB, 1, 1); err != nil {
		t.Fatalf("apply live: %v", err)
	}
	tomb := live
	tomb.Deleted = true
	tomb.OwnerSeq = 2
	tomb.Version = manifest.VersionVector{nodeB: 2}
	if err := s.ApplyRemoteAndAdvance(ctx, []RemoteManifest{tomb}, "fld", nodeB, 1, 2); err != nil {
		t.Fatalf("apply tombstone: %v", err)
	}

	want := snapshot.Set{{Path: "a.txt", ManifestID: live.ID, Deleted: true}}.Root()
	got, err := s.CurrentRoot(ctx)
	if err != nil {
		t.Fatalf("CurrentRoot: %v", err)
	}
	if got != want {
		t.Fatalf("CurrentRoot after tombstone = %s, want %s", got, want)
	}
	// A tombstone keeps its chunk refs, so identity re-verification still passes.
	recs, err := s.ListManifests(ctx)
	if err != nil {
		t.Fatalf("ListManifests over a tombstone: %v", err)
	}
	if len(recs) != 1 || !recs[0].Deleted {
		t.Fatalf("expected one tombstone record, got %+v", recs)
	}
}

func TestFolderEpochStable(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	e1, err := s.FolderEpoch(ctx)
	if err != nil {
		t.Fatalf("FolderEpoch: %v", err)
	}
	e2, err := s.FolderEpoch(ctx)
	if err != nil {
		t.Fatalf("FolderEpoch: %v", err)
	}
	if e1 == 0 || e1 != e2 {
		t.Fatalf("epoch unstable or zero: e1=%d e2=%d", e1, e2)
	}
}

func TestHighWaterTracksMaxSeq(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	if hw, err := s.HighWater(ctx); err != nil || hw != 0 {
		t.Fatalf("empty HighWater = %d, err %v, want 0", hw, err)
	}
	for _, p := range []string{"a.txt", "b.txt"} {
		m := manifest.Manifest{Kind: manifest.KindRegular, Path: p, Mode: 0o644, Chunks: []manifest.ChunkRef{{ID: chunkID(t, p), Length: 8}}}
		if _, err := s.PutManifest(ctx, m, Metadata{}); err != nil {
			t.Fatalf("PutManifest %s: %v", p, err)
		}
	}
	if hw, err := s.HighWater(ctx); err != nil || hw != 2 {
		t.Fatalf("HighWater = %d, err %v, want 2", hw, err)
	}
}

func TestLoadCursorAbsent(t *testing.T) {
	s := newStore(t)
	if _, _, ok, err := s.LoadCursor(context.Background(), "fld", nodeB); err != nil || ok {
		t.Fatalf("absent cursor: ok=%v err=%v", ok, err)
	}
}

func TestApplyRemoteRejectsEscapingSymlink(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	for _, target := range []string{"../escape", "/etc/passwd", "a/../../b"} {
		m := manifest.Manifest{Kind: manifest.KindSymlink, Path: "link", SymlinkTarget: target}
		rm := RemoteManifest{Manifest: m, ID: m.ID(), Version: manifest.VersionVector{nodeB: 1}, OwnerSeq: 1}
		if err := s.ApplyRemoteAndAdvance(ctx, []RemoteManifest{rm}, "fld", nodeB, 1, 1); !errors.Is(err, ErrInvalidManifest) {
			t.Fatalf("target %q: err = %v, want ErrInvalidManifest", target, err)
		}
	}
	m := manifest.Manifest{Kind: manifest.KindSymlink, Path: "link", SymlinkTarget: "sibling.txt"}
	rm := RemoteManifest{Manifest: m, ID: m.ID(), Version: manifest.VersionVector{nodeB: 1}, OwnerSeq: 1}
	if err := s.ApplyRemoteAndAdvance(ctx, []RemoteManifest{rm}, "fld", nodeB, 1, 1); err != nil {
		t.Fatalf("relative symlink rejected: %v", err)
	}
}
