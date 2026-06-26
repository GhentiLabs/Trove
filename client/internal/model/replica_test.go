package model

import (
	"context"
	"errors"
	"maps"
	"slices"
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

func remoteFile(t *testing.T, path string, ver int64, chunks []manifest.ChunkRef) RemoteManifest {
	t.Helper()
	m := manifest.Manifest{Kind: manifest.KindRegular, Path: path, Mode: 0o644, Chunks: chunks}
	return RemoteManifest{
		Manifest:   m,
		ID:         m.ID(),
		Version:    manifest.VersionVector{nodeB: uint64(ver)},
		Author:     nodeB,
		AuthoredAt: time.UnixMilli(1000 + ver),
	}
}

// applyRemote runs ApplyRemote with a no-op materialize, the model-only path for tests
// that do not exercise the filesystem.
func applyRemote(ctx context.Context, s *Store, peer string, epoch uint64, hw int64, batch ...RemoteManifest) error {
	return s.ApplyRemote(ctx, "fld", peer, epoch, hw, batch, func([]RemoteManifest) error { return nil })
}

func TestApplyRemoteStoresVersionVerbatim(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	rm := remoteFile(t, "a.txt", 5, []manifest.ChunkRef{{ID: chunkID(t, "x"), Length: 1024}})

	if err := applyRemote(ctx, s, nodeB, 9, 5, rm); err != nil {
		t.Fatalf("ApplyRemote: %v", err)
	}

	rec, err := s.GetManifest(ctx, "a.txt")
	if err != nil {
		t.Fatalf("GetManifest: %v", err)
	}
	if rec.ID != rm.ID {
		t.Fatalf("manifest id not preserved")
	}
	if rec.Seq != 1 {
		t.Fatalf("seq = %d, want fresh local seq 1", rec.Seq)
	}
	if !maps.Equal(rec.Version, rm.Version) {
		t.Fatalf("version vector = %v, want verbatim %v", rec.Version, rm.Version)
	}
	if _, ok := rec.Version[s.NodeID()]; ok {
		t.Fatalf("applying node %q must not appear in the version vector", s.NodeID())
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

	if err := applyRemote(ctx, s, nodeB, 9, 5, rm); err != nil {
		t.Fatalf("ApplyRemote: %v", err)
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
	if err := applyRemote(ctx, s, nodeB, 1, 3, batch...); err != nil {
		t.Fatalf("ApplyRemote: %v", err)
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

	err := applyRemote(ctx, s, nodeB, 1, 2, good, bad)
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
	rm := remoteFile(t, "a.txt", 1, []manifest.ChunkRef{{ID: chunkID(t, "a"), Length: 10}})
	for range 2 {
		if err := applyRemote(ctx, s, nodeB, 1, 1, rm); err != nil {
			t.Fatalf("ApplyRemote: %v", err)
		}
	}
	recs, err := s.ListManifests(ctx)
	if err != nil {
		t.Fatalf("ListManifests: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("manifest count = %d, want 1", len(recs))
	}
	if recs[0].Seq != 1 {
		t.Fatalf("re-applying an identical version reseq'd it: seq = %d, want 1", recs[0].Seq)
	}
}

func TestApplyRemoteTombstoneRemovesFromCurrentRoot(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	live := remoteFile(t, "a.txt", 1, []manifest.ChunkRef{{ID: chunkID(t, "a"), Length: 10}})
	if err := applyRemote(ctx, s, nodeB, 1, 1, live); err != nil {
		t.Fatalf("apply live: %v", err)
	}
	tomb := live
	tomb.Deleted = true
	tomb.Version = manifest.VersionVector{nodeB: 2}
	if err := applyRemote(ctx, s, nodeB, 1, 2, tomb); err != nil {
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

func TestApplyRemoteFastForwardsDominating(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	v1 := remoteFile(t, "a.txt", 1, []manifest.ChunkRef{{ID: chunkID(t, "v1"), Length: 2}})
	v2 := remoteFile(t, "a.txt", 2, []manifest.ChunkRef{{ID: chunkID(t, "v2"), Length: 2}})
	if err := applyRemote(ctx, s, nodeB, 1, 1, v1); err != nil {
		t.Fatalf("apply v1: %v", err)
	}
	if err := applyRemote(ctx, s, nodeB, 1, 2, v2); err != nil {
		t.Fatalf("apply v2: %v", err)
	}
	if rec := mustGet(t, s, "a.txt"); rec.ID != v2.ID {
		t.Fatalf("dominating remote not applied: id=%s want %s", rec.ID, v2.ID)
	}
}

func TestApplyRemoteIgnoresDominated(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	// Local advances to {nodeA:2} through two edits.
	mustPut(t, s, regular("a.txt", "local1"), Metadata{})
	mustPut(t, s, regular("a.txt", "local2"), Metadata{})
	local := mustGet(t, s, "a.txt")

	stale := remoteFile(t, "a.txt", 1, []manifest.ChunkRef{{ID: chunkID(t, "stale"), Length: 2}})
	stale.Version = manifest.VersionVector{s.NodeID(): 1}
	if err := applyRemote(ctx, s, nodeB, 1, 1, stale); err != nil {
		t.Fatalf("apply stale: %v", err)
	}
	if rec := mustGet(t, s, "a.txt"); rec.ID != local.ID {
		t.Fatalf("dominated remote overwrote local: id=%s want %s", rec.ID, local.ID)
	}
}

func TestApplyRemoteConcurrentIdenticalContentJoins(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustPut(t, s, regular("a.txt", "same"), Metadata{}) // local {nodeA:1}
	same := regular("a.txt", "same")
	rm := RemoteManifest{Manifest: same, ID: same.ID(), Version: manifest.VersionVector{nodeB: 1}, Author: nodeB, AuthoredAt: time.UnixMilli(5)}
	if err := applyRemote(ctx, s, nodeB, 1, 1, rm); err != nil {
		t.Fatalf("apply identical concurrent: %v", err)
	}
	rec := mustGet(t, s, "a.txt")
	if rec.ID != same.ID() {
		t.Fatalf("identical content changed: %s", rec.ID)
	}
	want := manifest.VersionVector{s.NodeID(): 1, nodeB: 1}
	if !maps.Equal(rec.Version, want) {
		t.Fatalf("vectors not joined: %v, want %v", rec.Version, want)
	}
}

func TestApplyRemoteConcurrentDifferentContentPicksDeterministicWinner(t *testing.T) {
	ctx := context.Background()

	// Remote authored later wins the path and carries the joined vector.
	s := newStore(t)
	mustPut(t, s, regular("a.txt", "local"), Metadata{}) // local {nodeA:1}, authored ~now
	remoteWins := regular("a.txt", "remote-newer")
	rm := RemoteManifest{Manifest: remoteWins, ID: remoteWins.ID(), Version: manifest.VersionVector{nodeB: 1}, Author: nodeB, AuthoredAt: time.Now().Add(time.Hour)}
	if err := applyRemote(ctx, s, nodeB, 1, 1, rm); err != nil {
		t.Fatalf("apply newer remote: %v", err)
	}
	rec := mustGet(t, s, "a.txt")
	if rec.ID != remoteWins.ID() {
		t.Fatalf("later remote did not win: %s", rec.ID)
	}
	want := manifest.VersionVector{s.NodeID(): 1, nodeB: 1}
	if !maps.Equal(rec.Version, want) {
		t.Fatalf("winner vector not joined: %v, want %v", rec.Version, want)
	}

	// An older remote loses; local keeps the path but absorbs the remote into its vector
	// so it dominates and the conflict does not re-trigger.
	s2 := newStore(t)
	localRec := mustPut(t, s2, regular("a.txt", "local-newer"), Metadata{})
	old := regular("a.txt", "remote-older")
	rmOld := RemoteManifest{Manifest: old, ID: old.ID(), Version: manifest.VersionVector{nodeB: 1}, Author: nodeB, AuthoredAt: time.UnixMilli(1)}
	if err := applyRemote(ctx, s2, nodeB, 1, 1, rmOld); err != nil {
		t.Fatalf("apply older remote: %v", err)
	}
	rec2 := mustGet(t, s2, "a.txt")
	if rec2.ID != localRec {
		t.Fatalf("older remote overwrote local: %s", rec2.ID)
	}
	if !rec2.Version.Dominates(manifest.VersionVector{nodeB: 1}) {
		t.Fatalf("winning local did not absorb the loser's vector: %v", rec2.Version)
	}
}

func conflictingEdit(path, author string, counter uint64, atMs int64, content string) RemoteManifest {
	m := manifest.Manifest{Kind: manifest.KindRegular, Path: path, Mode: 0o644,
		Chunks: []manifest.ChunkRef{{ID: hasher.Sum([]byte(content)), Length: int64(len(content))}}}
	return RemoteManifest{
		Manifest: m, ID: m.ID(), Version: manifest.VersionVector{author: counter},
		Author: author, AuthoredAt: time.UnixMilli(atMs),
	}
}

func TestKeepBothConvergesDeterministically(t *testing.T) {
	ctx := context.Background()
	a := conflictingEdit("a.txt", nodeA, 1, 100, "content from A")
	b := conflictingEdit("a.txt", nodeB, 1, 200, "content from B") // later edit wins

	// Apply the two concurrent edits in both orders; the outcome must be identical.
	s1 := newStore(t)
	if err := applyRemote(ctx, s1, nodeA, 1, 1, a); err != nil {
		t.Fatalf("s1 apply a: %v", err)
	}
	if err := applyRemote(ctx, s1, nodeB, 1, 1, b); err != nil {
		t.Fatalf("s1 apply b: %v", err)
	}

	s2 := newStore(t)
	if err := applyRemote(ctx, s2, nodeB, 1, 1, b); err != nil {
		t.Fatalf("s2 apply b: %v", err)
	}
	if err := applyRemote(ctx, s2, nodeA, 1, 1, a); err != nil {
		t.Fatalf("s2 apply a: %v", err)
	}

	r1, _ := s1.CurrentRoot(ctx)
	r2, _ := s2.CurrentRoot(ctx)
	if r1 != r2 {
		t.Fatalf("keep-both diverged across apply order: %s vs %s", r1, r2)
	}

	// The winner (B) holds the path with the joined vector; the loser (A) survives as a
	// conflict copy carrying A's content and A's vector verbatim.
	win := mustGet(t, s1, "a.txt")
	if win.ID != b.ID {
		t.Fatalf("winner is not B's content: %s", win.ID)
	}
	if !win.Version.Dominates(manifest.VersionVector{nodeA: 1}) || !win.Version.Dominates(manifest.VersionVector{nodeB: 1}) {
		t.Fatalf("winner vector is not the join: %v", win.Version)
	}
	ccPath := ConflictPath("a.txt", nodeA, time.UnixMilli(100))
	cc := mustGet(t, s1, ccPath)
	if !slices.Equal(cc.Manifest.Chunks, a.Manifest.Chunks) {
		t.Fatalf("conflict copy does not hold A's content: %+v", cc.Manifest.Chunks)
	}
	if !maps.Equal(cc.Version, manifest.VersionVector{nodeA: 1}) {
		t.Fatalf("conflict copy vector not verbatim: %v", cc.Version)
	}

	// Idempotent: re-running detection over the resolved state produces nothing new.
	before, _ := s1.ListManifests(ctx)
	if err := applyRemote(ctx, s1, nodeA, 1, 1, a); err != nil {
		t.Fatalf("re-apply a: %v", err)
	}
	if err := applyRemote(ctx, s1, nodeB, 1, 1, b); err != nil {
		t.Fatalf("re-apply b: %v", err)
	}
	after, _ := s1.ListManifests(ctx)
	if len(after) != len(before) {
		t.Fatalf("resolution not idempotent: %d manifests before, %d after", len(before), len(after))
	}
}

func tombstone(path, author string, counter uint64, atMs int64, content string) RemoteManifest {
	rm := conflictingEdit(path, author, counter, atMs, content)
	rm.Deleted = true
	rm.DeletedAt = time.UnixMilli(atMs)
	return rm
}

func TestDeleteVsEditResolvesToEditEitherOrder(t *testing.T) {
	ctx := context.Background()
	edit := conflictingEdit("a.txt", nodeA, 1, 100, "live edit by A")
	del := tombstone("a.txt", nodeB, 1, 200, "B's stale content") // later, but a delete

	// Edit then delete, and delete then edit, must both land on the live edit.
	for _, order := range [][]RemoteManifest{{edit, del}, {del, edit}} {
		s := newStore(t)
		if err := applyRemote(ctx, s, order[0].Author, 1, 1, order[0]); err != nil {
			t.Fatalf("apply first: %v", err)
		}
		if err := applyRemote(ctx, s, order[1].Author, 1, 1, order[1]); err != nil {
			t.Fatalf("apply second: %v", err)
		}
		rec := mustGet(t, s, "a.txt")
		if rec.Deleted {
			t.Fatalf("concurrent edit lost to a delete (order %q,%q)", order[0].Author, order[1].Author)
		}
		if rec.ID != edit.ID {
			t.Fatalf("path does not hold the live edit: %s", rec.ID)
		}
	}
}

func TestDeleteVsEditSameContentConverges(t *testing.T) {
	ctx := context.Background()
	// A keeps content X live; B deletes content X. The tombstone retains X, so both
	// manifests share an id but differ in deleted state — this must not diverge.
	live := conflictingEdit("a.txt", nodeA, 1, 100, "content X")
	del := live
	del.Author, del.Version, del.AuthoredAt = nodeB, manifest.VersionVector{nodeB: 1}, time.UnixMilli(200)
	del.Deleted, del.DeletedAt = true, time.UnixMilli(200)

	s1 := newStore(t)
	_ = applyRemote(ctx, s1, nodeA, 1, 1, live)
	_ = applyRemote(ctx, s1, nodeB, 1, 1, del)

	s2 := newStore(t)
	_ = applyRemote(ctx, s2, nodeB, 1, 1, del)
	_ = applyRemote(ctx, s2, nodeA, 1, 1, live)

	r1, _ := s1.CurrentRoot(ctx)
	r2, _ := s2.CurrentRoot(ctx)
	if r1 != r2 {
		t.Fatalf("live-vs-tombstone of same content diverged: %s vs %s", r1, r2)
	}
	if mustGet(t, s1, "a.txt").Deleted {
		t.Fatal("edit must win delete-vs-edit, even for identical content")
	}
}

func TestDeleteVsDeleteConvergesToTombstone(t *testing.T) {
	ctx := context.Background()
	delA := tombstone("a.txt", nodeA, 1, 100, "A's last content")
	delB := tombstone("a.txt", nodeB, 1, 200, "B's last content")

	s1 := newStore(t)
	_ = applyRemote(ctx, s1, nodeA, 1, 1, delA)
	_ = applyRemote(ctx, s1, nodeB, 1, 1, delB)

	s2 := newStore(t)
	_ = applyRemote(ctx, s2, nodeB, 1, 1, delB)
	_ = applyRemote(ctx, s2, nodeA, 1, 1, delA)

	r1, _ := s1.CurrentRoot(ctx)
	r2, _ := s2.CurrentRoot(ctx)
	if r1 != r2 {
		t.Fatalf("concurrent deletes diverged: %s vs %s", r1, r2)
	}
	recs, _ := s1.ListManifests(ctx)
	if len(recs) != 1 || !recs[0].Deleted {
		t.Fatalf("delete-vs-delete must converge to one tombstone, got %+v", recs)
	}
}

func TestApplyRemoteRejectsMalformedAuthor(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	rm := remoteFile(t, "a.txt", 1, []manifest.ChunkRef{{ID: chunkID(t, "x"), Length: 2}})
	rm.Author = "../../etc/passwd" // would inject parent segments into a conflict path
	if err := applyRemote(ctx, s, nodeB, 1, 1, rm); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("err = %v, want ErrInvalidManifest", err)
	}
	if _, _, ok, _ := s.LoadCursor(ctx, "fld", nodeB); ok {
		t.Fatal("cursor advanced despite a rejected batch")
	}
}

func TestApplyRemoteRejectsEscapingSymlink(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	for _, target := range []string{"../escape", "/etc/passwd", "a/../../b"} {
		m := manifest.Manifest{Kind: manifest.KindSymlink, Path: "link", SymlinkTarget: target}
		rm := RemoteManifest{Manifest: m, ID: m.ID(), Version: manifest.VersionVector{nodeB: 1}, Author: nodeB}
		if err := applyRemote(ctx, s, nodeB, 1, 1, rm); !errors.Is(err, ErrInvalidManifest) {
			t.Fatalf("target %q: err = %v, want ErrInvalidManifest", target, err)
		}
	}
	m := manifest.Manifest{Kind: manifest.KindSymlink, Path: "link", SymlinkTarget: "sibling.txt"}
	rm := RemoteManifest{Manifest: m, ID: m.ID(), Version: manifest.VersionVector{nodeB: 1}, Author: nodeB}
	if err := applyRemote(ctx, s, nodeB, 1, 1, rm); err != nil {
		t.Fatalf("relative symlink rejected: %v", err)
	}
}
