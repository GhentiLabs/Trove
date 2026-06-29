package gc

import (
	"bytes"
	"context"
	"errors"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/chunkstore"
	"github.com/GhentiLabs/Trove/client/internal/hasher"
	"github.com/GhentiLabs/Trove/client/internal/manifest"
	"github.com/GhentiLabs/Trove/client/internal/model"
	"github.com/GhentiLabs/Trove/client/internal/storage"
)

func genData(n int, seed uint64) []byte {
	r := rand.New(rand.NewPCG(seed, 0x9e3779b9))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.Uint64())
	}
	return b
}

// ingestClone writes content to diskPath, stores it as a clone, and records the
// manifest under name, as the owner scanner does.
func ingestClone(t *testing.T, ms *model.Store, cs *chunkstore.Store, name, diskPath string, content []byte) manifest.ID {
	t.Helper()
	ctx := context.Background()
	if err := os.WriteFile(diskPath, content, 0o644); err != nil {
		t.Fatalf("write %q: %v", diskPath, err)
	}
	refs, err := cs.IngestClone(ctx, diskPath)
	if err != nil {
		t.Fatalf("IngestClone %q: %v", diskPath, err)
	}
	mid, err := ms.PutManifest(ctx, manifest.Manifest{Kind: manifest.KindRegular, Path: name, Mode: 0o644, Chunks: refs}, model.Metadata{Size: int64(len(content))})
	if err != nil {
		t.Fatalf("PutManifest: %v", err)
	}
	return mid
}

const testNode = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

var fc = chunkstore.FolderContext{}

func newStores(t *testing.T) (*model.Store, *chunkstore.Store) {
	t.Helper()
	dir := t.TempDir()
	cdb, err := storage.Open(storage.Options{Path: filepath.Join(dir, "chunks.db"), MaxOpenConns: 8})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cdb.Close() })
	cs, err := chunkstore.Open(chunkstore.Options{DB: cdb, BlobDir: filepath.Join(dir, "blobs")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	mdb, err := storage.Open(storage.Options{Path: filepath.Join(dir, "state.db"), MaxOpenConns: 8})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mdb.Close() })
	ms, err := model.Open(model.Options{DB: mdb, NodeID: testNode})
	if err != nil {
		t.Fatal(err)
	}
	return ms, cs
}

func putFile(t *testing.T, ms *model.Store, cs *chunkstore.Store, path, content string) manifest.ID {
	t.Helper()
	ctx := context.Background()
	id, err := cs.Put(ctx, fc, []byte(content))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	m := manifest.Manifest{Kind: manifest.KindRegular, Path: path, Mode: 0o644, Chunks: []manifest.ChunkRef{{ID: id, Length: int64(len(content))}}}
	mid, err := ms.PutManifest(ctx, m, model.Metadata{Size: int64(len(content))})
	if err != nil {
		t.Fatalf("PutManifest: %v", err)
	}
	return mid
}

func chunkID(content string) hasher.ChunkID { return hasher.Sum([]byte(content)) }

func has(t *testing.T, cs *chunkstore.Store, content string) bool {
	t.Helper()
	ok, err := cs.Has(context.Background(), chunkID(content))
	if err != nil {
		t.Fatalf("Has: %v", err)
	}
	return ok
}

func TestSweepReclaimsOnlyUnreachablePastGrace(t *testing.T) {
	ctx := context.Background()
	ms, cs := newStores(t)

	putFile(t, ms, cs, "a.txt", "version one")
	s1, _ := ms.Cut(ctx)
	putFile(t, ms, cs, "a.txt", "version two is different")
	if _, err := ms.Cut(ctx); err != nil {
		t.Fatal(err)
	}

	// Both versions are reachable (v1 via S1, v2 via current + S2): sweep deletes nothing.
	if r, err := Sweep(ctx, ms, cs, time.Hour, time.Now().Add(24*time.Hour)); err != nil || r.ChunksDeleted != 0 {
		t.Fatalf("sweep with both reachable deleted %d (err %v), want 0", r.ChunksDeleted, err)
	}

	// Forget S1: v1's chunk becomes unreachable.
	if err := ms.Forget(ctx, s1); err != nil {
		t.Fatalf("Forget: %v", err)
	}
	r, err := Sweep(ctx, ms, cs, time.Hour, time.Now().Add(24*time.Hour))
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if r.ChunksDeleted != 1 {
		t.Fatalf("deleted %d chunks, want exactly 1 (the forgotten version)", r.ChunksDeleted)
	}
	if has(t, cs, "version one") {
		t.Fatal("forgotten version's chunk was not reclaimed")
	}
	if !has(t, cs, "version two is different") {
		t.Fatal("reachable chunk was wrongly reclaimed")
	}
}

func TestSweepRespectsGraceAge(t *testing.T) {
	ctx := context.Background()
	ms, cs := newStores(t)
	putFile(t, ms, cs, "a.txt", "v1")
	s1, _ := ms.Cut(ctx)
	putFile(t, ms, cs, "a.txt", "v2")
	if _, err := ms.Cut(ctx); err != nil {
		t.Fatal(err)
	}
	if err := ms.Forget(ctx, s1); err != nil {
		t.Fatal(err)
	}

	// v1 is unreachable but was just written: a sweep at "now" with an hour of grace
	// must not touch it.
	if r, err := Sweep(ctx, ms, cs, time.Hour, time.Now()); err != nil || r.ChunksDeleted != 0 {
		t.Fatalf("sweep within grace deleted %d (err %v), want 0", r.ChunksDeleted, err)
	}
	if !has(t, cs, "v1") {
		t.Fatal("within-grace chunk deleted")
	}
	// Past the grace window, it is collected.
	if r, err := Sweep(ctx, ms, cs, time.Hour, time.Now().Add(2*time.Hour)); err != nil || r.ChunksDeleted != 1 {
		t.Fatalf("sweep past grace deleted %d (err %v), want 1", r.ChunksDeleted, err)
	}
}

func TestSweepSpareInFlightUnreferencedChunk(t *testing.T) {
	ctx := context.Background()
	ms, cs := newStores(t)
	// A chunk stored but not yet referenced by any manifest — exactly the window a
	// concurrent ingest is in after Put, before PutManifest. Grace must protect it.
	if _, err := cs.Put(ctx, fc, []byte("in flight")); err != nil {
		t.Fatal(err)
	}
	if r, err := Sweep(ctx, ms, cs, time.Hour, time.Now()); err != nil || r.ChunksDeleted != 0 {
		t.Fatalf("sweep deleted in-flight chunk: %d (err %v)", r.ChunksDeleted, err)
	}
	if !has(t, cs, "in flight") {
		t.Fatal("in-flight chunk was collected within grace")
	}
}

func TestSweepIsIdempotent(t *testing.T) {
	ctx := context.Background()
	ms, cs := newStores(t)
	putFile(t, ms, cs, "a.txt", "v1")
	s1, _ := ms.Cut(ctx)
	putFile(t, ms, cs, "a.txt", "v2")
	if _, err := ms.Cut(ctx); err != nil {
		t.Fatal(err)
	}
	if err := ms.Forget(ctx, s1); err != nil {
		t.Fatal(err)
	}
	future := time.Now().Add(2 * time.Hour)
	if r, _ := Sweep(ctx, ms, cs, time.Hour, future); r.ChunksDeleted != 1 {
		t.Fatalf("first sweep deleted %d, want 1", r.ChunksDeleted)
	}
	if r, err := Sweep(ctx, ms, cs, time.Hour, future); err != nil || r.ChunksDeleted != 0 {
		t.Fatalf("second sweep deleted %d (err %v), want 0", r.ChunksDeleted, err)
	}
}

func TestHistoryIntegrityRestoreThenReclaim(t *testing.T) {
	ctx := context.Background()
	ms, cs := newStores(t)

	const v1 = "the original contents of the file"
	putFile(t, ms, cs, "a.txt", v1)
	s1, _ := ms.Cut(ctx)
	putFile(t, ms, cs, "a.txt", "completely different second contents")
	if _, err := ms.Cut(ctx); err != nil {
		t.Fatal(err)
	}

	// While S1 is retained, its version restores bit-exact.
	snap, err := ms.GetSnapshot(ctx, s1)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	mid := leafID(t, snap, "a.txt")
	if got := restore(t, ms, cs, mid); got != v1 {
		t.Fatalf("restored %q, want %q", got, v1)
	}

	// Even after a sweep, a retained snapshot's old version is untouched.
	if _, err := Sweep(ctx, ms, cs, time.Hour, time.Now().Add(24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if got := restore(t, ms, cs, mid); got != v1 {
		t.Fatal("retained old version lost to sweep")
	}

	// Once forgotten and swept past grace, its chunks are reclaimed.
	if err := ms.Forget(ctx, s1); err != nil {
		t.Fatal(err)
	}
	if _, err := Sweep(ctx, ms, cs, time.Hour, time.Now().Add(24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	refs, err := ms.ManifestChunks(ctx, mid)
	if err != nil {
		t.Fatalf("ManifestChunks: %v", err)
	}
	if err := cs.Reassemble(ctx, fc, ids(refs), &bytes.Buffer{}); !errors.Is(err, chunkstore.ErrChunkNotFound) {
		t.Fatalf("expected forgotten version's chunks reclaimed, got %v", err)
	}
}

// TestRestoreOldVersionFromClone restores an older, multi-chunk version of a file
// through the clone path: its clone survives an in-place overwrite, restores
// bit-exact while the snapshot is retained, and is reclaimed once the snapshot is
// forgotten, leaving the current version intact.
func TestRestoreOldVersionFromClone(t *testing.T) {
	ctx := context.Background()
	ms, cs := newStores(t)
	diskPath := filepath.Join(t.TempDir(), "a.bin")

	v1 := genData(5<<20, 1)
	ingestClone(t, ms, cs, "a.bin", diskPath, v1)
	s1, _ := ms.Cut(ctx)

	v2 := genData(5<<20, 2)
	mid2 := ingestClone(t, ms, cs, "a.bin", diskPath, v2)
	if _, err := ms.Cut(ctx); err != nil {
		t.Fatal(err)
	}

	snap, err := ms.GetSnapshot(ctx, s1)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	oldMid := leafID(t, snap, "a.bin")
	if refs, _ := ms.ManifestChunks(ctx, oldMid); len(refs) < 2 {
		t.Fatalf("want a multi-chunk old version, got %d chunks", len(refs))
	}

	// While S1 is retained the old version restores bit-exact, even after a sweep.
	if got := restore(t, ms, cs, oldMid); got != string(v1) {
		t.Fatal("old version not restored bit-exact from its clone")
	}
	if _, err := Sweep(ctx, ms, cs, time.Hour, time.Now().Add(24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if got := restore(t, ms, cs, oldMid); got != string(v1) {
		t.Fatal("retained old version lost to sweep")
	}

	// Forget S1 and sweep: the old clone is reclaimed, v1 is gone, v2 still restores.
	if err := ms.Forget(ctx, s1); err != nil {
		t.Fatal(err)
	}
	r, err := Sweep(ctx, ms, cs, time.Hour, time.Now().Add(24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if r.ObjectsReclaimed == 0 {
		t.Fatal("old clone object not reclaimed after the snapshot was forgotten")
	}
	oldRefs, _ := ms.ManifestChunks(ctx, oldMid)
	if err := cs.Reassemble(ctx, fc, ids(oldRefs), &bytes.Buffer{}); !errors.Is(err, chunkstore.ErrChunkNotFound) {
		t.Fatalf("expected the forgotten version's chunks reclaimed, got %v", err)
	}
	if got := restore(t, ms, cs, mid2); got != string(v2) {
		t.Fatal("current version harmed by reclaim")
	}
}

func TestLogicalBytesReflectsDedup(t *testing.T) {
	ctx := context.Background()
	ms, cs := newStores(t)

	putFile(t, ms, cs, "a.txt", "hello")
	putFile(t, ms, cs, "b.txt", "hello") // identical content → shared chunk
	lb, err := ms.LogicalBytes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if lb != 5 {
		t.Fatalf("logical bytes = %d, want 5 (deduped)", lb)
	}

	putFile(t, ms, cs, "c.txt", "world")
	if lb, _ := ms.LogicalBytes(ctx); lb != 10 {
		t.Fatalf("logical bytes = %d, want 10", lb)
	}
}

func leafID(t *testing.T, snap model.Snapshot, path string) manifest.ID {
	t.Helper()
	for _, l := range snap.Leaves {
		if l.Path == path {
			return l.ManifestID
		}
	}
	t.Fatalf("path %q not in snapshot", path)
	return manifest.ID{}
}

func ids(refs []manifest.ChunkRef) []hasher.ChunkID {
	out := make([]hasher.ChunkID, len(refs))
	for i, r := range refs {
		out[i] = r.ID
	}
	return out
}

func restore(t *testing.T, ms *model.Store, cs *chunkstore.Store, mid manifest.ID) string {
	t.Helper()
	refs, err := ms.ManifestChunks(context.Background(), mid)
	if err != nil {
		t.Fatalf("ManifestChunks: %v", err)
	}
	var buf bytes.Buffer
	if err := cs.Reassemble(context.Background(), fc, ids(refs), &buf); err != nil {
		t.Fatalf("Reassemble: %v", err)
	}
	return buf.String()
}
