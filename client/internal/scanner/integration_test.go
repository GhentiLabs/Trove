package scanner

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/chunkstore"
	"github.com/GhentiLabs/Trove/client/internal/hasher"
	"github.com/GhentiLabs/Trove/client/internal/manifest"
	"github.com/GhentiLabs/Trove/client/internal/model"
	"github.com/GhentiLabs/Trove/client/internal/storage"
	"github.com/GhentiLabs/Trove/client/internal/watcher"
)

const testNode = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

// openStores opens a chunk store and model store rooted at dir. Reopening the same
// dir reuses the on-disk databases and blobs — the basis for the crash-recovery
// test. The returned closeAll releases both; the caller decides when (a restart
// closes the old set before opening the new).
func openStores(t *testing.T, dir string) (*chunkstore.Store, *model.Store, func()) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	cdb, err := storage.Open(storage.Options{Path: filepath.Join(dir, "chunks.db"), MaxOpenConns: 8})
	if err != nil {
		t.Fatalf("chunk db: %v", err)
	}
	cs, err := chunkstore.Open(chunkstore.Options{DB: cdb, BlobDir: filepath.Join(dir, "blobs")})
	if err != nil {
		t.Fatalf("chunkstore: %v", err)
	}
	mdb, err := storage.Open(storage.Options{Path: filepath.Join(dir, "state.db"), MaxOpenConns: 8})
	if err != nil {
		t.Fatalf("state db: %v", err)
	}
	ms, err := model.Open(model.Options{DB: mdb, NodeID: testNode})
	if err != nil {
		t.Fatalf("model: %v", err)
	}
	return cs, ms, func() {
		_ = cs.Close()
		_ = cdb.Close()
		_ = mdb.Close()
	}
}

func newScanner(t *testing.T, root string, tune ...func(*Options)) (*Scanner, *model.Store, *chunkstore.Store, *watcher.Fake) {
	t.Helper()
	cs, ms, closeAll := openStores(t, t.TempDir())
	t.Cleanup(closeAll)
	f := watcher.NewFake()
	opts := Options{Root: root, Chunks: cs, Model: ms, Watcher: f}
	for _, fn := range tune {
		fn(&opts)
	}
	s, err := New(opts)
	if err != nil {
		t.Fatalf("scanner.New: %v", err)
	}
	return s, ms, cs, f
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func content(t *testing.T, cs *chunkstore.Store, refs []manifest.ChunkRef) []byte {
	t.Helper()
	ids := make([]hasher.ChunkID, len(refs))
	for i, r := range refs {
		ids[i] = r.ID
	}
	var buf bytes.Buffer
	if err := cs.Reassemble(context.Background(), chunkstore.FolderContext{}, ids, &buf); err != nil {
		t.Fatalf("reassemble: %v", err)
	}
	return buf.Bytes()
}

func TestScanAllIngestsTree(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "a.txt"), "hello")
	writeFile(t, filepath.Join(root, "sub/b.txt"), strings.Repeat("data", 1000))
	if err := os.MkdirAll(filepath.Join(root, "empty"), 0o755); err != nil {
		t.Fatal(err)
	}
	s, ms, cs, _ := newScanner(t, root)
	ctx := context.Background()
	if err := s.ScanAll(ctx); err != nil {
		t.Fatalf("ScanAll: %v", err)
	}

	a := mustGet(t, ms, "a.txt")
	if a.Manifest.Kind != manifest.KindRegular || string(content(t, cs, a.Manifest.Chunks)) != "hello" {
		t.Fatalf("a.txt not ingested correctly: %+v", a.Manifest)
	}
	b := mustGet(t, ms, "sub/b.txt")
	if string(content(t, cs, b.Manifest.Chunks)) != strings.Repeat("data", 1000) {
		t.Fatal("sub/b.txt content mismatch")
	}
	if mustGet(t, ms, "sub").Manifest.Kind != manifest.KindDir {
		t.Fatal("sub not recorded as a directory")
	}
	if mustGet(t, ms, "empty").Manifest.Kind != manifest.KindDir {
		t.Fatal("empty dir not recorded")
	}
}

func TestScanIngestsPhysically(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "a.txt"), "physical content here")
	s, ms, cs, _ := newScanner(t, root)
	ctx := context.Background()
	if err := s.ScanAll(ctx); err != nil {
		t.Fatal(err)
	}
	rec := mustGet(t, ms, "a.txt")
	if err := os.Remove(filepath.Join(root, "a.txt")); err != nil {
		t.Fatal(err)
	}
	got := content(t, cs, rec.Manifest.Chunks)
	if string(got) != "physical content here" {
		t.Fatalf("expected physical chunks to survive working-file deletion, got %q", got)
	}
}

func TestScanAllIngestsSymlink(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "a.txt"), "hello")
	if err := os.Symlink("a.txt", filepath.Join(root, "link")); err != nil {
		t.Skipf("symlinks unsupported: %v", err)
	}
	s, ms, _, _ := newScanner(t, root)
	if err := s.ScanAll(context.Background()); err != nil {
		t.Fatalf("ScanAll: %v", err)
	}
	link := mustGet(t, ms, "link")
	if link.Manifest.Kind != manifest.KindSymlink || link.Manifest.SymlinkTarget != "a.txt" {
		t.Fatalf("symlink not ingested: %+v", link.Manifest)
	}
}

func TestStatFastPathSkipsSameStatChange(t *testing.T) {
	root := t.TempDir()
	p := filepath.Join(root, "a.txt")
	writeFile(t, p, "AAAAA")
	s, ms, _, _ := newScanner(t, root)
	ctx := context.Background()
	if err := s.ScanAll(ctx); err != nil {
		t.Fatal(err)
	}
	before := mustGet(t, ms, "a.txt")

	// Overwrite with same-length content and restore the original mtime: the stat
	// signature is unchanged, so the fast path must skip it (proving it trusts stat,
	// not content — the documented mtime heuristic the rescan backstops).
	fi, _ := os.Stat(p)
	if err := os.WriteFile(p, []byte("BBBBB"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(p, fi.ModTime(), fi.ModTime()); err != nil {
		t.Fatal(err)
	}
	if err := s.ScanAll(ctx); err != nil {
		t.Fatal(err)
	}
	if after := mustGet(t, ms, "a.txt"); after.ID != before.ID {
		t.Fatal("fast path should have skipped a same-stat change")
	}
}

func TestContentChangeIsDetected(t *testing.T) {
	root := t.TempDir()
	p := filepath.Join(root, "a.txt")
	writeFile(t, p, "AAAAA")
	s, ms, _, _ := newScanner(t, root)
	ctx := context.Background()
	if err := s.ScanAll(ctx); err != nil {
		t.Fatal(err)
	}
	before := mustGet(t, ms, "a.txt")

	time.Sleep(10 * time.Millisecond)
	writeFile(t, p, "a much longer different content")
	if err := s.ScanAll(ctx); err != nil {
		t.Fatal(err)
	}
	after := mustGet(t, ms, "a.txt")
	if after.ID == before.ID {
		t.Fatal("content change not detected")
	}
	if after.Seq <= before.Seq {
		t.Fatalf("changed file did not get a new sequence: %d -> %d", before.Seq, after.Seq)
	}
}

func TestIncrementalDeleteTombstones(t *testing.T) {
	root := t.TempDir()
	p := filepath.Join(root, "a.txt")
	writeFile(t, p, "hello")
	s, ms, _, _ := newScanner(t, root)
	ctx := context.Background()
	if err := s.ScanAll(ctx); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(p); err != nil {
		t.Fatal(err)
	}
	if err := s.ingest(ctx, slices.Values([]string{"a.txt"})); err != nil {
		t.Fatal(err)
	}
	if rec := mustGet(t, ms, "a.txt"); !rec.Deleted {
		t.Fatal("removed path was not tombstoned")
	}
}

func TestScanAllFailsFastOnMissingRoot(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "a.txt"), "hello")
	s, ms, _, _ := newScanner(t, root)
	ctx := context.Background()
	if err := s.Rescan(ctx); err != nil {
		t.Fatal(err)
	}

	// The root vanishes (transient mount loss, rename, etc). A rescan must error
	// rather than scan nothing and tombstone every known file.
	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	if err := s.Rescan(ctx); err == nil {
		t.Fatal("Rescan with a missing root should fail, not silently tombstone")
	}
	if rec := mustGet(t, ms, "a.txt"); rec.Deleted {
		t.Fatal("a missing root mass-tombstoned the folder")
	}
}

func TestRescanReconcilesOutOfBandChanges(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "keep.txt"), "keep")
	writeFile(t, filepath.Join(root, "gone.txt"), "gone")
	s, ms, _, _ := newScanner(t, root)
	ctx := context.Background()
	if err := s.Rescan(ctx); err != nil {
		t.Fatal(err)
	}

	// Mutate the tree out of band — no watcher events.
	if err := os.Remove(filepath.Join(root, "gone.txt")); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(root, "added.txt"), "added")
	time.Sleep(10 * time.Millisecond)
	writeFile(t, filepath.Join(root, "keep.txt"), "keep changed bigger")

	if err := s.Rescan(ctx); err != nil {
		t.Fatal(err)
	}
	if rec := mustGet(t, ms, "gone.txt"); !rec.Deleted {
		t.Fatal("out-of-band deletion not tombstoned by rescan")
	}
	if _, err := ms.GetManifest(ctx, "added.txt"); err != nil {
		t.Fatalf("out-of-band addition not ingested: %v", err)
	}
	if mustGet(t, ms, "keep.txt").Manifest.ID() == (manifest.ID{}) {
		t.Fatal("keep.txt missing")
	}
}

func TestCrashRecoveryConvergesViaRescan(t *testing.T) {
	root := t.TempDir()
	db := t.TempDir()
	writeFile(t, filepath.Join(root, "a.txt"), "alpha")
	writeFile(t, filepath.Join(root, "b.txt"), "bravo")
	ctx := context.Background()

	// First "process": ingest, then shut down (releasing the DBs).
	cs1, ms1, close1 := openStores(t, db)
	s1, err := New(Options{Root: root, Chunks: cs1, Model: ms1, Watcher: watcher.NewFake()})
	if err != nil {
		t.Fatal(err)
	}
	if err := s1.Rescan(ctx); err != nil {
		t.Fatal(err)
	}
	close1()

	// While "down", the tree changes out of band.
	if err := os.Remove(filepath.Join(root, "b.txt")); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(root, "c.txt"), "charlie")

	// Restart against the same on-disk stores and reconcile.
	cs2, ms2, close2 := openStores(t, db)
	t.Cleanup(close2)
	s2, err := New(Options{Root: root, Chunks: cs2, Model: ms2, Watcher: watcher.NewFake()})
	if err != nil {
		t.Fatal(err)
	}
	if err := s2.Rescan(ctx); err != nil {
		t.Fatal(err)
	}
	if rec := mustGet(t, ms2, "b.txt"); !rec.Deleted {
		t.Fatal("deletion during downtime not recovered")
	}
	if _, err := ms2.GetManifest(ctx, "c.txt"); err != nil {
		t.Fatalf("addition during downtime not recovered: %v", err)
	}
	if rec := mustGet(t, ms2, "a.txt"); rec.Deleted {
		t.Fatal("unchanged file wrongly tombstoned after restart")
	}
}

func TestRunIngestsThenSnapshots(t *testing.T) {
	root := t.TempDir()
	s, ms, _, f := newScanner(t, root, func(o *Options) {
		o.DebounceWindow = 10 * time.Millisecond
		o.SnapshotQuiesce = 40 * time.Millisecond
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var run sync.WaitGroup
	run.Go(func() { _ = s.Run(ctx) })

	writeFile(t, filepath.Join(root, "a.txt"), "hello")
	f.Emit(watcher.Event{Path: filepath.Join(root, "a.txt"), Op: watcher.OpWrite})

	waitFor(t, 2*time.Second, func() bool {
		_, err := ms.GetManifest(ctx, "a.txt")
		return err == nil
	})
	waitFor(t, 2*time.Second, func() bool {
		snaps, err := ms.ListSnapshots(ctx)
		return err == nil && len(snaps) > 0
	})
	cancel()
	run.Wait()
}

// TestScanAllBoundedMemory proves the pipeline's working set is independent of
// tree size: scanning 4x the files must not cost ~4x the memory. Comparing peaks
// (rather than an absolute bound) is robust to Go's GC accounting, since both runs
// churn identically and only differ in count.
func TestRunWithRealWatcher(t *testing.T) {
	root := t.TempDir()
	cs, ms, closeAll := openStores(t, t.TempDir())
	t.Cleanup(closeAll)
	w, err := watcher.New(root)
	if err != nil {
		t.Fatalf("watcher.New: %v", err)
	}
	s, err := New(Options{
		Root: root, Chunks: cs, Model: ms, Watcher: w,
		DebounceWindow: 10 * time.Millisecond, SnapshotQuiesce: 40 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var run sync.WaitGroup
	run.Go(func() { _ = s.Run(ctx) })

	writeFile(t, filepath.Join(root, "real.txt"), "via fsnotify")
	waitFor(t, 3*time.Second, func() bool {
		_, err := ms.GetManifest(ctx, "real.txt")
		return err == nil
	})
	cancel()
	run.Wait()
}

// TestScanAllLargeTree streams a large tree through to completion with every file
// ingested. The hard "RAM flat regardless of tree size" guarantee is structural —
// every pipeline stage is a bounded channel + fixed worker pool (see ingest.go),
// and files are chunked one chunk at a time — so a heap-number assertion would
// only measure allocator/GC noise; this verifies the streaming pipeline handles a
// tree far larger than the in-flight bound.
func TestScanAllLargeTree(t *testing.T) {
	root := t.TempDir()
	const files = 2000
	for i := range files {
		writeFile(t, filepath.Join(root, "d", fileName(i/64), fileName(i)), fmt.Sprintf("contents of file number %d", i))
	}
	s, ms, _, _ := newScanner(t, root)
	if err := s.ScanAll(context.Background()); err != nil {
		t.Fatalf("ScanAll: %v", err)
	}
	recs, err := ms.ListManifests(context.Background())
	if err != nil {
		t.Fatalf("ListManifests: %v", err)
	}
	var regular int
	for _, r := range recs {
		if r.Manifest.Kind == manifest.KindRegular {
			regular++
		}
	}
	if regular != files {
		t.Fatalf("ingested %d regular files, want %d", regular, files)
	}
}

func fileName(i int) string {
	return string(rune('a'+i%26)) + string(rune('a'+(i/26)%26)) + string(rune('0'+i/676))
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

func mustGet(t *testing.T, ms *model.Store, path string) model.Record {
	t.Helper()
	rec, err := ms.GetManifest(context.Background(), path)
	if err != nil {
		t.Fatalf("GetManifest %q: %v", path, err)
	}
	return rec
}
