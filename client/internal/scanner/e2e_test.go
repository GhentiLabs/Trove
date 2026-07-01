package scanner

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/chunkstore"
	"github.com/GhentiLabs/Trove/client/internal/gc"
	"github.com/GhentiLabs/Trove/client/internal/hasher"
	"github.com/GhentiLabs/Trove/client/internal/manifest"
	"github.com/GhentiLabs/Trove/client/internal/model"
	"github.com/GhentiLabs/Trove/client/internal/watcher"
)

// TestEndToEndEncryptedLifecycle drives the whole M2 stack together against an
// encrypted folder: scan -> snapshot -> quota -> edit -> structural sharing ->
// history-integrity restore -> out-of-band delete -> tombstone -> forget -> GC
// reclaim, with content read back through decryption at each step.
func TestEndToEndEncryptedLifecycle(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	cs, ms, closeAll := openStores(t, t.TempDir())
	t.Cleanup(closeAll)

	var key [32]byte
	for i := range key {
		key[i] = byte(i*3 + 1)
	}
	fc := chunkstore.FolderContext{Encrypted: true, MasterKey: key}

	s, err := New(Options{Root: root, FolderCtx: fc, Chunks: cs, Model: ms, Watcher: watcher.NewFake(), KeepHistory: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	const v1, readme = "alpha contents version one", "the readme body"
	writeFile(t, filepath.Join(root, "a.txt"), v1)
	writeFile(t, filepath.Join(root, "docs/readme.md"), readme)
	if err := s.Rescan(ctx); err != nil {
		t.Fatalf("rescan 1: %v", err)
	}
	snap1, err := ms.Cut(ctx)
	if err != nil {
		t.Fatalf("cut 1: %v", err)
	}

	// Content round-trips through encryption.
	if got := decrypt(t, cs, fc, mustGet(t, ms, "a.txt").Manifest.Chunks); got != v1 {
		t.Fatalf("a.txt = %q, want %q", got, v1)
	}
	readmeID := mustGet(t, ms, "docs/readme.md").ID

	// Quota counts unique plaintext bytes.
	if lb, _ := ms.LogicalBytes(ctx); lb != int64(len(v1)+len(readme)) {
		t.Fatalf("logical bytes = %d, want %d", lb, len(v1)+len(readme))
	}

	// Edit a.txt; readme is untouched -> its manifest id is byte-identical (sharing).
	time.Sleep(5 * time.Millisecond)
	writeFile(t, filepath.Join(root, "a.txt"), "alpha contents, a second and longer version")
	if err := s.Rescan(ctx); err != nil {
		t.Fatalf("rescan 2: %v", err)
	}
	if _, err := ms.Cut(ctx); err != nil {
		t.Fatalf("cut 2: %v", err)
	}
	if mustGet(t, ms, "docs/readme.md").ID != readmeID {
		t.Fatal("unchanged file's manifest id changed across snapshots")
	}

	// History integrity: a.txt's v1 still restores bit-exact from snap1 while retained.
	snap1Leaves, err := ms.GetSnapshot(ctx, snap1)
	if err != nil {
		t.Fatalf("get snap1: %v", err)
	}
	oldA := leafIDFor(t, snap1Leaves, "a.txt")
	oldRefs, _ := ms.ManifestChunks(ctx, oldA)
	if got := decrypt(t, cs, fc, oldRefs); got != v1 {
		t.Fatalf("restored old a.txt = %q, want %q", got, v1)
	}
	// The edit superseded v1's chunk, so it was promoted out of its plaintext clone
	// into a sealed history blob: reading it now needs the key.
	if _, err := cs.Get(ctx, chunkstore.FolderContext{}, oldRefs[0].ID); !errors.Is(err, chunkstore.ErrNoKey) {
		t.Fatalf("superseded chunk keyless Get = %v, want ErrNoKey (sealed history)", err)
	}

	// Out-of-band delete of readme -> rescan tombstones it.
	if err := os.Remove(filepath.Join(root, "docs/readme.md")); err != nil {
		t.Fatal(err)
	}
	if err := s.Rescan(ctx); err != nil {
		t.Fatalf("rescan 3: %v", err)
	}
	if !mustGet(t, ms, "docs/readme.md").Deleted {
		t.Fatal("out-of-band delete not tombstoned")
	}
	// The delete superseded readme's chunk while snap1/snap2 still pin it, so it too
	// was promoted into sealed history rather than left in its plaintext clone.
	readmeChunk := hasher.Sum([]byte(readme))
	if _, err := cs.Get(ctx, chunkstore.FolderContext{}, readmeChunk); !errors.Is(err, chunkstore.ErrNoKey) {
		t.Fatalf("deleted file's snapshot-pinned chunk keyless Get = %v, want ErrNoKey (sealed history)", err)
	}

	// Forget snap1, then GC: a.txt v1 becomes unreachable and is reclaimed; the
	// current version stays intact.
	if err := ms.Forget(ctx, snap1); err != nil {
		t.Fatalf("forget: %v", err)
	}
	res, err := gc.Sweep(ctx, ms, cs, time.Minute, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if res.ChunksDeleted == 0 {
		t.Fatal("GC reclaimed nothing after forgetting the only snapshot pinning v1")
	}
	if got := decrypt(t, cs, fc, mustGet(t, ms, "a.txt").Manifest.Chunks); got != "alpha contents, a second and longer version" {
		t.Fatalf("current a.txt corrupted by GC: %q", got)
	}
	if err := cs.Reassemble(ctx, fc, idsOf(oldRefs), &bytes.Buffer{}); err == nil {
		t.Fatal("forgotten v1 chunks should have been reclaimed")
	}
}

// TestScanPromoteSkipsChunkMovedToAnotherFile checks the batch-once-per-scan rule: a
// chunk one file drops but another file in the same scan reintroduces is still current,
// so it must not be promoted into history. Promoting per file (before the second file's
// manifest commits) would wrongly seal it.
func TestScanPromoteSkipsChunkMovedToAnotherFile(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	cs, ms, closeAll := openStores(t, t.TempDir())
	t.Cleanup(closeAll)

	var key [32]byte
	for i := range key {
		key[i] = byte(i*7 + 2)
	}
	fc := chunkstore.FolderContext{Encrypted: true, MasterKey: key}

	s, err := New(Options{Root: root, FolderCtx: fc, Chunks: cs, Model: ms, Watcher: watcher.NewFake(), KeepHistory: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	const shared = "the shared block of bytes"
	writeFile(t, filepath.Join(root, "a.txt"), shared)
	if err := s.Rescan(ctx); err != nil {
		t.Fatalf("rescan 1: %v", err)
	}
	if _, err := ms.Cut(ctx); err != nil { // pins a.txt's chunk as history
		t.Fatalf("cut: %v", err)
	}

	// One scan both supersedes a.txt's chunk and reintroduces it in b.txt.
	writeFile(t, filepath.Join(root, "a.txt"), "a completely different replacement body")
	writeFile(t, filepath.Join(root, "b.txt"), shared)
	if err := s.Rescan(ctx); err != nil {
		t.Fatalf("rescan 2: %v", err)
	}

	sharedChunk := hasher.Sum([]byte(shared))
	if _, err := cs.Get(ctx, chunkstore.FolderContext{}, sharedChunk); err != nil {
		t.Fatalf("chunk still current in b.txt must stay a plaintext clone; keyless Get = %v", err)
	}
}

func decrypt(t *testing.T, cs *chunkstore.Store, fc chunkstore.FolderContext, refs []manifest.ChunkRef) string {
	t.Helper()
	var buf bytes.Buffer
	if err := cs.Reassemble(context.Background(), fc, idsOf(refs), &buf); err != nil {
		t.Fatalf("reassemble: %v", err)
	}
	return buf.String()
}

func idsOf(refs []manifest.ChunkRef) []hasher.ChunkID {
	out := make([]hasher.ChunkID, len(refs))
	for i, r := range refs {
		out[i] = r.ID
	}
	return out
}

func leafIDFor(t *testing.T, snap model.Snapshot, path string) manifest.ID {
	t.Helper()
	for _, l := range snap.Leaves {
		if l.Path == path {
			return l.ManifestID
		}
	}
	t.Fatalf("path %q not in snapshot", path)
	return manifest.ID{}
}
