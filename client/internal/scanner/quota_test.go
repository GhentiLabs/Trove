package scanner

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/GhentiLabs/Trove/client/internal/chunkstore"
	"github.com/GhentiLabs/Trove/client/internal/model"
	"github.com/GhentiLabs/Trove/client/internal/storage"
	"github.com/GhentiLabs/Trove/client/internal/watcher"
)

// TestScannerPrunesHistoryAtQuota checks the owner path prunes history so a repeatedly
// edited file stays within its quota.
func TestScannerPrunesHistoryAtQuota(t *testing.T) {
	dir := t.TempDir()
	root := t.TempDir()
	ctx := context.Background()

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
	const chunk = 4000
	ms, err := model.Open(model.Options{DB: mdb, NodeID: testNode, QuotaBytes: 3 * chunk})
	if err != nil {
		t.Fatal(err)
	}

	s, err := New(Options{Root: root, Chunks: cs, Model: ms, Watcher: watcher.NewFake(), KeepHistory: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	for i := range 6 {
		writeFile(t, filepath.Join(root, "a.txt"), strings.Repeat(string(rune('a'+i)), chunk))
		if err := s.reconcile(ctx); err != nil {
			t.Fatalf("reconcile %d: %v", i, err)
		}
	}

	used, err := ms.ReachableLogicalBytes(ctx)
	if err != nil {
		t.Fatalf("ReachableLogicalBytes: %v", err)
	}
	if used > 3*chunk {
		t.Fatalf("reachable %d exceeds quota %d; history not pruned", used, 3*chunk)
	}
	if got := string(content(t, cs, mustGet(t, ms, "a.txt").Manifest.Chunks)); got != strings.Repeat("f", chunk) {
		t.Fatalf("current a.txt not the latest edit")
	}
}
