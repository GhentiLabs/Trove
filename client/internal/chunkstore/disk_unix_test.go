//go:build darwin || linux

package chunkstore

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func freeBytes(t *testing.T, path string) int64 {
	t.Helper()
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		t.Fatalf("statfs %q: %v", path, err)
	}
	return int64(st.Bavail) * int64(st.Bsize)
}

func fsync(t *testing.T, path string) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %q: %v", path, err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("fsync %q: %v", path, err)
	}
	_ = f.Close()
}

// TestIngestCloneIsAboutOneX is the headline measurement: cloning a current file
// adds almost no disk, because the clone shares the working file's extents. A
// physical copy would consume a second N bytes; the clone consumes far less than
// a quarter of N. Measured as a filesystem free-space delta, since per-file block
// counts double-count shared extents. Skipped where the filesystem has no reflink.
func TestIngestCloneIsAboutOneX(t *testing.T) {
	s, dir := newStore(t, 0)

	probe := filepath.Join(dir, "probe")
	if err := os.WriteFile(probe, genData(1<<20, 1), 0o600); err != nil {
		t.Fatalf("probe write: %v", err)
	}
	cloned, err := cloneOrCopy(probe, probe+".clone")
	if err != nil {
		t.Fatalf("probe clone: %v", err)
	}
	_ = os.Remove(probe)
	_ = os.Remove(probe + ".clone")
	if !cloned {
		t.Skip("filesystem does not support reflink; ~1x requires copy-on-write")
	}

	const n = 64 << 20
	path := filepath.Join(dir, "big.bin")
	if err := os.WriteFile(path, genData(n, 2), 0o600); err != nil {
		t.Fatalf("write working file: %v", err)
	}
	fsync(t, path)

	before := freeBytes(t, dir)
	if _, err := s.IngestClone(context.Background(), path); err != nil {
		t.Fatalf("IngestClone: %v", err)
	}
	after := freeBytes(t, dir)

	added := before - after
	if added > n/4 {
		t.Fatalf("ingesting a %d MiB file added %d MiB; clone did not share extents", n>>20, added>>20)
	}
	t.Logf("ingest of %d MiB added %d MiB", n>>20, added>>20)
}
