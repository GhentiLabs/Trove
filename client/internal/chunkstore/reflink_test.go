//go:build unix

package chunkstore

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func inodeOf(t *testing.T, path string) uint64 {
	t.Helper()
	var st syscall.Stat_t
	if err := syscall.Stat(path, &st); err != nil {
		t.Fatalf("stat %q: %v", path, err)
	}
	return st.Ino
}

// TestCloneOrCopy covers the real path on the dev filesystem: the result is
// byte-exact and a separate inode whether a clone or a copy was made, so deleting
// the destination can never touch the source.
func TestCloneOrCopy(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")
	data := genData(4<<20, 7)
	if err := os.WriteFile(src, data, 0o600); err != nil {
		t.Fatalf("write src: %v", err)
	}

	cloned, err := cloneOrCopy(src, dst)
	if err != nil {
		t.Fatalf("cloneOrCopy: %v", err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read dst: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("clone/copy not byte-exact")
	}
	if inodeOf(t, src) == inodeOf(t, dst) {
		t.Fatal("destination shares the source inode")
	}
	t.Logf("cloned=%v", cloned)
}

// TestPhysicalCopy covers the fallback directly: byte-exact, separate inode, and
// it refuses to clobber an existing destination.
func TestPhysicalCopy(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")
	data := genData(1<<20, 3)
	if err := os.WriteFile(src, data, 0o600); err != nil {
		t.Fatalf("write src: %v", err)
	}

	if err := physicalCopy(src, dst); err != nil {
		t.Fatalf("physicalCopy: %v", err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read dst: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("copy not byte-exact")
	}
	if inodeOf(t, src) == inodeOf(t, dst) {
		t.Fatal("copy shares the source inode")
	}
	if err := physicalCopy(src, dst); err == nil {
		t.Fatal("physicalCopy clobbered an existing destination")
	}
}

// TestCloneOrCopyFallback forces the copy path on a clone-capable filesystem by
// stubbing the clone primitive, so the fallback is exercised even on APFS.
func TestCloneOrCopyFallback(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")
	data := genData(2<<20, 9)
	if err := os.WriteFile(src, data, 0o600); err != nil {
		t.Fatalf("write src: %v", err)
	}

	unsupported := func(string, string) error { return errReflinkUnsupported }
	cloned, err := cloneOrCopyWith(unsupported, src, dst)
	if err != nil {
		t.Fatalf("cloneOrCopyWith: %v", err)
	}
	if cloned {
		t.Fatal("reported a clone where the primitive was unsupported")
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read dst: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("fallback copy not byte-exact")
	}
	if inodeOf(t, src) == inodeOf(t, dst) {
		t.Fatal("fallback copy shares the source inode")
	}
}

// TestCloneOrCopyPropagatesError checks that a clone failure other than
// "unsupported" is returned, not silently downgraded to a copy.
func TestCloneOrCopyPropagatesError(t *testing.T) {
	boom := errors.New("boom")
	failing := func(string, string) error { return boom }
	if _, err := cloneOrCopyWith(failing, "src", "dst"); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
}
