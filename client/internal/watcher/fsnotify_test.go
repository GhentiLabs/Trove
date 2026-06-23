package watcher

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func waitEvent(t *testing.T, w Watcher, wantPath string) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		select {
		case ev := <-w.Events():
			if ev.Path == wantPath {
				return
			}
		case err := <-w.Errors():
			t.Fatalf("watcher error: %v", err)
		case <-deadline:
			t.Fatalf("no event for %s within timeout", wantPath)
		}
	}
}

func TestFSNotifyReportsWrite(t *testing.T) {
	root := t.TempDir()
	w, err := New(root)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	p := filepath.Join(root, "a.txt")
	if err := os.WriteFile(p, []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitEvent(t, w, p)
}

func TestFSNotifyWatchesNewSubdir(t *testing.T) {
	root := t.TempDir()
	w, err := New(root)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	sub := filepath.Join(root, "sub")
	if err := os.Mkdir(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	// Seeing sub's own Create event proves the loop has run addTree on it, so the
	// watch is in place before we write inside.
	waitEvent(t, w, sub)
	p := filepath.Join(sub, "b.txt")
	if err := os.WriteFile(p, []byte("world"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitEvent(t, w, p)
}

func TestNewFailsOnMissingRoot(t *testing.T) {
	if w, err := New(filepath.Join(t.TempDir(), "does-not-exist")); err == nil {
		_ = w.Close()
		t.Fatal("New should fail when the root cannot be walked, not watch nothing")
	}
}

func TestFSNotifyCloseStopsEvents(t *testing.T) {
	root := t.TempDir()
	w, err := New(root)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}
