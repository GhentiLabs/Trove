package syncengine

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/GhentiLabs/Trove/client/internal/manifest"
	"github.com/GhentiLabs/Trove/client/internal/model"
	"github.com/GhentiLabs/Trove/client/internal/wire/wirepb"
)

func TestDestPathRejectsEscape(t *testing.T) {
	root := t.TempDir()
	fs := &folderState{cfg: FolderConfig{Root: root}}

	for _, rel := range []string{".", "", "..", "../escape", "a/../../b", "../../etc/passwd"} {
		if _, err := fs.destPath(rel); err == nil {
			t.Fatalf("destPath(%q) accepted an escaping path", rel)
		}
	}
	for _, rel := range []string{"a.txt", "dir/b.txt", "a/b/c.bin"} {
		got, err := fs.destPath(rel)
		if err != nil {
			t.Fatalf("destPath(%q): %v", rel, err)
		}
		if want := filepath.Join(root, filepath.FromSlash(rel)); got != want {
			t.Fatalf("destPath(%q) = %q, want %q", rel, got, want)
		}
	}
}

// A hostile owner that tombstones "." must not delete the folder root.
func TestApplyRejectsRootDeletion(t *testing.T) {
	root := t.TempDir()
	keep := filepath.Join(root, "keep.txt")
	if err := os.WriteFile(keep, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	fs := &folderState{cfg: FolderConfig{Root: root}}

	batch := []model.RemoteManifest{{Deleted: true, Manifest: manifest.Manifest{Path: "."}}}
	if err := fs.apply(context.Background(), batch, &wirepb.ManifestDelta{}); err == nil {
		t.Fatal("apply accepted a root-deletion tombstone")
	}
	if _, err := os.Stat(keep); err != nil {
		t.Fatalf("root content was destroyed: %v", err)
	}
}

// A hostile owner must not plant a symlink whose target escapes the folder root; the
// rejection has to happen before the symlink is written to disk.
func TestApplyRejectsEscapingSymlink(t *testing.T) {
	root := t.TempDir()
	fs := &folderState{cfg: FolderConfig{Root: root}}

	for _, target := range []string{"/etc/passwd", "../../etc/passwd"} {
		batch := []model.RemoteManifest{{Manifest: manifest.Manifest{
			Path: "link", Kind: manifest.KindSymlink, SymlinkTarget: target,
		}}}
		if err := fs.apply(context.Background(), batch, &wirepb.ManifestDelta{}); err == nil {
			t.Fatalf("apply accepted an escaping symlink target %q", target)
		}
		if _, err := os.Lstat(filepath.Join(root, "link")); !os.IsNotExist(err) {
			t.Fatalf("symlink for target %q was created on disk", target)
		}
	}
}
