package syncengine

import (
	"path/filepath"
	"testing"
)

func TestDestPathRejectsEscape(t *testing.T) {
	root := t.TempDir()
	fs := &folderState{cfg: FolderConfig{Root: root}}

	for _, rel := range []string{"..", "../escape", "a/../../b", "../../etc/passwd"} {
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
