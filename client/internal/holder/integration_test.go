package holder

import (
	"bytes"
	"context"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/GhentiLabs/Trove/client/internal/chunkstore"
	"github.com/GhentiLabs/Trove/client/internal/crypto"
	"github.com/GhentiLabs/Trove/client/internal/model"
	"github.com/GhentiLabs/Trove/client/internal/scanner"
	"github.com/GhentiLabs/Trove/client/internal/storage"
	"github.com/GhentiLabs/Trove/client/internal/watcher"
)

type folder struct {
	root   string
	model  *model.Store
	chunks *chunkstore.Store
	fc     chunkstore.FolderContext
}

func newFolder(t *testing.T, key [crypto.MasterKeyLen]byte) folder {
	t.Helper()
	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("mkdir root: %v", err)
	}
	mdb, err := storage.Open(storage.Options{Path: filepath.Join(dir, "model.db"), MaxOpenConns: 4})
	if err != nil {
		t.Fatalf("model db: %v", err)
	}
	t.Cleanup(func() { _ = mdb.Close() })
	ms, err := model.Open(model.Options{DB: mdb, NodeID: "nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn"})
	if err != nil {
		t.Fatalf("model open: %v", err)
	}
	cdb, err := storage.Open(storage.Options{Path: filepath.Join(dir, "chunk.db"), MaxOpenConns: 4})
	if err != nil {
		t.Fatalf("chunk db: %v", err)
	}
	t.Cleanup(func() { _ = cdb.Close() })
	cs, err := chunkstore.Open(chunkstore.Options{DB: cdb, BlobDir: filepath.Join(dir, "blobs")})
	if err != nil {
		t.Fatalf("chunkstore open: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return folder{root: root, model: ms, chunks: cs, fc: chunkstore.FolderContext{Encrypted: true, MasterKey: key}}
}

func (f folder) scan(t *testing.T) {
	t.Helper()
	sc, err := scanner.New(scanner.Options{Root: f.root, FolderCtx: f.fc, Chunks: f.chunks, Model: f.model, Watcher: watcher.NewFake()})
	if err != nil {
		t.Fatalf("scanner.New: %v", err)
	}
	if err := sc.Rescan(context.Background()); err != nil {
		t.Fatalf("Rescan: %v", err)
	}
}

func writeFile(t *testing.T, root, rel string, data []byte) {
	t.Helper()
	p := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatalf("write %s: %v", rel, err)
	}
}

func pseudoRandom(n int, seed uint64) []byte {
	r := rand.New(rand.NewPCG(seed, 0x9e3779b97f4a7c15))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.Uint64())
	}
	return b
}

// TestExportRestoreBitExact is the Phase C core: a writer exports an encrypted folder to a
// holder as blinded sealed blobs, and a member restores it bit-exact from those blobs
// alone, while the holder holds no key and no observable plaintext.
func TestExportRestoreBitExact(t *testing.T) {
	ctx := context.Background()
	key := testKey(0x5A)
	src := newFolder(t, key)

	secret := []byte("TOP SECRET CONTENTS that must never appear on the holder")
	writeFile(t, src.root, "secret-filename.txt", secret)
	writeFile(t, src.root, "dir/notes.md", []byte("more private notes"))
	writeFile(t, src.root, "big.bin", pseudoRandom(4<<20, 9))
	if err := os.Symlink("secret-filename.txt", filepath.Join(src.root, "link")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(src.root, "emptydir"), 0o755); err != nil {
		t.Fatalf("mkdir empty: %v", err)
	}
	src.scan(t)

	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("holder Open: %v", err)
	}
	put := func(_ context.Context, b [crypto.BlindLen]byte, data []byte) error { return store.Put(b, data) }
	get := func(_ context.Context, b [crypto.BlindLen]byte) ([]byte, error) { return store.Get(b) }

	if err := Export(ctx, key, src.model, src.chunks, src.fc, put); err != nil {
		t.Fatalf("Export: %v", err)
	}

	assertHolderLeaksNothing(t, store.dir, secret, "secret-filename.txt", "notes.md")

	dst := newFolder(t, key)
	if err := Restore(ctx, key, dst.chunks, dst.fc, dst.root, get); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	assertTreesEqual(t, src.root, dst.root)
}

// TestRestoreRejectsTamperedChunk checks a holder that flips a stored byte cannot corrupt
// a restore: the AEAD tag fails and Restore errors rather than writing bad bytes.
func TestRestoreRejectsTamperedChunk(t *testing.T) {
	ctx := context.Background()
	key := testKey(0x33)
	src := newFolder(t, key)
	writeFile(t, src.root, "f.txt", pseudoRandom(2<<20, 3))
	src.scan(t)

	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("holder Open: %v", err)
	}
	put := func(_ context.Context, b [crypto.BlindLen]byte, data []byte) error { return store.Put(b, data) }
	if err := Export(ctx, key, src.model, src.chunks, src.fc, put); err != nil {
		t.Fatalf("Export: %v", err)
	}
	tamperOneChunkBlob(t, store.dir)

	dst := newFolder(t, key)
	get := func(_ context.Context, b [crypto.BlindLen]byte) ([]byte, error) { return store.Get(b) }
	if err := Restore(ctx, key, dst.chunks, dst.fc, dst.root, get); err == nil {
		t.Fatal("Restore accepted a tampered chunk blob")
	}
}

func testKey(b byte) [crypto.MasterKeyLen]byte {
	var k [crypto.MasterKeyLen]byte
	for i := range k {
		k[i] = b ^ byte(i)
	}
	return k
}

func assertHolderLeaksNothing(t *testing.T, dir string, needles ...any) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read holder dir: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("holder stored nothing")
	}
	for _, e := range entries {
		blob, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatalf("read blob: %v", err)
		}
		for _, n := range needles {
			var probe []byte
			switch v := n.(type) {
			case []byte:
				probe = v
			case string:
				probe = []byte(v)
			}
			if bytes.Contains(blob, probe) {
				t.Fatalf("blob %s leaks plaintext %q", e.Name(), probe)
			}
		}
	}
}

func tamperOneChunkBlob(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read holder dir: %v", err)
	}
	var biggest os.DirEntry
	var biggestSize int64
	for _, e := range entries {
		fi, err := e.Info()
		if err != nil {
			t.Fatalf("stat: %v", err)
		}
		if fi.Size() > biggestSize {
			biggest, biggestSize = e, fi.Size()
		}
	}
	if biggest == nil {
		t.Fatal("no blob to tamper")
	}
	p := filepath.Join(dir, biggest.Name())
	blob, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read blob: %v", err)
	}
	blob[len(blob)/2] ^= 0xFF
	if err := os.WriteFile(p, blob, 0o600); err != nil {
		t.Fatalf("write blob: %v", err)
	}
}

func assertTreesEqual(t *testing.T, wantRoot, gotRoot string) {
	t.Helper()
	want := walk(t, wantRoot)
	got := walk(t, gotRoot)
	if len(want) != len(got) {
		t.Fatalf("entry count: want %d, got %d\nwant=%v\ngot=%v", len(want), len(got), keys(want), keys(got))
	}
	for rel, w := range want {
		g, ok := got[rel]
		if !ok {
			t.Fatalf("restored tree missing %q", rel)
		}
		if w.mode != g.mode {
			t.Fatalf("%q mode: want %v, got %v", rel, w.mode, g.mode)
		}
		switch {
		case w.link != "":
			if w.link != g.link {
				t.Fatalf("%q symlink: want %q, got %q", rel, w.link, g.link)
			}
		case !w.dir:
			if !bytes.Equal(w.data, g.data) {
				t.Fatalf("%q content differs (want %d bytes, got %d bytes)", rel, len(w.data), len(g.data))
			}
		}
	}
}

type entry struct {
	dir  bool
	mode os.FileMode
	data []byte
	link string
}

func walk(t *testing.T, root string) map[string]entry {
	t.Helper()
	out := make(map[string]entry)
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		if rel == "." {
			return nil
		}
		fi, err := os.Lstat(path)
		if err != nil {
			return err
		}
		e := entry{dir: d.IsDir(), mode: fi.Mode().Perm()}
		switch {
		case fi.Mode()&os.ModeSymlink != 0:
			if e.link, err = os.Readlink(path); err != nil {
				return err
			}
		case !d.IsDir():
			if e.data, err = os.ReadFile(path); err != nil {
				return err
			}
		}
		out[rel] = e
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	return out
}

func keys(m map[string]entry) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
