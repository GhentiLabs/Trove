package holder

import (
	"bytes"
	"context"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/chunkstore"
	"github.com/GhentiLabs/Trove/client/internal/crypto"
	"github.com/GhentiLabs/Trove/client/internal/hasher"
	"github.com/GhentiLabs/Trove/client/internal/manifest"
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
	sc, err := scanner.New(scanner.Options{Root: f.root, Chunks: f.chunks, Model: f.model, Watcher: watcher.NewFake()})
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
	writeFile(t, src.root, "empty.txt", nil)
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
	put := func(_ context.Context, b [crypto.BlindIDLen]byte, data []byte) error { return store.Put(b, data) }
	get := func(_ context.Context, b [crypto.BlindIDLen]byte) ([]byte, error) { return store.Get(b) }

	if err := Reconcile(ctx, key, src.model, src.chunks, src.fc, storeHas(store), put); err != nil {
		t.Fatalf("Export: %v", err)
	}

	assertHolderLeaksNothing(t, store.dir, secret, "secret-filename.txt", "notes.md")

	dst := newFolder(t, key)
	if err := Restore(ctx, key, dst.chunks, dst.fc, dst.root, get); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	assertTreesEqual(t, src.root, dst.root)
}

// TestReconcileSkipsExistingBlobs checks a second reconcile of an unchanged folder pushes
// only the tiny pointer — never re-uploading chunks the holder already has.
func TestReconcileSkipsExistingBlobs(t *testing.T) {
	ctx := context.Background()
	key := testKey(0x21)
	src := newFolder(t, key)
	writeFile(t, src.root, "a.txt", []byte("hello"))
	writeFile(t, src.root, "big.bin", pseudoRandom(3<<20, 4))
	src.scan(t)

	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("holder Open: %v", err)
	}
	var puts atomic.Int64
	put := func(_ context.Context, b [crypto.BlindIDLen]byte, data []byte) error {
		puts.Add(1)
		return store.Put(b, data)
	}
	if err := Reconcile(ctx, key, src.model, src.chunks, src.fc, storeHas(store), put); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	if first := puts.Load(); first < 3 {
		t.Fatalf("first reconcile pushed only %d blobs", first)
	}

	puts.Store(0)
	if err := Reconcile(ctx, key, src.model, src.chunks, src.fc, storeHas(store), put); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if n := puts.Load(); n != 1 {
		t.Fatalf("second reconcile pushed %d blobs, want 1 (the pointer only)", n)
	}
}

// TestCollectReclaimsSupersededBlobs checks GC deletes the old catalog and the chunks of a
// deleted file while keeping everything the current version needs (restore stays bit-exact).
func TestCollectReclaimsSupersededBlobs(t *testing.T) {
	ctx := context.Background()
	key := testKey(0x5C)
	src := newFolder(t, key)
	writeFile(t, src.root, "keep.txt", []byte("keep me"))
	writeFile(t, src.root, "gone.bin", pseudoRandom(3<<20, 8))
	src.scan(t)

	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("holder Open: %v", err)
	}
	put := func(_ context.Context, b [crypto.BlindIDLen]byte, data []byte) error { return store.Put(b, data) }
	if err := Reconcile(ctx, key, src.model, src.chunks, src.fc, storeHas(store), put); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}

	if err := os.Remove(filepath.Join(src.root, "gone.bin")); err != nil {
		t.Fatalf("remove: %v", err)
	}
	writeFile(t, src.root, "keep.txt", []byte("keep me, now edited and longer"))
	src.scan(t)
	if err := Reconcile(ctx, key, src.model, src.chunks, src.fc, storeHas(store), put); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	beforeGC := len(holderBlobs(t, store.dir))

	now := time.Now().UnixMilli() + 1000
	if err := Collect(ctx, key, src.model, storeList(store), storeDelete(store), 0, now); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	afterGC := len(holderBlobs(t, store.dir))
	if afterGC >= beforeGC {
		t.Fatalf("GC reclaimed nothing: %d -> %d blobs", beforeGC, afterGC)
	}

	dst := newFolder(t, key)
	get := func(_ context.Context, b [crypto.BlindIDLen]byte) ([]byte, error) { return store.Get(b) }
	if err := Restore(ctx, key, dst.chunks, dst.fc, dst.root, get); err != nil {
		t.Fatalf("restore after GC: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dst.root, "keep.txt"))
	if err != nil || string(got) != "keep me, now edited and longer" {
		t.Fatalf("restored keep.txt = %q err=%v", got, err)
	}
	if _, err := os.Stat(filepath.Join(dst.root, "gone.bin")); !os.IsNotExist(err) {
		t.Fatal("deleted file reappeared after GC+restore")
	}
}

// TestCollectGraceSkipsRecentBlobs checks GC never reaps a blob written inside the grace
// window, so a concurrent writer's in-flight push survives.
func TestCollectGraceSkipsRecentBlobs(t *testing.T) {
	ctx := context.Background()
	key := testKey(0x6D)
	src := newFolder(t, key)
	writeFile(t, src.root, "a.txt", []byte("one"))
	src.scan(t)
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("holder Open: %v", err)
	}
	put := func(_ context.Context, b [crypto.BlindIDLen]byte, data []byte) error { return store.Put(b, data) }
	if err := Reconcile(ctx, key, src.model, src.chunks, src.fc, storeHas(store), put); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	writeFile(t, src.root, "a.txt", []byte("two, longer than one"))
	src.scan(t)
	if err := Reconcile(ctx, key, src.model, src.chunks, src.fc, storeHas(store), put); err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}
	before := len(holderBlobs(t, store.dir))

	// now ~= the blobs' write time, so a generous grace protects all of them.
	if err := Collect(ctx, key, src.model, storeList(store), storeDelete(store), int64(time.Hour/time.Millisecond), time.Now().UnixMilli()); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if after := len(holderBlobs(t, store.dir)); after != before {
		t.Fatalf("grace did not protect recent blobs: %d -> %d", before, after)
	}
}

// TestReconcileInterruptedKeepsPriorVersion checks that a push interrupted before the
// pointer flip leaves the holder serving its previous, consistent version.
func TestReconcileInterruptedKeepsPriorVersion(t *testing.T) {
	ctx := context.Background()
	key := testKey(0x77)
	src := newFolder(t, key)
	writeFile(t, src.root, "a.txt", []byte("version one"))
	src.scan(t)

	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("holder Open: %v", err)
	}
	put := func(_ context.Context, b [crypto.BlindIDLen]byte, data []byte) error { return store.Put(b, data) }
	if err := Reconcile(ctx, key, src.model, src.chunks, src.fc, storeHas(store), put); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}

	writeFile(t, src.root, "a.txt", []byte("version two, different and longer"))
	src.scan(t)
	pointerBlind := crypto.BlindID(key, []byte(pointerLabel))
	failOnPointer := func(_ context.Context, b [crypto.BlindIDLen]byte, data []byte) error {
		if b == pointerBlind {
			return errBadOp
		}
		return store.Put(b, data)
	}
	if err := Reconcile(ctx, key, src.model, src.chunks, src.fc, storeHas(store), failOnPointer); err == nil {
		t.Fatal("interrupted reconcile returned no error")
	}

	dst := newFolder(t, key)
	get := func(_ context.Context, b [crypto.BlindIDLen]byte) ([]byte, error) { return store.Get(b) }
	if err := Restore(ctx, key, dst.chunks, dst.fc, dst.root, get); err != nil {
		t.Fatalf("restore after interrupted push: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dst.root, "a.txt"))
	if err != nil {
		t.Fatalf("read restored file: %v", err)
	}
	if string(got) != "version one" {
		t.Fatalf("restored %q, want the prior version (the pointer never flipped)", got)
	}
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
	put := func(_ context.Context, b [crypto.BlindIDLen]byte, data []byte) error { return store.Put(b, data) }
	if err := Reconcile(ctx, key, src.model, src.chunks, src.fc, storeHas(store), put); err != nil {
		t.Fatalf("Export: %v", err)
	}
	tamperChunkBlob(t, store, key)

	dst := newFolder(t, key)
	get := func(_ context.Context, b [crypto.BlindIDLen]byte) ([]byte, error) { return store.Get(b) }
	if err := Restore(ctx, key, dst.chunks, dst.fc, dst.root, get); err == nil {
		t.Fatal("Restore accepted a tampered chunk blob")
	}
}

// TestRestoreRejectsWrongKey checks restoring with the wrong master key fails and writes
// nothing, rather than producing garbage.
func TestRestoreRejectsWrongKey(t *testing.T) {
	ctx := context.Background()
	src := newFolder(t, testKey(0x10))
	writeFile(t, src.root, "f.txt", []byte("data"))
	src.scan(t)

	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("holder Open: %v", err)
	}
	put := func(_ context.Context, b [crypto.BlindIDLen]byte, data []byte) error { return store.Put(b, data) }
	if err := Reconcile(ctx, testKey(0x10), src.model, src.chunks, src.fc, storeHas(store), put); err != nil {
		t.Fatalf("Export: %v", err)
	}

	wrong := testKey(0x99)
	dst := newFolder(t, wrong)
	get := func(_ context.Context, b [crypto.BlindIDLen]byte) ([]byte, error) { return store.Get(b) }
	if err := Restore(ctx, wrong, dst.chunks, dst.fc, dst.root, get); err == nil {
		t.Fatal("Restore with the wrong key succeeded")
	}
	if entries, _ := os.ReadDir(dst.root); len(entries) != 0 {
		t.Fatalf("wrong-key restore wrote %d entries, want 0", len(entries))
	}
}

// TestRestoreRejectsEscapingSymlink checks a hostile writer's sealed catalog cannot plant
// a symlink whose target escapes the restore root.
func TestRestoreRejectsEscapingSymlink(t *testing.T) {
	ctx := context.Background()
	key := testKey(0x44)
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("holder Open: %v", err)
	}
	evil := EncodeCatalog([]manifest.Manifest{{Kind: manifest.KindSymlink, Path: "evil", SymlinkTarget: "../../escape"}})
	evilID := hasher.Sum(evil)
	sealedCat, err := crypto.Seal(key, evilID, evil)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if err := store.Put(crypto.BlindID(key, evilID[:]), sealedCat); err != nil {
		t.Fatalf("Put catalog: %v", err)
	}
	sealedPtr, err := crypto.SealMutable(key, pointerLabel, evilID[:])
	if err != nil {
		t.Fatalf("SealMutable: %v", err)
	}
	if err := store.Put(crypto.BlindID(key, []byte(pointerLabel)), sealedPtr); err != nil {
		t.Fatalf("Put pointer: %v", err)
	}

	dst := newFolder(t, key)
	get := func(_ context.Context, b [crypto.BlindIDLen]byte) ([]byte, error) { return store.Get(b) }
	if err := Restore(ctx, key, dst.chunks, dst.fc, dst.root, get); err == nil {
		t.Fatal("Restore accepted a catalog with an escaping symlink")
	}
	if _, err := os.Lstat(filepath.Join(dst.root, "evil")); err == nil {
		t.Fatal("escaping symlink was planted before rejection")
	}
}

func testKey(b byte) [crypto.MasterKeyLen]byte {
	var k [crypto.MasterKeyLen]byte
	for i := range k {
		k[i] = b ^ byte(i)
	}
	return k
}

func holderBlobs(t *testing.T, dir string) []string {
	t.Helper()
	var files []string
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk holder dir: %v", err)
	}
	return files
}

func assertHolderLeaksNothing(t *testing.T, dir string, needles ...any) {
	t.Helper()
	files := holderBlobs(t, dir)
	if len(files) == 0 {
		t.Fatal("holder stored nothing")
	}
	for _, f := range files {
		blob, err := os.ReadFile(f)
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
				t.Fatalf("blob %s leaks plaintext %q", f, probe)
			}
		}
	}
}

func storeList(store *Store) ListBlobs {
	return func(_ context.Context, after [crypto.BlindIDLen]byte) ([]BlobRef, error) {
		return store.List(after, MaxListPage)
	}
}

func storeDelete(store *Store) DeleteBlobs {
	return func(_ context.Context, ids [][crypto.BlindIDLen]byte) error {
		for _, id := range ids {
			if err := store.Delete(id); err != nil {
				return err
			}
		}
		return nil
	}
}

// storeHas adapts a local Store to the HasBlobs reconcile callback.
func storeHas(store *Store) HasBlobs {
	return func(_ context.Context, ids [][crypto.BlindIDLen]byte) ([]bool, error) {
		out := make([]bool, len(ids))
		for i, id := range ids {
			out[i] = store.Has(id)
		}
		return out, nil
	}
}

// tamperChunkBlob flips a byte in the largest stored blob, which for a multi-chunk file is a
// chunk (the catalog and pointer are small), exercising chunk verification.
func tamperChunkBlob(t *testing.T, store *Store, _ [crypto.MasterKeyLen]byte) {
	t.Helper()
	var biggest string
	var biggestSize int64
	for _, f := range holderBlobs(t, store.dir) {
		fi, err := os.Stat(f)
		if err != nil {
			t.Fatalf("stat blob: %v", err)
		}
		if fi.Size() > biggestSize {
			biggest, biggestSize = f, fi.Size()
		}
	}
	if biggest == "" {
		t.Fatal("no blob to tamper")
	}
	blob, err := os.ReadFile(biggest)
	if err != nil {
		t.Fatalf("read blob: %v", err)
	}
	blob[len(blob)/2] ^= 0xFF
	if err := os.WriteFile(biggest, blob, 0o600); err != nil {
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
