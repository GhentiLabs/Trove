package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/GhentiLabs/Trove/client/internal/hasher"
	"github.com/GhentiLabs/Trove/client/internal/storage"
)

func genData(n int, seed uint64) []byte {
	r := rand.New(rand.NewPCG(seed, 0x1234))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

func newStore(t *testing.T, target int64) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	db, err := storage.Open(storage.Options{Path: filepath.Join(dir, "idx.db"), MaxOpenConns: 4})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	s, err := Open(Options{DB: db, BlobDir: filepath.Join(dir, "blobs"), BlobTargetSize: target})
	if err != nil {
		t.Fatalf("chunkstore.Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, dir
}

func refcount(t *testing.T, s *Store, id hasher.ChunkID) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow(context.Background(), `SELECT refcount FROM chunks WHERE chunk_id = ?`, id.Bytes()).Scan(&n); err != nil {
		t.Fatalf("refcount: %v", err)
	}
	return n
}

func countChunks(t *testing.T, s *Store) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow(context.Background(), `SELECT COUNT(*) FROM chunks`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

func TestPutGetDedup(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t, 0)
	data := []byte("content addressed storage")

	id1, err := s.Put(ctx, FolderContext{}, data)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	id2, err := s.Put(ctx, FolderContext{}, data)
	if err != nil {
		t.Fatalf("Put again: %v", err)
	}
	if id1 != id2 {
		t.Fatal("same content produced different ids")
	}
	if rc := refcount(t, s, id1); rc != 2 {
		t.Fatalf("refcount = %d, want 2", rc)
	}
	if n := countChunks(t, s); n != 1 {
		t.Fatalf("chunk count = %d, want 1 (deduped)", n)
	}

	got, err := s.Get(ctx, FolderContext{}, id1)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("Get mismatch")
	}

	if _, err := s.Get(ctx, FolderContext{}, hasher.Sum([]byte("nope"))); !errors.Is(err, ErrChunkNotFound) {
		t.Fatalf("Get(missing) err = %v, want ErrChunkNotFound", err)
	}
}

func TestCorruptBlobDetected(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t, 0)
	data := genData(2048, 7) // random => stored uncompressed, so hash check catches it

	id, err := s.Put(ctx, FolderContext{}, data)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	var path string
	var offset int64
	if err := s.db.QueryRow(ctx,
		`SELECT b.path, c.blob_offset FROM chunks c JOIN blobs b ON b.blob_id = c.blob_id WHERE c.chunk_id = ?`,
		id.Bytes()).Scan(&path, &offset); err != nil {
		t.Fatalf("locate: %v", err)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open blob: %v", err)
	}
	if _, err := f.WriteAt([]byte{0xFF}, offset); err != nil {
		t.Fatalf("corrupt: %v", err)
	}
	_ = f.Close()

	if _, err := s.Get(ctx, FolderContext{}, id); !errors.Is(err, ErrHashMismatch) {
		t.Fatalf("Get(corrupt) err = %v, want ErrHashMismatch", err)
	}
}

func TestEncryptedRoundTripAndDedup(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t, 0)
	var key [32]byte
	for i := range key {
		key[i] = byte(i + 1)
	}
	fc := FolderContext{Encrypted: true, MasterKey: key}
	data := bytes.Repeat([]byte("secret "), 4096)

	id, err := s.Put(ctx, fc, data)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := s.Put(ctx, fc, data); err != nil {
		t.Fatalf("Put again: %v", err)
	}
	if rc := refcount(t, s, id); rc != 2 {
		t.Fatalf("refcount = %d, want 2 (encrypted dedup)", rc)
	}

	got, err := s.Get(ctx, fc, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("encrypted round trip mismatch")
	}

	if _, err := s.Get(ctx, FolderContext{}, id); !errors.Is(err, ErrNoKey) {
		t.Fatalf("Get without key err = %v, want ErrNoKey", err)
	}
}

func TestRollover(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t, 4096)

	for i := range 10 {
		if _, err := s.Put(ctx, FolderContext{}, genData(1500, uint64(i))); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	var blobs int
	if err := s.db.QueryRow(ctx, `SELECT COUNT(*) FROM blobs`).Scan(&blobs); err != nil {
		t.Fatalf("count blobs: %v", err)
	}
	if blobs < 2 {
		t.Fatalf("expected rollover to multiple blobs, got %d", blobs)
	}
}

func TestCrashRecoveryTruncatesOrphanTail(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "idx.db")
	blobDir := filepath.Join(dir, "blobs")

	db1, err := storage.Open(storage.Options{Path: dbPath, MaxOpenConns: 4})
	if err != nil {
		t.Fatalf("open db1: %v", err)
	}
	s1, err := Open(Options{DB: db1, BlobDir: blobDir})
	if err != nil {
		t.Fatalf("open store1: %v", err)
	}
	data := genData(1024, 1)
	id, err := s1.Put(ctx, FolderContext{}, data)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	var path string
	var committed int64
	if err := db1.QueryRow(ctx, `SELECT path, size FROM blobs ORDER BY blob_id DESC LIMIT 1`).Scan(&path, &committed); err != nil {
		t.Fatalf("blob info: %v", err)
	}
	_ = s1.Close()

	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open blob: %v", err)
	}
	if _, err := f.WriteAt(genData(500, 2), committed); err != nil {
		t.Fatalf("write orphan: %v", err)
	}
	_ = f.Close()
	_ = db1.Close()

	db2, err := storage.Open(storage.Options{Path: dbPath, MaxOpenConns: 4})
	if err != nil {
		t.Fatalf("open db2: %v", err)
	}
	t.Cleanup(func() { _ = db2.Close() })
	s2, err := Open(Options{DB: db2, BlobDir: blobDir})
	if err != nil {
		t.Fatalf("open store2: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Size() != committed {
		t.Fatalf("blob size = %d after recovery, want %d", info.Size(), committed)
	}
	got, err := s2.Get(ctx, FolderContext{}, id)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("Get after recovery: %v", err)
	}
	if _, err := s2.Put(ctx, FolderContext{}, genData(800, 3)); err != nil {
		t.Fatalf("Put after recovery: %v", err)
	}
}

func TestVirtualBackingAndFileChange(t *testing.T) {
	ctx := context.Background()
	s, dir := newStore(t, 0)

	data := genData(3<<20, 5)
	path := filepath.Join(dir, "work.bin")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	ids, err := s.MirrorFile(ctx, path)
	if err != nil {
		t.Fatalf("MirrorFile: %v", err)
	}
	if len(ids) < 2 {
		t.Fatalf("expected multiple chunks, got %d", len(ids))
	}

	var out bytes.Buffer
	if err := s.Reassemble(ctx, FolderContext{}, ids, &out); err != nil {
		t.Fatalf("Reassemble: %v", err)
	}
	if !bytes.Equal(out.Bytes(), data) {
		t.Fatal("virtual reassembly mismatch")
	}

	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("reopen file: %v", err)
	}
	if _, err := f.WriteAt([]byte{^data[0]}, 0); err != nil {
		t.Fatalf("mutate: %v", err)
	}
	_ = f.Close()

	if _, err := s.Get(ctx, FolderContext{}, ids[0]); !errors.Is(err, ErrFileChanged) {
		t.Fatalf("Get after file change err = %v, want ErrFileChanged", err)
	}
}

func TestVirtualFileDeletedIsNotFound(t *testing.T) {
	ctx := context.Background()
	s, dir := newStore(t, 0)

	data := genData(2<<20, 11)
	path := filepath.Join(dir, "gone.bin")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
	ids, err := s.MirrorFile(ctx, path)
	if err != nil {
		t.Fatalf("MirrorFile: %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if _, err := s.Get(ctx, FolderContext{}, ids[0]); !errors.Is(err, ErrChunkNotFound) {
		t.Fatalf("Get after delete err = %v, want ErrChunkNotFound", err)
	}
}

func TestImportReassembleMatrix(t *testing.T) {
	ctx := context.Background()
	data := genData(5<<20, 6)
	var key [32]byte
	key[0] = 9

	cases := []struct {
		name string
		fc   FolderContext
	}{
		{"plaintext", FolderContext{}},
		{"encrypted", FolderContext{Encrypted: true, MasterKey: key}},
	}
	for _, tc := range cases {
		t.Run("physical/"+tc.name, func(t *testing.T) {
			s, _ := newStore(t, 0)
			ids, err := s.ImportStream(ctx, tc.fc, bytes.NewReader(data))
			if err != nil {
				t.Fatalf("ImportStream: %v", err)
			}
			var out bytes.Buffer
			if err := s.Reassemble(ctx, tc.fc, ids, &out); err != nil {
				t.Fatalf("Reassemble: %v", err)
			}
			if !bytes.Equal(out.Bytes(), data) {
				t.Fatal("restore not bit-exact")
			}
		})
	}

	t.Run("virtual", func(t *testing.T) {
		s, dir := newStore(t, 0)
		path := filepath.Join(dir, "f.bin")
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}
		ids, err := s.MirrorFile(ctx, path)
		if err != nil {
			t.Fatalf("MirrorFile: %v", err)
		}
		var out bytes.Buffer
		if err := s.Reassemble(ctx, FolderContext{}, ids, &out); err != nil {
			t.Fatalf("Reassemble: %v", err)
		}
		if !bytes.Equal(out.Bytes(), data) {
			t.Fatal("virtual restore not bit-exact")
		}
	})
}

func TestSchemaTooNew(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := storage.Open(storage.Options{Path: filepath.Join(dir, "idx.db"), MaxOpenConns: 4})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	s, err := Open(Options{DB: db, BlobDir: filepath.Join(dir, "blobs")})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_ = s.Close()

	if _, err := db.Exec(ctx, `UPDATE meta SET value = ? WHERE key = 'schema_version'`, SchemaVersion+1); err != nil {
		t.Fatalf("bump version: %v", err)
	}
	if _, err := Open(Options{DB: db, BlobDir: filepath.Join(dir, "blobs")}); !errors.Is(err, ErrSchemaTooNew) {
		t.Fatalf("Open err = %v, want ErrSchemaTooNew", err)
	}
}

func TestZeroKeyRejected(t *testing.T) {
	s, _ := newStore(t, 0)
	if _, err := s.Put(context.Background(), FolderContext{Encrypted: true}, []byte("x")); !errors.Is(err, ErrZeroKey) {
		t.Fatalf("Put with zero key err = %v, want ErrZeroKey", err)
	}
}

func TestMirrorRequiresAbsolutePath(t *testing.T) {
	s, _ := newStore(t, 0)
	if _, err := s.MirrorFile(context.Background(), "relative/path.bin"); err == nil {
		t.Fatal("expected error for relative path")
	}
}

func TestBackingMismatch(t *testing.T) {
	ctx := context.Background()
	s, dir := newStore(t, 0)

	// Physical first, then attempt virtual for the same identity.
	data := genData(2000, 1)
	id, err := s.Put(ctx, FolderContext{}, data)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	path := filepath.Join(dir, "f.bin")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := s.PutVirtual(ctx, id, path, 0, len(data), len(data)); !errors.Is(err, ErrBackingMismatch) {
		t.Fatalf("PutVirtual over physical err = %v, want ErrBackingMismatch", err)
	}

	// Virtual first, then attempt physical for the same identity.
	other := genData(2000, 2)
	otherPath := filepath.Join(dir, "g.bin")
	if err := os.WriteFile(otherPath, other, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	vids, err := s.MirrorFile(ctx, otherPath)
	if err != nil {
		t.Fatalf("MirrorFile: %v", err)
	}
	if _, err := s.Put(ctx, FolderContext{}, other); !errors.Is(err, ErrBackingMismatch) {
		t.Fatalf("Put over virtual err = %v, want ErrBackingMismatch", err)
	}
	_ = vids
}

func TestConcurrentPutAndGet(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t, 64<<10) // small target exercises rollover under concurrency

	const readSet = 20
	readData := make([][]byte, readSet)
	readIDs := make([]hasher.ChunkID, readSet)
	for i := range readData {
		readData[i] = genData(2000, uint64(1000+i))
		id, err := s.Put(ctx, FolderContext{}, readData[i])
		if err != nil {
			t.Fatalf("seed put: %v", err)
		}
		readIDs[i] = id
	}

	var wg sync.WaitGroup
	var bad atomic.Int64
	for w := range 8 {
		wg.Go(func() {
			for i := range 25 {
				if _, err := s.Put(ctx, FolderContext{}, genData(2000, uint64(w*100_000+i))); err != nil {
					bad.Add(1)
				}
			}
		})
	}
	for range 8 {
		wg.Go(func() {
			for i := range 100 {
				idx := i % readSet
				got, err := s.Get(ctx, FolderContext{}, readIDs[idx])
				if err != nil || !bytes.Equal(got, readData[idx]) {
					bad.Add(1)
				}
			}
		})
	}
	wg.Wait()
	if bad.Load() != 0 {
		t.Fatalf("%d concurrent failures", bad.Load())
	}
}

func FuzzImportReassemble(f *testing.F) {
	f.Add([]byte("hi"), false)
	f.Add(bytes.Repeat([]byte("xy"), 400_000), true)
	f.Fuzz(func(t *testing.T, data []byte, enc bool) {
		ctx := context.Background()
		dir, err := os.MkdirTemp("", "chunkstore-fuzz")
		if err != nil {
			t.Fatalf("mkdir temp: %v", err)
		}
		defer os.RemoveAll(dir)

		db, err := storage.Open(storage.Options{Path: filepath.Join(dir, "idx.db"), MaxOpenConns: 4})
		if err != nil {
			t.Fatalf("storage.Open: %v", err)
		}
		defer db.Close()
		s, err := Open(Options{DB: db, BlobDir: filepath.Join(dir, "blobs")})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer s.Close()

		var fc FolderContext
		if enc {
			fc.Encrypted = true
			fc.MasterKey[0] = 7
		}
		ids, err := s.ImportStream(ctx, fc, bytes.NewReader(data))
		if err != nil {
			t.Fatalf("ImportStream: %v", err)
		}
		var out bytes.Buffer
		if err := s.Reassemble(ctx, fc, ids, &out); err != nil {
			t.Fatalf("Reassemble: %v", err)
		}
		if !bytes.Equal(out.Bytes(), data) {
			t.Fatalf("restore not bit-exact: %d in, %d out", len(data), out.Len())
		}
	})
}

func TestDedupOnSmallEdit(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t, 0)
	data := genData(8<<20, 8)

	if _, err := s.ImportStream(ctx, FolderContext{}, bytes.NewReader(data)); err != nil {
		t.Fatalf("import 1: %v", err)
	}
	before := countChunks(t, s)

	edited := bytes.Clone(data)
	mid := len(edited) / 2
	for i := range 16 {
		edited[mid+i] ^= 0xFF
	}
	if _, err := s.ImportStream(ctx, FolderContext{}, bytes.NewReader(edited)); err != nil {
		t.Fatalf("import 2: %v", err)
	}
	added := countChunks(t, s) - before
	if added < 1 || added > 3 {
		t.Fatalf("small edit added %d chunks, want 1..3", added)
	}
}
