package chunkstore

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/hasher"
	"github.com/GhentiLabs/Trove/client/internal/manifest"
	"github.com/GhentiLabs/Trove/client/internal/storage"
)

// backingOf reports how a chunk is stored; used by tests to assert re-pointing.
func (s *Store) backingOf(ctx context.Context, id hasher.ChunkID) (Backing, bool, error) {
	var b int
	err := s.db.QueryRow(ctx, `SELECT backing FROM chunks WHERE chunk_id = ?`, id.Bytes()).Scan(&b)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, false, nil
	case err != nil:
		return 0, false, err
	}
	return Backing(b), true, nil
}

func idsOf(refs []manifest.ChunkRef) []hasher.ChunkID {
	ids := make([]hasher.ChunkID, len(refs))
	for i, r := range refs {
		ids[i] = r.ID
	}
	return ids
}

func blobBytes(t *testing.T, s *Store) int64 {
	t.Helper()
	var total int64
	if err := s.db.QueryRow(context.Background(), `SELECT COALESCE(SUM(size), 0) FROM blobs`).Scan(&total); err != nil {
		t.Fatalf("sum blob sizes: %v", err)
	}
	return total
}

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

func lastSeen(t *testing.T, s *Store, id hasher.ChunkID) int64 {
	t.Helper()
	var ms int64
	if err := s.db.QueryRow(context.Background(), `SELECT last_seen_ms FROM chunks WHERE chunk_id = ?`, id.Bytes()).Scan(&ms); err != nil {
		t.Fatalf("last_seen: %v", err)
	}
	return ms
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
	if lastSeen(t, s, id1) == 0 {
		t.Fatal("last_seen not set after put")
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
		`SELECT b.path, c.backing_offset FROM chunks c JOIN blobs b ON b.blob_id = c.blob_id WHERE c.chunk_id = ?`,
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
	if n := countChunks(t, s); n != 1 {
		t.Fatalf("chunk count = %d, want 1 (encrypted dedup)", n)
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

func TestIterChunks(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t, 0)
	want := map[hasher.ChunkID]bool{}
	for i := range 5 {
		id, err := s.Put(ctx, FolderContext{}, genData(1000, uint64(i)))
		if err != nil {
			t.Fatalf("Put: %v", err)
		}
		want[id] = true
	}
	seen := map[hasher.ChunkID]bool{}
	for st, err := range s.IterChunks(ctx) {
		if err != nil {
			t.Fatalf("IterChunks: %v", err)
		}
		if st.LastSeen <= 0 {
			t.Fatalf("chunk %s has no last_seen", st.ID)
		}
		seen[st.ID] = true
	}
	if len(seen) != len(want) {
		t.Fatalf("iterated %d chunks, want %d", len(seen), len(want))
	}
	for id := range want {
		if !seen[id] {
			t.Fatalf("chunk %s missing from iteration", id)
		}
	}
}

func TestDeleteChunks(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t, 0)
	a, _ := s.Put(ctx, FolderContext{}, genData(1000, 1))
	b, _ := s.Put(ctx, FolderContext{}, genData(1000, 2))

	before := lastSeen(t, s, a) + 1
	if n, err := s.DeleteChunks(ctx, []hasher.ChunkID{a}, before); err != nil || n != 1 {
		t.Fatalf("DeleteChunks: n=%d err=%v, want 1, nil", n, err)
	}
	if _, err := s.Get(ctx, FolderContext{}, a); !errors.Is(err, ErrChunkNotFound) {
		t.Fatalf("deleted chunk Get err = %v, want ErrChunkNotFound", err)
	}
	if _, err := s.Get(ctx, FolderContext{}, b); err != nil {
		t.Fatalf("surviving chunk gone: %v", err)
	}
	// Idempotent: deleting again (and a missing id) is a no-op.
	if n, err := s.DeleteChunks(ctx, []hasher.ChunkID{a}, before); err != nil || n != 0 {
		t.Fatalf("re-delete: n=%d err=%v, want 0, nil", n, err)
	}
}

func TestDeleteChunksRespectsLastSeenGuard(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t, 0)
	id, err := s.Put(ctx, FolderContext{}, genData(1000, 1))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	ls := lastSeen(t, s, id)

	// beforeMs at or below last_seen: the guard spares the chunk.
	if n, err := s.DeleteChunks(ctx, []hasher.ChunkID{id}, ls); err != nil || n != 0 {
		t.Fatalf("guarded delete: n=%d err=%v, want 0, nil", n, err)
	}
	if ok, err := s.Has(ctx, id); err != nil || !ok {
		t.Fatalf("chunk missing after guarded delete: ok=%v err=%v", ok, err)
	}

	// beforeMs above last_seen: the chunk is deleted.
	if n, err := s.DeleteChunks(ctx, []hasher.ChunkID{id}, ls+1); err != nil || n != 1 {
		t.Fatalf("delete past guard: n=%d err=%v, want 1, nil", n, err)
	}
	if ok, err := s.Has(ctx, id); err != nil || ok {
		t.Fatalf("chunk present after delete: ok=%v err=%v", ok, err)
	}
}

func TestReclaimBlobs(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t, 100) // tiny target so each put rolls to a new blob
	a, _ := s.Put(ctx, FolderContext{}, genData(200, 1))
	if _, err := s.Put(ctx, FolderContext{}, genData(200, 2)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// a lives in a rolled, now-closed blob. Delete it and reclaim.
	if _, err := s.DeleteChunks(ctx, []hasher.ChunkID{a}, lastSeen(t, s, a)+1); err != nil {
		t.Fatal(err)
	}
	n, err := s.ReclaimBlobs(ctx)
	if err != nil {
		t.Fatalf("ReclaimBlobs: %v", err)
	}
	if n != 1 {
		t.Fatalf("reclaimed %d blobs, want 1", n)
	}
	// The open blob (holding chunk b) must survive.
	if got := countChunks(t, s); got != 1 {
		t.Fatalf("chunk count after reclaim = %d, want 1", got)
	}
}

func TestOpenRejectsFutureSchema(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t, 0)
	if _, err := s.db.Exec(ctx, `UPDATE meta SET value='99' WHERE key='schema_version'`); err != nil {
		t.Fatalf("bump version: %v", err)
	}
	_ = s.Close()

	db, err := storage.Open(storage.Options{Path: s.db.Path(), MaxOpenConns: 1})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := Open(Options{DB: db, BlobDir: filepath.Join(filepath.Dir(s.db.Path()), "blobs")}); !errors.Is(err, ErrSchemaTooNew) {
		t.Fatalf("Open future schema err = %v, want ErrSchemaTooNew", err)
	}
}

func TestIngestCloneRoundTrip(t *testing.T) {
	ctx := context.Background()
	s, dir := newStore(t, 0)

	data := genData(5<<20, 21)
	path := filepath.Join(dir, "work.bin")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	refs, err := s.IngestClone(ctx, path)
	if err != nil {
		t.Fatalf("IngestClone: %v", err)
	}
	if len(refs) < 2 {
		t.Fatalf("expected multiple chunks, got %d", len(refs))
	}

	var out bytes.Buffer
	if err := s.Reassemble(ctx, FolderContext{}, idsOf(refs), &out); err != nil {
		t.Fatalf("Reassemble: %v", err)
	}
	if !bytes.Equal(out.Bytes(), data) {
		t.Fatal("clone reassembly not bit-exact")
	}
	if b, ok, _ := s.backingOf(ctx, refs[0].ID); !ok || b != BackingClone {
		t.Fatalf("backing = %v exists=%v, want clone", b, ok)
	}
}

// TestIngestCloneFrozenIdentity asserts the storage layout does not leak into
// identity: clone-backed chunk ids equal the physical ids for the same content.
func TestIngestCloneFrozenIdentity(t *testing.T) {
	ctx := context.Background()
	data := genData(5<<20, 22)

	sClone, dir := newStore(t, 0)
	path := filepath.Join(dir, "f.bin")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	cloneRefs, err := sClone.IngestClone(ctx, path)
	if err != nil {
		t.Fatalf("IngestClone: %v", err)
	}

	sPhys, _ := newStore(t, 0)
	physIDs, err := sPhys.ImportStream(ctx, FolderContext{}, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("ImportStream: %v", err)
	}
	if len(cloneRefs) != len(physIDs) {
		t.Fatalf("chunk count differs: clone %d, physical %d", len(cloneRefs), len(physIDs))
	}
	for i := range physIDs {
		if cloneRefs[i].ID != physIDs[i] {
			t.Fatalf("identity differs at chunk %d", i)
		}
	}
}

// TestCloneSurvivesWorkingFileOverwrite is the storage-layer half of the
// history-survival property: the clone is an independent inode, so overwriting
// the working file in place leaves its chunks byte-exact (the OS forks the old
// blocks). No promote is needed.
func TestCloneSurvivesWorkingFileOverwrite(t *testing.T) {
	ctx := context.Background()
	s, dir := newStore(t, 0)

	orig := genData(4<<20, 23)
	path := filepath.Join(dir, "f.bin")
	if err := os.WriteFile(path, orig, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	refs, err := s.IngestClone(ctx, path)
	if err != nil {
		t.Fatalf("IngestClone: %v", err)
	}
	if len(refs) < 2 {
		t.Fatalf("expected a multi-chunk file, got %d chunks", len(refs))
	}

	newData := genData(4<<20, 24)
	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if _, err := f.WriteAt(newData, 0); err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	_ = f.Close()

	// The whole old version still reassembles bit-exact from the preserved clone.
	var out bytes.Buffer
	if err := s.Reassemble(ctx, FolderContext{}, idsOf(refs), &out); err != nil {
		t.Fatalf("reassemble old version: %v", err)
	}
	if !bytes.Equal(out.Bytes(), orig) {
		t.Fatal("old version not byte-exact after the working file was overwritten in place")
	}
}

// TestRepointPhysicalToClone covers a chunk first stored physically (e.g. pulled
// over the wire) being re-pointed to a clone on a later ingest of the same content.
func TestRepointPhysicalToClone(t *testing.T) {
	ctx := context.Background()
	s, dir := newStore(t, 0)

	data := genData(3000, 25)
	id, err := s.Put(ctx, FolderContext{}, data)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if b, _, _ := s.backingOf(ctx, id); b != BackingPhysical {
		t.Fatalf("backing = %v, want physical", b)
	}

	path := filepath.Join(dir, "f.bin")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := s.IngestClone(ctx, path); err != nil {
		t.Fatalf("IngestClone: %v", err)
	}
	if b, ok, _ := s.backingOf(ctx, id); !ok || b != BackingClone {
		t.Fatalf("backing = %v exists=%v, want clone", b, ok)
	}
	got, err := s.Get(ctx, FolderContext{}, id)
	if err != nil {
		t.Fatalf("Get after re-point: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("re-pointed chunk not byte-exact")
	}
}

// TestPutDedupsAgainstClone covers the pull path racing an ingest: a chunk a peer
// sends physically, already re-pointed to a clone locally, must dedup rather than
// fail with a backing conflict.
func TestPutDedupsAgainstClone(t *testing.T) {
	ctx := context.Background()
	s, dir := newStore(t, 0)

	data := genData(3000, 41)
	path := filepath.Join(dir, "f.bin")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	refs, err := s.IngestClone(ctx, path)
	if err != nil {
		t.Fatalf("IngestClone: %v", err)
	}
	id := refs[0].ID

	got, err := s.Put(ctx, FolderContext{}, data)
	if err != nil {
		t.Fatalf("Put over existing clone: %v", err)
	}
	if got != id {
		t.Fatalf("Put returned %s, want %s", got, id)
	}
	if b, _, _ := s.backingOf(ctx, id); b != BackingClone {
		t.Fatalf("backing = %v after dedup Put, want clone", b)
	}
	out, err := s.Get(ctx, FolderContext{}, id)
	if err != nil || !bytes.Equal(out, data) {
		t.Fatalf("Get after dedup Put: err=%v equal=%v", err, bytes.Equal(out, data))
	}
}

// TestIngestCloneEmptyFile checks an empty file produces no chunks and leaves no
// clone object behind (it would otherwise be an immediately-orphaned object row
// and 0-byte file on every rescan).
func TestIngestCloneEmptyFile(t *testing.T) {
	ctx := context.Background()
	s, dir := newStore(t, 0)

	path := filepath.Join(dir, "empty.bin")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	refs, err := s.IngestClone(ctx, path)
	if err != nil {
		t.Fatalf("IngestClone empty: %v", err)
	}
	if len(refs) != 0 {
		t.Fatalf("refs = %d, want 0 for an empty file", len(refs))
	}
	if n, err := s.ReclaimObjects(ctx); err != nil || n != 0 {
		t.Fatalf("ReclaimObjects after empty ingest = %d, %v; want 0 (no orphan)", n, err)
	}
}

// TestIngestCloneSharedChunkReclaimsOldObject covers cross-file dedup: ingesting a
// second file with identical content re-points the shared chunks onto its clone,
// orphaning the first file's clone, which GC then reclaims while the chunk stays
// servable.
func TestIngestCloneSharedChunkReclaimsOldObject(t *testing.T) {
	ctx := context.Background()
	s, dir := newStore(t, 0)

	data := genData(1<<20, 42)
	a := filepath.Join(dir, "a.bin")
	b := filepath.Join(dir, "b.bin")
	if err := os.WriteFile(a, data, 0o644); err != nil {
		t.Fatalf("write a: %v", err)
	}
	if err := os.WriteFile(b, data, 0o644); err != nil {
		t.Fatalf("write b: %v", err)
	}

	refsA, err := s.IngestClone(ctx, a)
	if err != nil {
		t.Fatalf("IngestClone a: %v", err)
	}
	refsB, err := s.IngestClone(ctx, b)
	if err != nil {
		t.Fatalf("IngestClone b: %v", err)
	}
	if refsA[0].ID != refsB[0].ID {
		t.Fatal("identical content produced different chunk ids")
	}
	if n, err := s.ReclaimObjects(ctx); err != nil || n != 1 {
		t.Fatalf("ReclaimObjects = %d, %v; want 1 (a's superseded clone)", n, err)
	}
	got, err := s.Get(ctx, FolderContext{}, refsB[0].ID)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("Get after orphan reclaim: err=%v equal=%v", err, bytes.Equal(got, data))
	}
}

// TestReclaimObjects covers the clone reclaimer: an object still backing a chunk
// is kept; once its chunks are deleted it is reclaimed; and reclaiming it leaves
// the user's working file intact (the separate-inode refcount property).
func TestReclaimObjects(t *testing.T) {
	ctx := context.Background()
	s, dir := newStore(t, 0)

	data := genData(4<<20, 26)
	path := filepath.Join(dir, "f.bin")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	refs, err := s.IngestClone(ctx, path)
	if err != nil {
		t.Fatalf("IngestClone: %v", err)
	}

	if n, err := s.ReclaimObjects(ctx); err != nil || n != 0 {
		t.Fatalf("ReclaimObjects with live chunks = %d, %v; want 0, nil", n, err)
	}

	cutoff := time.Now().Add(time.Hour).UnixMilli()
	if _, err := s.DeleteChunks(ctx, idsOf(refs), cutoff); err != nil {
		t.Fatalf("DeleteChunks: %v", err)
	}
	if n, err := s.ReclaimObjects(ctx); err != nil || n != 1 {
		t.Fatalf("ReclaimObjects after delete = %d, %v; want 1, nil", n, err)
	}

	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("working file harmed by object reclaim: err=%v equal=%v", err, bytes.Equal(got, data))
	}
	if _, err := s.Get(ctx, FolderContext{}, refs[0].ID); !errors.Is(err, ErrChunkNotFound) {
		t.Fatalf("Get after reclaim err = %v, want ErrChunkNotFound", err)
	}
}

// TestRepointReclaimsOpenPullBlob mirrors the replica path: chunks pulled
// physically into the open blob are re-pointed to a clone on materialize, after
// which the now-empty pull blob's space is reclaimed in place so the replica
// settles to ~1x.
func TestRepointReclaimsOpenPullBlob(t *testing.T) {
	ctx := context.Background()
	s, dir := newStore(t, 0)

	data := genData(5<<20, 31)
	ids, err := s.ImportStream(ctx, FolderContext{}, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("ImportStream (pull): %v", err)
	}
	if blobBytes(t, s) == 0 {
		t.Fatal("expected pulled bytes in the open blob")
	}

	path := filepath.Join(dir, "f.bin")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	refs, err := s.IngestClone(ctx, path)
	if err != nil {
		t.Fatalf("IngestClone: %v", err)
	}
	if len(refs) != len(ids) {
		t.Fatalf("chunk count differs: clone %d, pulled %d", len(refs), len(ids))
	}

	if _, err := s.ReclaimBlobs(ctx); err != nil {
		t.Fatalf("ReclaimBlobs: %v", err)
	}
	if b := blobBytes(t, s); b != 0 {
		t.Fatalf("open pull blob holds %d bytes after re-point and reclaim, want 0", b)
	}
	if _, err := s.Get(ctx, FolderContext{}, refs[0].ID); err != nil {
		t.Fatalf("Get after reclaim: %v", err)
	}
	if b, _, _ := s.backingOf(ctx, refs[0].ID); b != BackingClone {
		t.Fatalf("backing = %v, want clone", b)
	}
}

// TestOpenSweepsOrphanObjects covers crash recovery: a clone object file with no
// committed row is removed at Open, while committed clones still serve.
func TestOpenSweepsOrphanObjects(t *testing.T) {
	ctx := context.Background()
	s, dir := newStore(t, 0)

	data := genData(2<<20, 27)
	path := filepath.Join(dir, "f.bin")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	refs, err := s.IngestClone(ctx, path)
	if err != nil {
		t.Fatalf("IngestClone: %v", err)
	}

	orphan := filepath.Join(dir, "objects", "99999999999999999999.clone")
	if err := os.WriteFile(orphan, []byte("orphan"), 0o600); err != nil {
		t.Fatalf("plant orphan: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s2, err := Open(Options{DB: s.db, BlobDir: filepath.Join(dir, "blobs")})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })

	if _, err := os.Stat(orphan); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("orphan object not swept at Open: %v", err)
	}
	if _, err := s2.Get(ctx, FolderContext{}, refs[0].ID); err != nil {
		t.Fatalf("committed clone unreadable after reopen: %v", err)
	}
}
