// Package chunkstore is the content-addressed store. It maps a chunk identity to
// its stored bytes, deduplicating on write, and reassembles the original bytes on
// read. A chunk is backed either physically (compressed, optionally sealed bytes
// packed into append-only blob files) or by a clone (a byte range in a whole-file
// copy-on-write clone of a current file, holding plaintext that shares extents
// with the working file, so current data costs ~1x disk). Every read is verified
// against the requested identity before any bytes are returned.
package chunkstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/chunker"
	"github.com/GhentiLabs/Trove/client/internal/compression"
	"github.com/GhentiLabs/Trove/client/internal/crypto"
	"github.com/GhentiLabs/Trove/client/internal/hasher"
	"github.com/GhentiLabs/Trove/client/internal/manifest"
	"github.com/GhentiLabs/Trove/client/internal/storage"
)

// BlobTargetSize is the size past which the current pack blob rolls over to a new
// one. It is a tunable default, not a hard limit.
const BlobTargetSize = 64 << 20

// Backing identifies where a chunk's bytes live.
type Backing uint8

const (
	// BackingPhysical is compressed (optionally sealed) bytes packed in a blob.
	BackingPhysical Backing = 0
	// BackingClone is a plaintext byte range in a whole-file copy-on-write clone.
	BackingClone Backing = 1
)

var (
	// ErrChunkNotFound is returned when no chunk has the requested identity.
	ErrChunkNotFound = errors.New("chunkstore: chunk not found")
	// ErrHashMismatch is returned when stored bytes fail verification.
	ErrHashMismatch = errors.New("chunkstore: stored bytes failed hash verification")
	// ErrNoKey is returned when reading an encrypted chunk without a folder key.
	ErrNoKey = errors.New("chunkstore: chunk is encrypted but no key was provided")
	// ErrSchemaTooNew is returned when the database was written by a newer binary.
	ErrSchemaTooNew = errors.New("chunkstore: database schema newer than this binary")
	// ErrZeroKey is returned when encryption is requested with a zero master key.
	ErrZeroKey = errors.New("chunkstore: encryption requested with a zero master key")
	// ErrCorruptIndex is returned when an index row is implausible, e.g. a stored
	// length larger than any chunk can be.
	ErrCorruptIndex = errors.New("chunkstore: corrupt index entry")
)

// maxStoredLen bounds a stored chunk's bytes: at most a max-size plaintext chunk
// plus compression framing and the AEAD tag. Reads reject any index length beyond
// it rather than allocating blindly.
const maxStoredLen = chunker.MaxSize + 1024

// SchemaVersion is the chunkindex database layout version. Open rejects a
// database written by a newer binary.
const SchemaVersion = 2

const schema = `
CREATE TABLE IF NOT EXISTS meta (
	key   TEXT PRIMARY KEY,
	value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS chunks (
	chunk_id         BLOB PRIMARY KEY,
	backing          INTEGER NOT NULL,
	blob_id          INTEGER,
	object_id        INTEGER,
	backing_offset   INTEGER,
	length           INTEGER NOT NULL,
	codec            INTEGER NOT NULL DEFAULT 0,
	encrypted        INTEGER NOT NULL DEFAULT 0,
	plaintext_length INTEGER NOT NULL,
	last_seen_ms     INTEGER NOT NULL DEFAULT 0
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS blobs (
	blob_id INTEGER PRIMARY KEY,
	path    TEXT    NOT NULL,
	size    INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS objects (
	object_id INTEGER PRIMARY KEY,
	path      TEXT NOT NULL
);`

// FolderContext carries the per-operation encryption decision for a folder.
type FolderContext struct {
	Encrypted bool
	MasterKey [crypto.MasterKeyLen]byte
}

type openBlob struct {
	id   int64
	f    *os.File
	size int64
	path string
}

// Store is the content-addressed store.
type Store struct {
	db        *storage.DB
	dir       string
	objectDir string
	log       *slog.Logger

	mu           sync.Mutex
	cur          *openBlob
	nextID       int64
	nextObjectID int64
	target       int64

	physicalOnce sync.Once // guards the one-time "reflink unsupported" warning
}

// Options configures Open. BlobTargetSize defaults to the package constant.
type Options struct {
	DB      *storage.DB
	BlobDir string
	// ObjectDir holds whole-file clone objects; it defaults to an "objects" sibling
	// of BlobDir. For ~1x disk it must be on the same filesystem as the working
	// files, else clonefile falls back to a physical copy.
	ObjectDir      string
	Logger         *slog.Logger
	BlobTargetSize int64
}

// Open prepares the store, ensuring the schema and recovering any pack blob that
// was open at the last shutdown.
func Open(opts Options) (*Store, error) {
	if opts.DB == nil {
		return nil, errors.New("chunkstore: nil database")
	}
	if opts.BlobDir == "" {
		return nil, errors.New("chunkstore: empty blob dir")
	}
	objectDir := opts.ObjectDir
	if objectDir == "" {
		objectDir = filepath.Join(filepath.Dir(opts.BlobDir), "objects")
	}
	for _, d := range [...]string{opts.BlobDir, objectDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return nil, fmt.Errorf("chunkstore: store dir: %w", err)
		}
		// MkdirAll leaves an existing directory's mode untouched, so tighten it.
		if err := os.Chmod(d, 0o700); err != nil {
			return nil, fmt.Errorf("chunkstore: store dir perms: %w", err)
		}
	}
	log := opts.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	target := opts.BlobTargetSize
	if target <= 0 {
		target = BlobTargetSize
	}
	s := &Store{db: opts.DB, dir: opts.BlobDir, objectDir: objectDir, log: log, nextID: 1, nextObjectID: 1, target: target}
	if err := s.init(context.Background()); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) init(ctx context.Context) error {
	if err := s.db.WithTx(ctx, func(tx *storage.Tx) error {
		if _, err := tx.Exec(ctx, schema); err != nil {
			return fmt.Errorf("chunkstore: schema: %w", err)
		}
		var v string
		err := tx.QueryRow(ctx, `SELECT value FROM meta WHERE key = 'schema_version'`).Scan(&v)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			if _, err := tx.Exec(ctx, `INSERT INTO meta (key, value) VALUES ('schema_version', ?)`, SchemaVersion); err != nil {
				return fmt.Errorf("chunkstore: set version: %w", err)
			}
			return nil
		case err != nil:
			return fmt.Errorf("chunkstore: read version: %w", err)
		}
		var stored int
		if _, err := fmt.Sscanf(v, "%d", &stored); err != nil {
			return fmt.Errorf("chunkstore: unreadable schema_version %q: %w", v, err)
		}
		if stored > SchemaVersion {
			return fmt.Errorf("%w: found %d, support %d", ErrSchemaTooNew, stored, SchemaVersion)
		}
		return nil
	}); err != nil {
		return err
	}
	if err := s.recoverBlob(ctx); err != nil {
		return err
	}
	return s.recoverObjects(ctx)
}

// recoverBlob reopens the most recent blob and truncates any bytes written past
// the committed size, discarding an orphan tail left by a crash.
func (s *Store) recoverBlob(ctx context.Context) error {
	var id, size int64
	var path string
	err := s.db.QueryRow(ctx, `SELECT blob_id, path, size FROM blobs ORDER BY blob_id DESC LIMIT 1`).
		Scan(&id, &path, &size)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("chunkstore: recover blob: %w", err)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("chunkstore: reopen blob: %w", err)
	}
	if err := f.Truncate(size); err != nil {
		_ = f.Close()
		return fmt.Errorf("chunkstore: truncate blob: %w", err)
	}
	s.cur = &openBlob{id: id, f: f, size: size, path: path}
	s.nextID = id + 1
	return nil
}

// recoverObjects sets the next object id past the highest recorded one and unlinks
// orphan clone files (a file on disk with no committed row).
func (s *Store) recoverObjects(ctx context.Context) error {
	var maxID int64
	if err := s.db.QueryRow(ctx, `SELECT COALESCE(MAX(object_id), 0) FROM objects`).Scan(&maxID); err != nil {
		return fmt.Errorf("chunkstore: recover objects: %w", err)
	}
	s.nextObjectID = maxID + 1

	known := make(map[string]struct{})
	rows, err := s.db.Query(ctx, `SELECT path FROM objects`)
	if err != nil {
		return fmt.Errorf("chunkstore: list objects: %w", err)
	}
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			_ = rows.Close()
			return fmt.Errorf("chunkstore: scan object: %w", err)
		}
		known[p] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("chunkstore: iter objects: %w", err)
	}
	_ = rows.Close()

	entries, err := os.ReadDir(s.objectDir)
	if err != nil {
		return fmt.Errorf("chunkstore: read object dir: %w", err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".clone") {
			continue
		}
		p := filepath.Join(s.objectDir, e.Name())
		if _, ok := known[p]; ok {
			continue
		}
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("chunkstore: remove orphan object: %w", err)
		}
	}
	return nil
}

// Close closes the open pack blob. The database is owned by the caller.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cur != nil {
		err := s.cur.f.Close()
		s.cur = nil
		return err
	}
	return nil
}

// Has reports whether a chunk with the given identity is stored.
func (s *Store) Has(ctx context.Context, id hasher.ChunkID) (bool, error) {
	return s.has(ctx, id)
}

// HasBulk reports which of ids are already stored, batching into one query per group
// instead of a round-trip per id. The returned set holds exactly the present ids.
func (s *Store) HasBulk(ctx context.Context, ids []hasher.ChunkID) (map[hasher.ChunkID]struct{}, error) {
	present := make(map[hasher.ChunkID]struct{}, len(ids))
	const batch = 900 // under SQLite's default 999 bound-parameter limit
	for start := 0; start < len(ids); start += batch {
		group := ids[start:min(start+batch, len(ids))]
		args := make([]any, len(group))
		for i, id := range group {
			args[i] = id.Bytes()
		}
		q := `SELECT chunk_id FROM chunks WHERE chunk_id IN (` + strings.Repeat("?,", len(group)-1) + `?)`
		rows, err := s.db.Query(ctx, q, args...)
		if err != nil {
			return nil, fmt.Errorf("chunkstore: has bulk: %w", err)
		}
		err = func() error {
			defer func() { _ = rows.Close() }()
			for rows.Next() {
				var b []byte
				if err := rows.Scan(&b); err != nil {
					return err
				}
				id, err := hasher.FromBytes(b)
				if err != nil {
					return err
				}
				present[id] = struct{}{}
			}
			return rows.Err()
		}()
		if err != nil {
			return nil, fmt.Errorf("chunkstore: has bulk: %w", err)
		}
	}
	return present, nil
}

// Put stores a plaintext chunk physically and returns its identity. Storing a
// chunk that already exists refreshes its last-seen time and writes no bytes.
func (s *Store) Put(ctx context.Context, fc FolderContext, plaintext []byte) (hasher.ChunkID, error) {
	id := hasher.Sum(plaintext)
	if fc.Encrypted && fc.MasterKey == ([crypto.MasterKeyLen]byte{}) {
		return id, ErrZeroKey
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// An existing chunk, in any backing, is already servable: refresh last-seen and
	// write nothing.
	switch exists, err := s.has(ctx, id); {
	case err != nil:
		return id, err
	case exists:
		return id, s.touchLastSeen(ctx, id)
	}

	codec, packed := compression.Compress(plaintext)
	stored := packed
	if fc.Encrypted {
		sealed, err := crypto.Seal(fc.MasterKey, id, packed)
		if err != nil {
			return id, err
		}
		stored = sealed
	}

	blob, offset, err := s.append(ctx, stored)
	if err != nil {
		return id, err
	}

	err = s.db.WithTx(ctx, func(tx *storage.Tx) error {
		if _, err := tx.Exec(ctx,
			`INSERT INTO chunks (chunk_id, backing, blob_id, backing_offset, length, codec, encrypted, plaintext_length, last_seen_ms)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			id.Bytes(), int(BackingPhysical), blob.id, offset, len(stored), int(codec), fc.Encrypted, len(plaintext), time.Now().UnixMilli()); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE blobs SET size = ? WHERE blob_id = ?`, offset+int64(len(stored)), blob.id)
		return err
	})
	if err != nil {
		return id, fmt.Errorf("chunkstore: index chunk: %w", err)
	}
	blob.size = offset + int64(len(stored))
	return id, nil
}

// append writes stored to the current blob (rolling over first if needed) and
// fsyncs it before the caller commits the index, so committed index rows never
// point past durable bytes.
func (s *Store) append(ctx context.Context, stored []byte) (*openBlob, int64, error) {
	if s.cur == nil || (s.cur.size > 0 && s.cur.size+int64(len(stored)) > s.target) {
		if err := s.roll(ctx); err != nil {
			return nil, 0, err
		}
	}
	offset := s.cur.size
	if _, err := s.cur.f.WriteAt(stored, offset); err != nil {
		return nil, 0, fmt.Errorf("chunkstore: write blob: %w", err)
	}
	if err := s.cur.f.Sync(); err != nil {
		return nil, 0, fmt.Errorf("chunkstore: sync blob: %w", err)
	}
	return s.cur, offset, nil
}

func (s *Store) roll(ctx context.Context) error {
	if s.cur != nil {
		if err := s.cur.f.Close(); err != nil {
			return fmt.Errorf("chunkstore: close blob: %w", err)
		}
		s.cur = nil
	}
	id := s.nextID
	s.nextID++ // advance regardless of outcome so a failed roll never reuses an id
	path := filepath.Join(s.dir, fmt.Sprintf("%020d.blob", id))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("chunkstore: create blob: %w", err)
	}
	if err := syncDir(s.dir); err != nil {
		_ = f.Close()
		return err
	}
	if err := s.db.WithTx(ctx, func(tx *storage.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO blobs (blob_id, path, size) VALUES (?, ?, 0)`, id, path)
		return err
	}); err != nil {
		_ = f.Close()
		return fmt.Errorf("chunkstore: register blob: %w", err)
	}
	s.cur = &openBlob{id: id, f: f, size: 0, path: path}
	return nil
}

func (s *Store) has(ctx context.Context, id hasher.ChunkID) (bool, error) {
	var one int
	err := s.db.QueryRow(ctx, `SELECT 1 FROM chunks WHERE chunk_id = ?`, id.Bytes()).Scan(&one)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("chunkstore: has: %w", err)
	}
	return true, nil
}

// touchLastSeen marks a chunk as referenced now, so a concurrent or just-finished
// reference keeps it above the sweep's grace cutoff even before the referencing
// manifest is committed.
func (s *Store) touchLastSeen(ctx context.Context, id hasher.ChunkID) error {
	return s.db.WithTx(ctx, func(tx *storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE chunks SET last_seen_ms = ? WHERE chunk_id = ?`, time.Now().UnixMilli(), id.Bytes())
		return err
	})
}

// cloneRef locates one chunk's plaintext within a clone object.
type cloneRef struct {
	id     hasher.ChunkID
	offset int64
	length int
}

// IngestClone clones the file at srcPath, chunks the clone, and points every chunk
// at its (object, offset, length) range, re-pointing chunks that already exist so
// current data consolidates onto the clone that shares extents with the working
// file. It returns the ordered chunk references for the caller's manifest. Chunking
// the immutable clone keeps the recorded ranges consistent with their identities.
func (s *Store) IngestClone(ctx context.Context, srcPath string) ([]manifest.ChunkRef, error) {
	objID, objPath := s.reserveObject()
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(objPath)
		}
	}()

	cloned, err := cloneOrCopy(srcPath, objPath)
	if err != nil {
		return nil, fmt.Errorf("chunkstore: clone %q: %w", srcPath, err)
	}
	if !cloned {
		s.physicalOnce.Do(func() {
			s.log.Warn("chunkstore: filesystem does not support reflink; using a physical copy, so current data costs about 2x disk")
		})
	}
	// Durable before indexing, so a committed row never points at bytes not on disk.
	if err := fsyncFile(objPath); err != nil {
		return nil, err
	}
	if err := syncDir(s.objectDir); err != nil {
		return nil, err
	}

	refs, crefs, err := chunkObject(objPath)
	if err != nil {
		return nil, err
	}
	if len(crefs) == 0 {
		// An empty file has no chunks and needs no object.
		return refs, nil
	}
	if err := s.indexCloneRows(ctx, objID, objPath, crefs); err != nil {
		return nil, err
	}
	committed = true
	return refs, nil
}

func (s *Store) reserveObject() (int64, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.nextObjectID
	s.nextObjectID++
	return id, filepath.Join(s.objectDir, fmt.Sprintf("%020d.clone", id))
}

// chunkObject runs the chunker over an immutable clone, returning both the
// manifest references (identity, plaintext length) and the clone locations
// (identity, byte offset, length) for the index.
func chunkObject(path string) ([]manifest.ChunkRef, []cloneRef, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("chunkstore: open clone: %w", err)
	}
	defer func() { _ = f.Close() }()

	c := chunker.New(chunker.Options{Reader: f})
	var refs []manifest.ChunkRef
	var crefs []cloneRef
	for {
		ch, data, err := c.NextChunk()
		if errors.Is(err, io.EOF) {
			return refs, crefs, nil
		}
		if err != nil {
			return nil, nil, fmt.Errorf("chunkstore: chunk clone: %w", err)
		}
		id := hasher.Sum(data)
		refs = append(refs, manifest.ChunkRef{ID: id, Length: int64(ch.Length)})
		crefs = append(crefs, cloneRef{id: id, offset: ch.Offset, length: ch.Length})
	}
}

// indexCloneRows commits the object row and points each chunk at its range in one
// transaction, re-pointing existing chunks off any prior backing.
func (s *Store) indexCloneRows(ctx context.Context, objID int64, objPath string, refs []cloneRef) error {
	now := time.Now().UnixMilli()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.db.WithTx(ctx, func(tx *storage.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO objects (object_id, path) VALUES (?, ?)`, objID, objPath); err != nil {
			return err
		}
		for _, r := range refs {
			if _, err := tx.Exec(ctx,
				`INSERT INTO chunks (chunk_id, backing, object_id, backing_offset, length, codec, encrypted, plaintext_length, last_seen_ms)
				 VALUES (?, ?, ?, ?, ?, 0, 0, ?, ?)
				 ON CONFLICT(chunk_id) DO UPDATE SET
				   backing = excluded.backing, blob_id = NULL, object_id = excluded.object_id,
				   backing_offset = excluded.backing_offset, length = excluded.length,
				   codec = 0, encrypted = 0, plaintext_length = excluded.plaintext_length,
				   last_seen_ms = excluded.last_seen_ms`,
				r.id.Bytes(), int(BackingClone), objID, r.offset, r.length, r.length, now); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("chunkstore: index clone: %w", err)
	}
	return nil
}

func fsyncFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("chunkstore: open for fsync: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("chunkstore: fsync %q: %w", path, err)
	}
	return f.Close()
}

// Get returns the verified plaintext for a chunk, regardless of backing.
func (s *Store) Get(ctx context.Context, fc FolderContext, id hasher.ChunkID) ([]byte, error) {
	var (
		backing  int
		blobID   sql.NullInt64
		objectID sql.NullInt64
		offset   sql.NullInt64
		length   int
		codec    int
		enc      bool
		plen     int
	)
	err := s.db.QueryRow(ctx,
		`SELECT backing, blob_id, object_id, backing_offset, length, codec, encrypted, plaintext_length FROM chunks WHERE chunk_id = ?`,
		id.Bytes()).Scan(&backing, &blobID, &objectID, &offset, &length, &codec, &enc, &plen)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, ErrChunkNotFound
	case err != nil:
		return nil, fmt.Errorf("chunkstore: lookup: %w", err)
	}
	if !offset.Valid {
		return nil, fmt.Errorf("%w: chunk missing offset", ErrCorruptIndex)
	}

	var path string
	switch Backing(backing) {
	case BackingClone:
		if !objectID.Valid {
			return nil, fmt.Errorf("%w: clone chunk missing object", ErrCorruptIndex)
		}
		if err := s.db.QueryRow(ctx, `SELECT path FROM objects WHERE object_id = ?`, objectID.Int64).Scan(&path); err != nil {
			return nil, fmt.Errorf("chunkstore: object path: %w", err)
		}
	default:
		if !blobID.Valid {
			return nil, fmt.Errorf("%w: physical chunk missing blob", ErrCorruptIndex)
		}
		if err := s.db.QueryRow(ctx, `SELECT path FROM blobs WHERE blob_id = ?`, blobID.Int64).Scan(&path); err != nil {
			return nil, fmt.Errorf("chunkstore: blob path: %w", err)
		}
	}
	stored, err := readAt(path, offset.Int64, length)
	if err != nil {
		return nil, fmt.Errorf("chunkstore: read backing: %w", err)
	}
	return verify(fc, id, compression.Codec(codec), enc, stored)
}

func verify(fc FolderContext, id hasher.ChunkID, codec compression.Codec, encrypted bool, stored []byte) ([]byte, error) {
	data := stored
	if encrypted {
		if !fc.Encrypted {
			return nil, ErrNoKey
		}
		opened, err := crypto.Open(fc.MasterKey, id, data)
		if err != nil {
			return nil, err
		}
		data = opened
	}
	plain, err := compression.Decompress(codec, data, compression.MaxDecodedSize)
	if err != nil {
		return nil, err
	}
	if hasher.Sum(plain) != id {
		return nil, ErrHashMismatch
	}
	return plain, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("chunkstore: open dir: %w", err)
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return fmt.Errorf("chunkstore: sync dir: %w", err)
	}
	return d.Close()
}

func readAt(path string, offset int64, length int) ([]byte, error) {
	if length < 0 || length > maxStoredLen {
		return nil, fmt.Errorf("%w: length %d", ErrCorruptIndex, length)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	buf := make([]byte, length)
	if _, err := f.ReadAt(buf, offset); err != nil {
		return nil, err
	}
	return buf, nil
}

// ImportStream chunks r, storing new chunks physically, and returns the ordered
// chunk identities. Identical content is stored once.
func (s *Store) ImportStream(ctx context.Context, fc FolderContext, r io.Reader) ([]hasher.ChunkID, error) {
	c := chunker.New(chunker.Options{Reader: r})
	var ids []hasher.ChunkID
	for {
		_, data, err := c.NextChunk()
		if errors.Is(err, io.EOF) {
			return ids, nil
		}
		if err != nil {
			return nil, fmt.Errorf("chunkstore: chunk stream: %w", err)
		}
		id, err := s.Put(ctx, fc, data)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
}

// ImportFile imports a file's contents, storing new chunks physically.
func (s *Store) ImportFile(ctx context.Context, fc FolderContext, path string) ([]hasher.ChunkID, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("chunkstore: open import: %w", err)
	}
	defer func() { _ = f.Close() }()
	return s.ImportStream(ctx, fc, f)
}

// Reassemble writes the chunks named by ids to w in order, reproducing the
// original bytes exactly.
func (s *Store) Reassemble(ctx context.Context, fc FolderContext, ids []hasher.ChunkID, w io.Writer) error {
	for _, id := range ids {
		data, err := s.Get(ctx, fc, id)
		if err != nil {
			return err
		}
		if _, err := w.Write(data); err != nil {
			return fmt.Errorf("chunkstore: write output: %w", err)
		}
	}
	return nil
}

// ChunkStat is a chunk's identity and when it was last referenced, the inputs the
// garbage collector needs to decide collectability.
type ChunkStat struct {
	ID       hasher.ChunkID
	LastSeen int64
}

// IterChunks streams every stored chunk's identity and last-seen time. It holds a
// read cursor, so consume it fully (or stop early) before deleting.
func (s *Store) IterChunks(ctx context.Context) iter.Seq2[ChunkStat, error] {
	return func(yield func(ChunkStat, error) bool) {
		rows, err := s.db.Query(ctx, `SELECT chunk_id, last_seen_ms FROM chunks`)
		if err != nil {
			yield(ChunkStat{}, fmt.Errorf("chunkstore: iter chunks: %w", err))
			return
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var raw []byte
			var ls int64
			if err := rows.Scan(&raw, &ls); err != nil {
				yield(ChunkStat{}, fmt.Errorf("chunkstore: scan chunk: %w", err))
				return
			}
			id, err := hasher.FromBytes(raw)
			if err != nil {
				yield(ChunkStat{}, fmt.Errorf("chunkstore: chunk id: %w", err))
				return
			}
			if !yield(ChunkStat{ID: id, LastSeen: ls}, nil) {
				return
			}
		}
		if err := rows.Err(); err != nil {
			yield(ChunkStat{}, err)
		}
	}
}

// DeleteChunks removes the index entries for ids whose last_seen_ms is still
// below beforeMs, in one transaction serialized with writes, and returns how many
// chunks were actually deleted. The last_seen guard is re-checked at delete time,
// so a chunk a concurrent ingest touched after it was selected as a victim is
// spared. Blob bytes are reclaimed separately by ReclaimBlobs. Deleting a missing
// chunk is a no-op, so a re-run is safe.
func (s *Store) DeleteChunks(ctx context.Context, ids []hasher.ChunkID, beforeMs int64) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var deleted int
	if err := s.db.WithTx(ctx, func(tx *storage.Tx) error {
		deleted = 0
		for _, id := range ids {
			res, err := tx.Exec(ctx, `DELETE FROM chunks WHERE chunk_id = ? AND last_seen_ms < ?`, id.Bytes(), beforeMs)
			if err != nil {
				return fmt.Errorf("chunkstore: delete chunk: %w", err)
			}
			n, err := res.RowsAffected()
			if err != nil {
				return fmt.Errorf("chunkstore: delete chunk: %w", err)
			}
			if n == 0 {
				continue
			}
			deleted += int(n)
		}
		return nil
	}); err != nil {
		return 0, err
	}
	return deleted, nil
}

// ReclaimBlobs deletes blob files that no longer back any chunk, along with their
// rows, and returns how many it removed. The currently open blob is never
// reclaimed. The row is dropped before the file is unlinked, so an interrupted
// reclaim leaves at worst an orphan file (harmless, reclaimed on a later run)
// rather than a row pointing at a missing file.
func (s *Store) ReclaimBlobs(ctx context.Context) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	type deadBlob struct {
		id   int64
		path string
	}
	var dead []deadBlob
	openEmpty := false
	rows, err := s.db.Query(ctx,
		`SELECT blob_id, path FROM blobs WHERE blob_id NOT IN (SELECT blob_id FROM chunks WHERE blob_id IS NOT NULL)`)
	if err != nil {
		return 0, fmt.Errorf("chunkstore: find dead blobs: %w", err)
	}
	for rows.Next() {
		var b deadBlob
		if err := rows.Scan(&b.id, &b.path); err != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("chunkstore: scan blob: %w", err)
		}
		if s.cur != nil && b.id == s.cur.id {
			openEmpty = true
			continue
		}
		dead = append(dead, b)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, err
	}
	_ = rows.Close()

	// The open blob backs no chunk: reclaim its space in place rather than unlink a
	// file still in use. Commit size 0 before truncating so recoverBlob never
	// re-extends after a crash.
	if openEmpty && s.cur.size > 0 {
		if err := s.db.WithTx(ctx, func(tx *storage.Tx) error {
			_, err := tx.Exec(ctx, `UPDATE blobs SET size = 0 WHERE blob_id = ?`, s.cur.id)
			return err
		}); err != nil {
			return 0, fmt.Errorf("chunkstore: reset open blob size: %w", err)
		}
		if err := s.cur.f.Truncate(0); err != nil {
			return 0, fmt.Errorf("chunkstore: truncate open blob: %w", err)
		}
		if err := s.cur.f.Sync(); err != nil {
			return 0, fmt.Errorf("chunkstore: sync open blob: %w", err)
		}
		s.cur.size = 0
	}

	for _, b := range dead {
		if err := s.db.WithTx(ctx, func(tx *storage.Tx) error {
			_, err := tx.Exec(ctx, `DELETE FROM blobs WHERE blob_id = ?`, b.id)
			return err
		}); err != nil {
			return 0, fmt.Errorf("chunkstore: delete blob row: %w", err)
		}
		if err := os.Remove(b.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return 0, fmt.Errorf("chunkstore: remove blob: %w", err)
		}
	}
	return len(dead), nil
}

// ReclaimObjects deletes clone objects that no longer back any chunk, along with
// their rows, and returns how many it removed. A clone is a separate inode, so
// unlinking it never touches the working file's bytes.
func (s *Store) ReclaimObjects(ctx context.Context) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	type deadObject struct {
		id   int64
		path string
	}
	var dead []deadObject
	rows, err := s.db.Query(ctx,
		`SELECT object_id, path FROM objects WHERE object_id NOT IN (SELECT object_id FROM chunks WHERE object_id IS NOT NULL)`)
	if err != nil {
		return 0, fmt.Errorf("chunkstore: find dead objects: %w", err)
	}
	for rows.Next() {
		var o deadObject
		if err := rows.Scan(&o.id, &o.path); err != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("chunkstore: scan object: %w", err)
		}
		dead = append(dead, o)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, fmt.Errorf("chunkstore: iter dead objects: %w", err)
	}
	_ = rows.Close()

	for _, o := range dead {
		if err := s.db.WithTx(ctx, func(tx *storage.Tx) error {
			_, err := tx.Exec(ctx, `DELETE FROM objects WHERE object_id = ?`, o.id)
			return err
		}); err != nil {
			return 0, fmt.Errorf("chunkstore: delete object row: %w", err)
		}
		if err := os.Remove(o.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return 0, fmt.Errorf("chunkstore: remove object: %w", err)
		}
	}
	return len(dead), nil
}
