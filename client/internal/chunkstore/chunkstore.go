// Package chunkstore is the content-addressed store. It maps a chunk identity to
// its stored bytes, deduplicating on write, and reassembles the original bytes on
// read. Chunks are backed either physically (packed into append-only blob files)
// or virtually (a pointer into a real working file, so a node holding the data as
// a normal file does not duplicate it). Every read is verified against the
// requested identity before any bytes are returned.
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
	"github.com/GhentiLabs/Trove/client/internal/storage"
)

// BlobTargetSize is the size past which the current pack blob rolls over to a new
// one. It is a tunable default, not a hard limit.
const BlobTargetSize = 64 << 20

// Backing identifies where a chunk's bytes live.
type Backing uint8

const (
	BackingPhysical Backing = 0
	BackingVirtual  Backing = 1
)

var (
	// ErrChunkNotFound is returned when no chunk has the requested identity.
	ErrChunkNotFound = errors.New("chunkstore: chunk not found")
	// ErrHashMismatch is returned when stored bytes fail verification.
	ErrHashMismatch = errors.New("chunkstore: stored bytes failed hash verification")
	// ErrFileChanged is returned when a virtual chunk's backing file no longer
	// hashes to the chunk identity.
	ErrFileChanged = errors.New("chunkstore: backing file changed")
	// ErrNoKey is returned when reading an encrypted chunk without a folder key.
	ErrNoKey = errors.New("chunkstore: chunk is encrypted but no key was provided")
	// ErrSchemaTooNew is returned when the database was written by a newer binary.
	ErrSchemaTooNew = errors.New("chunkstore: database schema newer than this binary")
	// ErrZeroKey is returned when encryption is requested with a zero master key.
	ErrZeroKey = errors.New("chunkstore: encryption requested with a zero master key")
	// ErrBackingMismatch is returned when an operation's backing conflicts with
	// how the chunk is already stored (a chunk has one backing until promoted).
	ErrBackingMismatch = errors.New("chunkstore: chunk already stored with a different backing")
	// ErrCorruptIndex is returned when an index row is implausible, e.g. a stored
	// length larger than any chunk can be.
	ErrCorruptIndex = errors.New("chunkstore: corrupt index entry")
)

// maxStoredLen bounds a physical chunk's stored bytes: at most a max-size
// plaintext chunk plus compression framing and the AEAD tag. Reads reject any
// index length beyond it rather than allocating blindly.
const maxStoredLen = chunker.MaxSize + 1024

// SchemaVersion is the current chunkindex database layout. Open rejects a
// database written by a newer binary and migrates older ones forward.
//
// v1: chunks carried an increment-only refcount.
// v2: refcount dropped for last_seen_ms; reclamation is reachability mark-and-sweep guarded by a grace age.
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
	blob_offset      INTEGER,
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
CREATE TABLE IF NOT EXISTS chunk_locations (
	chunk_id    BLOB    NOT NULL,
	file_path   TEXT    NOT NULL,
	file_offset INTEGER NOT NULL,
	length      INTEGER NOT NULL,
	PRIMARY KEY (chunk_id, file_path, file_offset)
) WITHOUT ROWID;`

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
	db  *storage.DB
	dir string
	log *slog.Logger

	mu     sync.Mutex
	cur    *openBlob
	nextID int64
	target int64
}

// Options configures Open. BlobTargetSize defaults to the package constant.
type Options struct {
	DB             *storage.DB
	BlobDir        string
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
	if err := os.MkdirAll(opts.BlobDir, 0o700); err != nil {
		return nil, fmt.Errorf("chunkstore: blob dir: %w", err)
	}
	// MkdirAll leaves an existing directory's mode untouched, so tighten it.
	if err := os.Chmod(opts.BlobDir, 0o700); err != nil {
		return nil, fmt.Errorf("chunkstore: blob dir perms: %w", err)
	}
	log := opts.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	target := opts.BlobTargetSize
	if target <= 0 {
		target = BlobTargetSize
	}
	s := &Store{db: opts.DB, dir: opts.BlobDir, log: log, nextID: 1, target: target}
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
		switch {
		case stored > SchemaVersion:
			return fmt.Errorf("%w: found %d, support %d", ErrSchemaTooNew, stored, SchemaVersion)
		case stored < SchemaVersion:
			return migrate(ctx, tx, stored)
		}
		return nil
	}); err != nil {
		return err
	}
	return s.recoverBlob(ctx)
}

// migrate upgrades an older chunkindex to SchemaVersion in place.
func migrate(ctx context.Context, tx *storage.Tx, from int) error {
	if from == 1 {
		// v1→v2: replace the increment-only refcount with last_seen_ms. Existing
		// chunks are backfilled to now so the next sweep's grace age protects them.
		stmts := []string{
			`ALTER TABLE chunks ADD COLUMN last_seen_ms INTEGER NOT NULL DEFAULT 0`,
			`ALTER TABLE chunks DROP COLUMN refcount`,
		}
		for _, q := range stmts {
			if _, err := tx.Exec(ctx, q); err != nil {
				return fmt.Errorf("chunkstore: migrate v2: %w", err)
			}
		}
		if _, err := tx.Exec(ctx, `UPDATE chunks SET last_seen_ms = ?`, time.Now().UnixMilli()); err != nil {
			return fmt.Errorf("chunkstore: migrate v2 backfill: %w", err)
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE meta SET value = ? WHERE key = 'schema_version'`, SchemaVersion); err != nil {
		return fmt.Errorf("chunkstore: set version: %w", err)
	}
	return nil
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

	switch backing, exists, err := s.backingOf(ctx, id); {
	case err != nil:
		return id, err
	case exists && backing != BackingPhysical:
		return id, ErrBackingMismatch
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
			`INSERT INTO chunks (chunk_id, backing, blob_id, blob_offset, length, codec, encrypted, plaintext_length, last_seen_ms)
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

func (s *Store) backingOf(ctx context.Context, id hasher.ChunkID) (Backing, bool, error) {
	var b int
	err := s.db.QueryRow(ctx, `SELECT backing FROM chunks WHERE chunk_id = ?`, id.Bytes()).Scan(&b)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, false, nil
	case err != nil:
		return 0, false, fmt.Errorf("chunkstore: backing: %w", err)
	}
	return Backing(b), true, nil
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

// PutVirtual records a chunk as a pointer into filePath without copying bytes.
func (s *Store) PutVirtual(ctx context.Context, id hasher.ChunkID, filePath string, offset int64, length, plaintextLen int) error {
	if !filepath.IsAbs(filePath) {
		return fmt.Errorf("chunkstore: PutVirtual requires an absolute path, got %q", filePath)
	}
	if offset < 0 || length <= 0 || length > maxStoredLen || plaintextLen <= 0 {
		return fmt.Errorf("chunkstore: invalid virtual chunk: offset=%d length=%d plaintext=%d", offset, length, plaintextLen)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch backing, exists, err := s.backingOf(ctx, id); {
	case err != nil:
		return err
	case exists && backing != BackingVirtual:
		return ErrBackingMismatch
	}
	return s.db.WithTx(ctx, func(tx *storage.Tx) error {
		_, err := tx.Exec(ctx,
			`INSERT INTO chunks (chunk_id, backing, length, plaintext_length, last_seen_ms)
			 VALUES (?, ?, ?, ?, ?)
			 ON CONFLICT(chunk_id) DO UPDATE SET last_seen_ms = excluded.last_seen_ms`,
			id.Bytes(), int(BackingVirtual), length, plaintextLen, time.Now().UnixMilli())
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx,
			`INSERT OR IGNORE INTO chunk_locations (chunk_id, file_path, file_offset, length) VALUES (?, ?, ?, ?)`,
			id.Bytes(), filePath, offset, length)
		return err
	})
}

// Get returns the verified plaintext for a chunk, regardless of backing.
func (s *Store) Get(ctx context.Context, fc FolderContext, id hasher.ChunkID) ([]byte, error) {
	var (
		backing int
		blobID  sql.NullInt64
		offset  sql.NullInt64
		length  int
		codec   int
		enc     bool
		plen    int
	)
	err := s.db.QueryRow(ctx,
		`SELECT backing, blob_id, blob_offset, length, codec, encrypted, plaintext_length FROM chunks WHERE chunk_id = ?`,
		id.Bytes()).Scan(&backing, &blobID, &offset, &length, &codec, &enc, &plen)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, ErrChunkNotFound
	case err != nil:
		return nil, fmt.Errorf("chunkstore: lookup: %w", err)
	}

	if Backing(backing) == BackingVirtual {
		return s.readVirtual(ctx, id)
	}

	if !blobID.Valid || !offset.Valid {
		return nil, fmt.Errorf("%w: physical chunk missing blob location", ErrCorruptIndex)
	}
	var path string
	if err := s.db.QueryRow(ctx, `SELECT path FROM blobs WHERE blob_id = ?`, blobID.Int64).Scan(&path); err != nil {
		return nil, fmt.Errorf("chunkstore: blob path: %w", err)
	}
	stored, err := readAt(path, offset.Int64, length)
	if err != nil {
		return nil, fmt.Errorf("chunkstore: read blob: %w", err)
	}
	return verify(fc, id, compression.Codec(codec), enc, stored)
}

func (s *Store) readVirtual(ctx context.Context, id hasher.ChunkID) ([]byte, error) {
	rows, err := s.db.Query(ctx, `SELECT file_path, file_offset, length FROM chunk_locations WHERE chunk_id = ?`, id.Bytes())
	if err != nil {
		return nil, fmt.Errorf("chunkstore: locations: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var located, readable bool
	for rows.Next() {
		var path string
		var offset int64
		var length int
		if err := rows.Scan(&path, &offset, &length); err != nil {
			return nil, fmt.Errorf("chunkstore: scan location: %w", err)
		}
		located = true
		data, err := readAt(path, offset, length)
		switch {
		case errors.Is(err, os.ErrNotExist):
			continue
		case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
			readable = true // file exists but is shorter than recorded: it changed
			continue
		case err != nil:
			return nil, fmt.Errorf("chunkstore: read virtual: %w", err)
		}
		readable = true
		if hasher.Sum(data) == id {
			return data, nil
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if !located || !readable {
		return nil, ErrChunkNotFound
	}
	return nil, ErrFileChanged
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

// MirrorFile chunks a file and records each chunk as a virtual location into it,
// duplicating no bytes, and returns the ordered chunk identities.
func (s *Store) MirrorFile(ctx context.Context, path string) ([]hasher.ChunkID, error) {
	if !filepath.IsAbs(path) {
		return nil, fmt.Errorf("chunkstore: MirrorFile requires an absolute path, got %q", path)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("chunkstore: open mirror: %w", err)
	}
	defer func() { _ = f.Close() }()

	c := chunker.New(chunker.Options{Reader: f})
	var ids []hasher.ChunkID
	for {
		ch, data, err := c.NextChunk()
		if errors.Is(err, io.EOF) {
			return ids, nil
		}
		if err != nil {
			return nil, fmt.Errorf("chunkstore: chunk mirror: %w", err)
		}
		id := hasher.Sum(data)
		if err := s.PutVirtual(ctx, id, path, ch.Offset, ch.Length, ch.Length); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
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

// DeleteChunks removes the index entries (and virtual locations) for ids whose
// last_seen_ms is still below beforeMs, in one transaction serialized with
// writes, and returns how many chunks were actually deleted. The last_seen guard
// is re-checked at delete time, so a chunk a concurrent ingest touched after it
// was selected as a victim is spared. Blob bytes are reclaimed separately by
// ReclaimBlobs. Deleting a missing chunk is a no-op, so a re-run is safe.
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
			if _, err := tx.Exec(ctx, `DELETE FROM chunk_locations WHERE chunk_id = ?`, id.Bytes()); err != nil {
				return fmt.Errorf("chunkstore: delete locations: %w", err)
			}
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
			continue
		}
		dead = append(dead, b)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, err
	}
	_ = rows.Close()

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
