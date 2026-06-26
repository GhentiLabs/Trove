package model

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"path/filepath"
	"strings"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/hasher"
	"github.com/GhentiLabs/Trove/client/internal/manifest"
	"github.com/GhentiLabs/Trove/client/internal/storage"
)

// Metadata is a path's change-detection and restore metadata. None of it enters
// the manifest's content identity.
type Metadata struct {
	Mtime time.Time
	Size  int64
	Inode uint64
}

// Record is a stored manifest: its content, its identity, the metadata, and the
// versioning fields the model maintains. Author and AuthoredAt name the node and
// wall-clock of the edit that minted this version; they propagate with the
// manifest and break concurrent ties, and never enter the content identity.
type Record struct {
	Manifest   manifest.Manifest
	ID         manifest.ID
	Metadata   Metadata
	Version    manifest.VersionVector
	Seq        int64
	Author     string
	AuthoredAt time.Time
	Deleted    bool
	DeletedAt  time.Time
}

// StatSig is a path's stored change-detection signature, loaded without reading or
// verifying its chunks — the scanner's fast path for deciding whether a file
// changed.
type StatSig struct {
	Kind    manifest.Kind
	Mode    uint32
	Mtime   time.Time
	Size    int64
	Inode   uint64
	Deleted bool
}

// Stat returns the change-detection signature for path. ok is false if the path
// is unknown.
func (s *Store) Stat(ctx context.Context, path string) (StatSig, bool, error) {
	rec, ok, err := loadRow(ctx, s.db, manifest.NormalizePath(path))
	if err != nil || !ok {
		return StatSig{}, ok, err
	}
	return StatSig{
		Kind:    rec.Manifest.Kind,
		Mode:    rec.Manifest.Mode,
		Mtime:   rec.Metadata.Mtime,
		Size:    rec.Metadata.Size,
		Inode:   rec.Metadata.Inode,
		Deleted: rec.Deleted,
	}, true, nil
}

// PutManifest records the current manifest for m.Path. If the path's stored
// identity is unchanged, only the metadata is refreshed: no new version is minted
// and no sequence number is consumed. Otherwise this node's version counter is
// bumped, a new sequence number is assigned, and the chunk references are stored.
func (s *Store) PutManifest(ctx context.Context, m manifest.Manifest, md Metadata) (manifest.ID, error) {
	m.Path = manifest.NormalizePath(m.Path)
	m.SymlinkTarget = manifest.NormalizePath(m.SymlinkTarget)
	if err := ValidateManifest(m); err != nil {
		return manifest.ID{}, err
	}
	id := m.ID()

	err := s.db.WithTx(ctx, func(tx *storage.Tx) error {
		prior, ok, err := loadRow(ctx, tx, m.Path)
		if err != nil {
			return err
		}
		if ok && prior.ID == id && !prior.Deleted {
			return refreshStat(ctx, tx, m, md)
		}

		vv := manifest.VersionVector{}
		if ok {
			vv = prior.Version.Clone()
		}
		vv.Bump(s.node)
		seq, err := allocate(ctx, tx, counterManifestSeq)
		if err != nil {
			return err
		}
		if err := writeRow(ctx, tx, m, md, id, vv, seq, s.node, time.Now(), false, time.Time{}); err != nil {
			return err
		}
		return writeChunks(ctx, tx, id, m.Chunks)
	})
	if err != nil {
		return manifest.ID{}, err
	}
	return id, nil
}

// DeleteManifest tombstones path: the manifest is marked deleted with an expiry,
// this node's version counter is bumped, and a new sequence number is assigned.
// The content identity is retained so the deletion can be distinguished from a
// never-present path and the prior content can still be restored from snapshots.
// Deleting an already-tombstoned path is a no-op that returns its identity;
// deleting a never-present path returns ErrManifestNotFound.
func (s *Store) DeleteManifest(ctx context.Context, path string) (manifest.ID, error) {
	path = manifest.NormalizePath(path)
	var id manifest.ID
	err := s.db.WithTx(ctx, func(tx *storage.Tx) error {
		prior, ok, err := loadRow(ctx, tx, path)
		if err != nil {
			return err
		}
		if !ok {
			return ErrManifestNotFound
		}
		if prior.Deleted {
			id = prior.ID
			return nil
		}
		id = prior.ID
		vv := prior.Version.Clone()
		vv.Bump(s.node)
		seq, err := allocate(ctx, tx, counterManifestSeq)
		if err != nil {
			return err
		}
		now := time.Now()
		m := prior.Manifest
		return writeRow(ctx, tx, m, prior.Metadata, prior.ID, vv, seq, s.node, now, true, now)
	})
	if err != nil {
		return manifest.ID{}, err
	}
	return id, nil
}

// ListManifests returns every current manifest, live and tombstoned, ordered by
// path.
func (s *Store) ListManifests(ctx context.Context) ([]Record, error) {
	return s.queryRecords(ctx, `SELECT path FROM manifests ORDER BY path`)
}

// ManifestsSince returns the manifests whose sequence is greater than seq,
// ordered by sequence: the cursor that drives incremental exchange.
func (s *Store) ManifestsSince(ctx context.Context, seq int64) ([]Record, error) {
	return s.queryRecords(ctx, `SELECT path FROM manifests WHERE seq > ? ORDER BY seq`, seq)
}

// LivePaths streams the folder-relative paths of all non-deleted manifests in
// path order. It holds one read cursor, so consume it fully (or stop early)
// before issuing model writes. It lets the rescan find deletions without loading
// every path into memory.
func (s *Store) LivePaths(ctx context.Context) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		rows, err := s.db.Query(ctx, `SELECT path FROM manifests WHERE deleted = 0 ORDER BY path`)
		if err != nil {
			yield("", fmt.Errorf("model: live paths: %w", err))
			return
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var p string
			if err := rows.Scan(&p); err != nil {
				yield("", fmt.Errorf("model: scan path: %w", err))
				return
			}
			if !yield(p, nil) {
				return
			}
		}
		if err := rows.Err(); err != nil {
			yield("", err)
		}
	}
}

// queryRecords runs path-listing query and hydrates each record inside one
// transaction, so a concurrent writer cannot delete a path between enumeration
// and hydration and make a listing fail spuriously.
func (s *Store) queryRecords(ctx context.Context, query string, args ...any) ([]Record, error) {
	var out []Record
	err := s.db.WithTx(ctx, func(tx *storage.Tx) error {
		paths, err := scanPaths(ctx, tx, query, args...)
		if err != nil {
			return err
		}
		out = make([]Record, 0, len(paths))
		for _, p := range paths {
			rec, err := getRecord(ctx, tx, p)
			if err != nil {
				return err
			}
			out = append(out, rec)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func scanPaths(ctx context.Context, q querier, query string, args ...any) ([]string, error) {
	rows, err := q.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("model: list manifests: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var paths []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, fmt.Errorf("model: scan path: %w", err)
		}
		paths = append(paths, p)
	}
	return paths, rows.Err()
}

// GetManifest returns the current record for path, verifying that the stored
// content still hashes to the recorded identity.
func (s *Store) GetManifest(ctx context.Context, path string) (Record, error) {
	return getRecord(ctx, s.db, manifest.NormalizePath(path))
}

// ManifestChunks returns the ordered chunk references of a manifest by its id,
// which need not be the current manifest for any path — this is how an old
// version is restored from a retained snapshot.
func (s *Store) ManifestChunks(ctx context.Context, id manifest.ID) ([]manifest.ChunkRef, error) {
	return loadChunks(ctx, s.db, id)
}

// getRecord loads the full record for an already-normalized path and verifies
// that its stored content still hashes to the recorded identity.
func getRecord(ctx context.Context, q querier, path string) (Record, error) {
	rec, ok, err := loadRow(ctx, q, path)
	if err != nil {
		return Record{}, err
	}
	if !ok {
		return Record{}, ErrManifestNotFound
	}
	chunks, err := loadChunks(ctx, q, rec.ID)
	if err != nil {
		return Record{}, err
	}
	rec.Manifest.Chunks = chunks
	if rec.Manifest.ID() != rec.ID {
		return Record{}, fmt.Errorf("%w: path %q", ErrCorruptModel, path)
	}
	return rec, nil
}

// ValidateManifest rejects a manifest whose path or symlink target would escape the
// folder root, or whose kind/content is inconsistent. The replica validates with it
// before touching the filesystem so a hostile owner cannot plant escaping paths.
func ValidateManifest(m manifest.Manifest) error {
	if m.Path == "" {
		return fmt.Errorf("%w: empty path", ErrInvalidManifest)
	}
	if strings.HasPrefix(m.Path, "/") {
		return fmt.Errorf("%w: absolute path %q", ErrInvalidManifest, m.Path)
	}
	for seg := range strings.SplitSeq(m.Path, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return fmt.Errorf("%w: bad path segment in %q", ErrInvalidManifest, m.Path)
		}
	}
	switch m.Kind {
	case manifest.KindSymlink:
		if m.SymlinkTarget == "" {
			return fmt.Errorf("%w: symlink without target", ErrInvalidManifest)
		}
		if len(m.Chunks) != 0 {
			return fmt.Errorf("%w: symlink with chunks", ErrInvalidManifest)
		}
		if filepath.IsAbs(m.SymlinkTarget) {
			return fmt.Errorf("%w: symlink target escapes root (absolute) %q", ErrInvalidManifest, m.SymlinkTarget)
		}
		if c := filepath.Clean(m.SymlinkTarget); c == ".." || strings.HasPrefix(c, ".."+string(filepath.Separator)) {
			return fmt.Errorf("%w: symlink target escapes root %q", ErrInvalidManifest, m.SymlinkTarget)
		}
	case manifest.KindDir:
		if len(m.Chunks) != 0 || m.SymlinkTarget != "" {
			return fmt.Errorf("%w: directory with chunks or target", ErrInvalidManifest)
		}
	case manifest.KindRegular:
		if m.SymlinkTarget != "" {
			return fmt.Errorf("%w: regular file with symlink target", ErrInvalidManifest)
		}
	default:
		return fmt.Errorf("%w: unknown kind %d", ErrInvalidManifest, m.Kind)
	}
	for _, c := range m.Chunks {
		if c.Length <= 0 {
			return fmt.Errorf("%w: chunk length %d", ErrInvalidManifest, c.Length)
		}
	}
	return nil
}

func loadRow(ctx context.Context, q querier, path string) (Record, bool, error) {
	var (
		rec       Record
		idRaw     []byte
		vvRaw     []byte
		kind      int
		mode      int64
		target    string
		mtimeNs   int64
		size      int64
		inode     int64
		authoredM int64
		deleted   bool
		deletedM  sql.NullInt64
	)
	err := q.QueryRow(ctx,
		`SELECT manifest_id, kind, raw_mode, symlink_tgt, mtime_ns, size, inode, version_vec, seq, author, authored_ms, deleted, deleted_ms
		 FROM manifests WHERE path = ?`, path).
		Scan(&idRaw, &kind, &mode, &target, &mtimeNs, &size, &inode, &vvRaw, &rec.Seq, &rec.Author, &authoredM, &deleted, &deletedM)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return Record{}, false, nil
	case err != nil:
		return Record{}, false, fmt.Errorf("model: load manifest: %w", err)
	}
	id, err := manifest.IDFromBytes(idRaw)
	if err != nil {
		return Record{}, false, fmt.Errorf("model: load manifest: %w", err)
	}
	vv, err := manifest.ParseVector(vvRaw)
	if err != nil {
		return Record{}, false, fmt.Errorf("model: load manifest: %w", err)
	}
	rec.ID = id
	rec.Version = vv
	rec.AuthoredAt = time.UnixMilli(authoredM)
	rec.Deleted = deleted
	if deletedM.Valid {
		rec.DeletedAt = time.UnixMilli(deletedM.Int64)
	}
	rec.Metadata = Metadata{Mtime: time.Unix(0, mtimeNs), Size: size, Inode: uint64(inode)}
	rec.Manifest = manifest.Manifest{
		Kind:          manifest.Kind(kind),
		Path:          path,
		Mode:          uint32(mode),
		SymlinkTarget: target,
	}
	return rec, true, nil
}

func loadChunks(ctx context.Context, q querier, id manifest.ID) ([]manifest.ChunkRef, error) {
	rows, err := q.Query(ctx, `SELECT chunk_id, length FROM manifest_chunks WHERE manifest_id = ? ORDER BY ord`, id.Bytes())
	if err != nil {
		return nil, fmt.Errorf("model: load chunks: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var refs []manifest.ChunkRef
	for rows.Next() {
		var raw []byte
		var length int64
		if err := rows.Scan(&raw, &length); err != nil {
			return nil, fmt.Errorf("model: scan chunk: %w", err)
		}
		cid, err := hasher.FromBytes(raw)
		if err != nil {
			return nil, fmt.Errorf("model: load chunk id: %w", err)
		}
		refs = append(refs, manifest.ChunkRef{ID: cid, Length: length})
	}
	return refs, rows.Err()
}

func writeRow(ctx context.Context, tx *storage.Tx, m manifest.Manifest, md Metadata, id manifest.ID, vv manifest.VersionVector, seq int64, author string, authoredAt time.Time, deleted bool, deletedAt time.Time) error {
	var deletedMs, expiresMs sql.NullInt64
	if deleted {
		deletedMs = sql.NullInt64{Int64: deletedAt.UnixMilli(), Valid: true}
		expiresMs = sql.NullInt64{Int64: deletedAt.Add(TombstoneLifetime).UnixMilli(), Valid: true}
	}
	_, err := tx.Exec(ctx,
		`INSERT INTO manifests
			(path, manifest_id, kind, raw_mode, symlink_tgt, mtime_ns, size, inode, version_vec, seq, author, authored_ms, deleted, deleted_ms, expires_ms)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(path) DO UPDATE SET
			manifest_id=excluded.manifest_id, kind=excluded.kind, raw_mode=excluded.raw_mode,
			symlink_tgt=excluded.symlink_tgt, mtime_ns=excluded.mtime_ns, size=excluded.size,
			inode=excluded.inode, version_vec=excluded.version_vec, seq=excluded.seq,
			author=excluded.author, authored_ms=excluded.authored_ms,
			deleted=excluded.deleted, deleted_ms=excluded.deleted_ms, expires_ms=excluded.expires_ms`,
		m.Path, id.Bytes(), int(m.Kind), int64(m.Mode), m.SymlinkTarget,
		md.Mtime.UnixNano(), md.Size, int64(md.Inode), vv.Canonical(), seq,
		author, authoredAt.UnixMilli(), deleted, deletedMs, expiresMs)
	if err != nil {
		return fmt.Errorf("model: write manifest: %w", err)
	}
	return nil
}

func refreshStat(ctx context.Context, tx *storage.Tx, m manifest.Manifest, md Metadata) error {
	_, err := tx.Exec(ctx,
		`UPDATE manifests SET raw_mode=?, mtime_ns=?, size=?, inode=? WHERE path=?`,
		int64(m.Mode), md.Mtime.UnixNano(), md.Size, int64(md.Inode), m.Path)
	if err != nil {
		return fmt.Errorf("model: refresh stat: %w", err)
	}
	return nil
}

func writeChunks(ctx context.Context, tx *storage.Tx, id manifest.ID, chunks []manifest.ChunkRef) error {
	for i, c := range chunks {
		if _, err := tx.Exec(ctx,
			`INSERT OR IGNORE INTO manifest_chunks (manifest_id, ord, chunk_id, length) VALUES (?, ?, ?, ?)`,
			id.Bytes(), i, c.ID.Bytes(), c.Length); err != nil {
			return fmt.Errorf("model: write chunk: %w", err)
		}
	}
	return nil
}
