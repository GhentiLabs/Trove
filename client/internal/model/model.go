// Package model is the client's per-folder sync-state store: the versioned set of
// manifests describing the folder's current paths, the immutable snapshots that
// name whole folder states, and the tombstones recording deletions. It is held in
// its own SQLite database, separate from the chunk index, so snapshot writes never
// contend with the chunk store's hot path. All mutations are transactional.
package model

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/storage"
)

// SchemaVersion is the current sync-state database layout. Open refuses a database
// written by a newer binary.
const SchemaVersion = 1

// TombstoneLifetime is how long a deletion is retained before SweepTombstones may
// remove it. It must exceed the longest plausible offline window so a peer cannot
// resurrect a deleted file.
const TombstoneLifetime = 90 * 24 * time.Hour

const (
	counterManifestSeq = "manifest_seq"
	counterSnapshotSeq = "snapshot_seq"
)

var (
	// ErrManifestNotFound is returned when no manifest has the given path.
	ErrManifestNotFound = errors.New("model: manifest not found")
	// ErrSnapshotNotFound is returned when no snapshot has the given root.
	ErrSnapshotNotFound = errors.New("model: snapshot not found")
	// ErrCorruptModel is returned when a stored manifest no longer hashes to its
	// recorded identity.
	ErrCorruptModel = errors.New("model: stored manifest fails identity verification")
	// ErrSchemaTooNew is returned when the database was written by a newer binary.
	ErrSchemaTooNew = errors.New("model: database schema newer than this binary")
	// ErrNodeMismatch is returned when the database belongs to a different node.
	ErrNodeMismatch = errors.New("model: database belongs to a different node")
	// ErrInvalidManifest is returned when a manifest is not well-formed for its kind.
	ErrInvalidManifest = errors.New("model: invalid manifest")
)

const schema = `
CREATE TABLE IF NOT EXISTS meta (
	key   TEXT PRIMARY KEY,
	value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS manifests (
	path        TEXT PRIMARY KEY,
	manifest_id BLOB    NOT NULL,
	kind        INTEGER NOT NULL,
	raw_mode    INTEGER NOT NULL,
	symlink_tgt TEXT    NOT NULL DEFAULT '',
	mtime_ns    INTEGER NOT NULL,
	size        INTEGER NOT NULL,
	inode       INTEGER NOT NULL DEFAULT 0,
	version_vec BLOB    NOT NULL,
	seq         INTEGER NOT NULL,
	deleted     INTEGER NOT NULL DEFAULT 0,
	deleted_ms  INTEGER,
	expires_ms  INTEGER
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS manifests_by_seq ON manifests(seq);
CREATE INDEX IF NOT EXISTS manifests_by_id ON manifests(manifest_id);
CREATE TABLE IF NOT EXISTS manifest_chunks (
	manifest_id BLOB    NOT NULL,
	ord         INTEGER NOT NULL,
	chunk_id    BLOB    NOT NULL,
	length      INTEGER NOT NULL,
	PRIMARY KEY (manifest_id, ord)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS snapshots (
	snap_seq    INTEGER PRIMARY KEY,
	root_hash   BLOB    NOT NULL,
	parent_seq  INTEGER,
	created_ms  INTEGER NOT NULL,
	created_by  TEXT    NOT NULL,
	manifest_n  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS snapshots_by_root ON snapshots(root_hash);
CREATE TABLE IF NOT EXISTS snapshot_manifests (
	snap_seq    INTEGER NOT NULL,
	path        TEXT    NOT NULL,
	manifest_id BLOB    NOT NULL,
	deleted     INTEGER NOT NULL,
	PRIMARY KEY (snap_seq, path)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS counters (
	name TEXT    PRIMARY KEY,
	next INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS replica_cursors (
	folder_id     TEXT    NOT NULL,
	owner_peer_id TEXT    NOT NULL,
	epoch         INTEGER NOT NULL,
	high_water    INTEGER NOT NULL,
	updated_ms    INTEGER NOT NULL,
	PRIMARY KEY (folder_id, owner_peer_id)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS folder_epoch (
	id         INTEGER PRIMARY KEY CHECK (id = 1),
	epoch      INTEGER NOT NULL,
	created_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS sync_receipts (
	peer_id       TEXT PRIMARY KEY,
	snapshot_root BLOB    NOT NULL,
	epoch         INTEGER NOT NULL,
	high_water    INTEGER NOT NULL,
	synced_ms     INTEGER NOT NULL
) WITHOUT ROWID;`

// querier is the read surface shared by *storage.DB and *storage.Tx, so load
// helpers can run both inside and outside a transaction.
type querier interface {
	QueryRow(ctx context.Context, query string, args ...any) *sql.Row
	Query(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// Store is the sync-state database handle.
type Store struct {
	db   *storage.DB
	node string
}

// Options configures Open.
type Options struct {
	// DB is the opened sync-state database.
	DB *storage.DB
	// NodeID is this node's identity. It is persisted on first open and verified
	// on every subsequent open.
	NodeID string
}

// Open ensures the schema, checks the version, and binds the database to NodeID.
func Open(opts Options) (*Store, error) {
	if opts.DB == nil {
		return nil, errors.New("model: nil database")
	}
	if opts.NodeID == "" {
		return nil, errors.New("model: empty node id")
	}
	s := &Store{db: opts.DB, node: opts.NodeID}
	if err := s.init(context.Background()); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) init(ctx context.Context) error {
	return s.db.WithTx(ctx, func(tx *storage.Tx) error {
		if _, err := tx.Exec(ctx, schema); err != nil {
			return fmt.Errorf("model: schema: %w", err)
		}
		for _, name := range []string{counterManifestSeq, counterSnapshotSeq} {
			if _, err := tx.Exec(ctx, `INSERT OR IGNORE INTO counters (name, next) VALUES (?, 1)`, name); err != nil {
				return fmt.Errorf("model: init counter %s: %w", name, err)
			}
		}
		if err := storage.CheckMeta(ctx, tx, "schema_version", fmt.Sprint(SchemaVersion), s.validateVersion); err != nil {
			return err
		}
		return storage.CheckMeta(ctx, tx, "node_id", s.node, func(got string) error {
			if got != s.node {
				return fmt.Errorf("%w: stored %q, this node %q", ErrNodeMismatch, got, s.node)
			}
			return nil
		})
	})
}

func (s *Store) validateVersion(got string) error {
	var v int
	if _, err := fmt.Sscanf(got, "%d", &v); err != nil {
		return fmt.Errorf("model: unreadable schema_version %q: %w", got, err)
	}
	if v > SchemaVersion {
		return fmt.Errorf("%w: found %d, support %d", ErrSchemaTooNew, v, SchemaVersion)
	}
	return nil
}

// NodeID returns this node's identity.
func (s *Store) NodeID() string { return s.node }

func allocate(ctx context.Context, tx *storage.Tx, name string) (int64, error) {
	var n int64
	if err := tx.QueryRow(ctx, `SELECT next FROM counters WHERE name = ?`, name).Scan(&n); err != nil {
		return 0, fmt.Errorf("model: read counter %s: %w", name, err)
	}
	if _, err := tx.Exec(ctx, `UPDATE counters SET next = ? WHERE name = ?`, n+1, name); err != nil {
		return 0, fmt.Errorf("model: bump counter %s: %w", name, err)
	}
	return n, nil
}
