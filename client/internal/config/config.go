// Package config is the client's persisted, versioned configuration, stored in
// its own SQLite database so it never contends with the high-churn chunk index.
// It holds this node's identity reference and the registry of synced folders,
// including each encrypted folder's master key. All mutations are transactional.
package config

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/crypto"
	"github.com/GhentiLabs/Trove/client/internal/storage"
)

// SchemaVersion is the current config database layout. Open refuses a database
// written by a newer binary.
const SchemaVersion = 1

// MasterKeyLen is the length of a folder master key.
const MasterKeyLen = crypto.MasterKeyLen

// saltLen is the length of the Argon2id salt stored per passphrase-derived key.
const saltLen = 32

var (
	// ErrFolderNotFound is returned when no folder has the given id.
	ErrFolderNotFound = errors.New("config: folder not found")
	// ErrFolderExists is returned when adding a folder whose id already exists.
	ErrFolderExists = errors.New("config: folder already exists")
	// ErrNoKey is returned when a folder has no master key set.
	ErrNoKey = errors.New("config: folder has no master key")
	// ErrKeyExists is returned when minting a key for a folder that already has
	// one; overwriting it would orphan any data encrypted under the old key.
	ErrKeyExists = errors.New("config: folder already has a key")
	// ErrSchemaTooNew is returned when the database was written by a newer binary.
	ErrSchemaTooNew = errors.New("config: database schema newer than this binary")
	// ErrNodeMismatch is returned when the database belongs to a different node.
	ErrNodeMismatch = errors.New("config: database belongs to a different node")
)

const schema = `
CREATE TABLE IF NOT EXISTS meta (
	key   TEXT PRIMARY KEY,
	value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS folders (
	id          TEXT PRIMARY KEY,
	root        TEXT    NOT NULL,
	encrypted   INTEGER NOT NULL DEFAULT 0,
	master_key  BLOB,
	kdf_salt    BLOB,
	kdf_time    INTEGER,
	kdf_mem_kib INTEGER,
	kdf_threads INTEGER,
	created_ms  INTEGER NOT NULL
);`

// Folder is a registered sync folder.
type Folder struct {
	ID        string
	Root      string
	Encrypted bool
}

// Store is the config database handle.
type Store struct {
	db   *storage.DB
	node string
}

// Options configures Open.
type Options struct {
	// DB is the opened config database.
	DB *storage.DB
	// NodeID is this node's identity, e.g. from identity.FingerprintCert. It is
	// persisted on first open and verified on every subsequent open.
	NodeID string
}

// Open ensures the schema, checks the version, and binds the database to NodeID.
func Open(opts Options) (*Store, error) {
	if opts.DB == nil {
		return nil, errors.New("config: nil database")
	}
	if opts.NodeID == "" {
		return nil, errors.New("config: empty node id")
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
			return fmt.Errorf("config: schema: %w", err)
		}
		if err := checkMeta(ctx, tx, "schema_version", fmt.Sprint(SchemaVersion), s.validateVersion); err != nil {
			return err
		}
		return checkMeta(ctx, tx, "node_id", s.node, func(got string) error {
			if got != s.node {
				return fmt.Errorf("%w: stored %q, this node %q", ErrNodeMismatch, got, s.node)
			}
			return nil
		})
	})
}

func checkMeta(ctx context.Context, tx *storage.Tx, key, want string, validate func(got string) error) error {
	var got string
	err := tx.QueryRow(ctx, `SELECT value FROM meta WHERE key = ?`, key).Scan(&got)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if _, err := tx.Exec(ctx, `INSERT INTO meta (key, value) VALUES (?, ?)`, key, want); err != nil {
			return fmt.Errorf("config: set %s: %w", key, err)
		}
		return nil
	case err != nil:
		return fmt.Errorf("config: read %s: %w", key, err)
	default:
		return validate(got)
	}
}

func (s *Store) validateVersion(got string) error {
	var v int
	if _, err := fmt.Sscanf(got, "%d", &v); err != nil {
		return fmt.Errorf("config: unreadable schema_version %q: %w", got, err)
	}
	if v > SchemaVersion {
		return fmt.Errorf("%w: found %d, support %d", ErrSchemaTooNew, v, SchemaVersion)
	}
	return nil
}

// NodeID returns this node's identity.
func (s *Store) NodeID() string { return s.node }

// AddFolder registers a folder. It returns ErrFolderExists if the id is taken.
func (s *Store) AddFolder(ctx context.Context, f Folder) error {
	return s.db.WithTx(ctx, func(tx *storage.Tx) error {
		var exists int
		err := tx.QueryRow(ctx, `SELECT 1 FROM folders WHERE id = ?`, f.ID).Scan(&exists)
		if err == nil {
			return ErrFolderExists
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("config: check folder: %w", err)
		}
		_, err = tx.Exec(ctx,
			`INSERT INTO folders (id, root, encrypted, created_ms) VALUES (?, ?, ?, ?)`,
			f.ID, f.Root, f.Encrypted, time.Now().UnixMilli())
		if err != nil {
			return fmt.Errorf("config: add folder: %w", err)
		}
		return nil
	})
}

// GetFolder returns the folder with the given id.
func (s *Store) GetFolder(ctx context.Context, id string) (Folder, error) {
	var f Folder
	err := s.db.QueryRow(ctx, `SELECT id, root, encrypted FROM folders WHERE id = ?`, id).
		Scan(&f.ID, &f.Root, &f.Encrypted)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return Folder{}, ErrFolderNotFound
	case err != nil:
		return Folder{}, fmt.Errorf("config: get folder: %w", err)
	}
	return f, nil
}

// ListFolders returns all registered folders ordered by id.
func (s *Store) ListFolders(ctx context.Context) ([]Folder, error) {
	rows, err := s.db.Query(ctx, `SELECT id, root, encrypted FROM folders ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("config: list folders: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []Folder
	for rows.Next() {
		var f Folder
		if err := rows.Scan(&f.ID, &f.Root, &f.Encrypted); err != nil {
			return nil, fmt.Errorf("config: scan folder: %w", err)
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// RemoveFolder deletes a folder and its key.
func (s *Store) RemoveFolder(ctx context.Context, id string) error {
	return s.db.WithTx(ctx, func(tx *storage.Tx) error {
		res, err := tx.Exec(ctx, `DELETE FROM folders WHERE id = ?`, id)
		if err != nil {
			return fmt.Errorf("config: remove folder: %w", err)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return ErrFolderNotFound
		}
		return nil
	})
}

// SetFolderKey stores an explicit master key for a folder, clearing any recorded
// passphrase-derivation parameters.
func (s *Store) SetFolderKey(ctx context.Context, id string, key [MasterKeyLen]byte) error {
	return s.updateKey(ctx, id, key[:], nil, nil, nil, nil)
}

// GenerateFolderKey mints a random master key for a folder and stores it. It
// refuses to overwrite an existing key (use SetFolderKey to replace one).
func (s *Store) GenerateFolderKey(ctx context.Context, id string) ([MasterKeyLen]byte, error) {
	var key [MasterKeyLen]byte
	if _, err := rand.Read(key[:]); err != nil {
		return [MasterKeyLen]byte{}, fmt.Errorf("config: generate key: %w", err)
	}
	if err := s.setKeyIfAbsent(ctx, id, key[:], nil, nil, nil, nil); err != nil {
		return [MasterKeyLen]byte{}, err
	}
	return key, nil
}

// DeriveFolderKey derives a folder master key from a passphrase via Argon2id,
// storing the key together with the salt and parameters so the same passphrase
// reproduces it later. It refuses to overwrite an existing key.
func (s *Store) DeriveFolderKey(ctx context.Context, id, passphrase string) ([MasterKeyLen]byte, error) {
	salt := make([]byte, saltLen)
	if _, err := rand.Read(salt); err != nil {
		return [MasterKeyLen]byte{}, fmt.Errorf("config: salt: %w", err)
	}
	key := crypto.DeriveMasterKey(passphrase, salt)
	t, m, p := int64(crypto.ArgonTime), int64(crypto.ArgonMemoryKiB), int64(crypto.ArgonThreads)
	if err := s.setKeyIfAbsent(ctx, id, key[:], salt, &t, &m, &p); err != nil {
		return [MasterKeyLen]byte{}, err
	}
	return key, nil
}

// setKeyIfAbsent writes a key only if the folder exists and has none, checking
// and writing in one transaction so concurrent callers cannot both succeed.
func (s *Store) setKeyIfAbsent(ctx context.Context, id string, key, salt []byte, t, m, p *int64) error {
	return s.db.WithTx(ctx, func(tx *storage.Tx) error {
		var existing []byte
		err := tx.QueryRow(ctx, `SELECT master_key FROM folders WHERE id = ?`, id).Scan(&existing)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return ErrFolderNotFound
		case err != nil:
			return fmt.Errorf("config: check key: %w", err)
		case existing != nil:
			return ErrKeyExists
		}
		_, err = tx.Exec(ctx,
			`UPDATE folders SET master_key = ?, kdf_salt = ?, kdf_time = ?, kdf_mem_kib = ?, kdf_threads = ? WHERE id = ?`,
			key, salt, t, m, p, id)
		if err != nil {
			return fmt.Errorf("config: set key: %w", err)
		}
		return nil
	})
}

func (s *Store) updateKey(ctx context.Context, id string, key, salt []byte, t, m, p *int64) error {
	return s.db.WithTx(ctx, func(tx *storage.Tx) error {
		res, err := tx.Exec(ctx,
			`UPDATE folders SET master_key = ?, kdf_salt = ?, kdf_time = ?, kdf_mem_kib = ?, kdf_threads = ? WHERE id = ?`,
			key, salt, t, m, p, id)
		if err != nil {
			return fmt.Errorf("config: set key: %w", err)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return ErrFolderNotFound
		}
		return nil
	})
}

// GetFolderKey returns a folder's master key. It returns ErrNoKey if the folder
// exists but has no key.
func (s *Store) GetFolderKey(ctx context.Context, id string) ([MasterKeyLen]byte, error) {
	var raw []byte
	err := s.db.QueryRow(ctx, `SELECT master_key FROM folders WHERE id = ?`, id).Scan(&raw)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return [MasterKeyLen]byte{}, ErrFolderNotFound
	case err != nil:
		return [MasterKeyLen]byte{}, fmt.Errorf("config: get key: %w", err)
	case raw == nil:
		return [MasterKeyLen]byte{}, ErrNoKey
	case len(raw) != MasterKeyLen:
		return [MasterKeyLen]byte{}, fmt.Errorf("config: stored key length %d, want %d", len(raw), MasterKeyLen)
	}
	var key [MasterKeyLen]byte
	copy(key[:], raw)
	return key, nil
}
