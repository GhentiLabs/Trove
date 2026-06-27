// Package config is the client's persisted, versioned configuration in its own
// SQLite database, holding this node's identity reference and the registry of synced
// folders (with each encrypted folder's master key). All mutations are transactional.
package config

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base32"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/crypto"
	"github.com/GhentiLabs/Trove/client/internal/storage"
)

// SchemaVersion is the current config database layout. Open refuses a database
// written by a newer binary and migrates older ones forward.
const SchemaVersion = 4

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
	// ErrKeyExists is returned when minting a key for a folder that already has one.
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
	id             TEXT PRIMARY KEY,
	root           TEXT    NOT NULL,
	encrypted      INTEGER NOT NULL DEFAULT 0,
	master_key     BLOB,
	key_generation INTEGER NOT NULL DEFAULT 0,
	kdf_salt       BLOB,
	kdf_time       INTEGER,
	kdf_mem_kib    INTEGER,
	kdf_threads    INTEGER,
	created_ms     INTEGER NOT NULL,
	share_id       TEXT    NOT NULL DEFAULT '',
	holder         INTEGER NOT NULL DEFAULT 0
);`

// Folder is a registered sync folder.
type Folder struct {
	ID        string
	Root      string
	Encrypted bool
	// ShareID is the cross-node match key agreed at pairing, distinct from ID and the
	// encryption key. Empty until the folder is paired.
	ShareID string
	// Holder marks a folder this node stores only as untrusted ciphertext: it keeps
	// blinded blobs and never holds the key, a root tree, or plaintext.
	Holder bool
}

// Store is the config database handle.
type Store struct {
	db   *storage.DB
	node string
}

// Options configures Open.
type Options struct {
	DB *storage.DB
	// NodeID is persisted on first open and verified on every subsequent open.
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
		if err := checkVersion(ctx, tx); err != nil {
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

func checkVersion(ctx context.Context, tx *storage.Tx) error {
	var v string
	err := tx.QueryRow(ctx, `SELECT value FROM meta WHERE key = 'schema_version'`).Scan(&v)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if _, err := tx.Exec(ctx, `INSERT INTO meta (key, value) VALUES ('schema_version', ?)`, SchemaVersion); err != nil {
			return fmt.Errorf("config: set version: %w", err)
		}
		return nil
	case err != nil:
		return fmt.Errorf("config: read version: %w", err)
	}
	var stored int
	if _, err := fmt.Sscanf(v, "%d", &stored); err != nil {
		return fmt.Errorf("config: unreadable schema_version %q: %w", v, err)
	}
	switch {
	case stored > SchemaVersion:
		return fmt.Errorf("%w: found %d, support %d", ErrSchemaTooNew, stored, SchemaVersion)
	case stored < SchemaVersion:
		return migrate(ctx, tx, stored)
	}
	return nil
}

// migrate upgrades an older config database to SchemaVersion in place. The schema
// statement already creates wholly new tables; migrate only alters existing ones.
func migrate(ctx context.Context, tx *storage.Tx, from int) error {
	if from < 2 {
		if _, err := tx.Exec(ctx, `ALTER TABLE folders ADD COLUMN share_id TEXT NOT NULL DEFAULT ''`); err != nil {
			return fmt.Errorf("config: migrate v2: %w", err)
		}
	}
	if from < 3 {
		if _, err := tx.Exec(ctx, `ALTER TABLE folders ADD COLUMN key_generation INTEGER NOT NULL DEFAULT 0`); err != nil {
			return fmt.Errorf("config: migrate v3: %w", err)
		}
	}
	if from < 4 {
		if _, err := tx.Exec(ctx, `ALTER TABLE folders ADD COLUMN holder INTEGER NOT NULL DEFAULT 0`); err != nil {
			return fmt.Errorf("config: migrate v4: %w", err)
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE meta SET value = ? WHERE key = 'schema_version'`, SchemaVersion); err != nil {
		return fmt.Errorf("config: set version: %w", err)
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
			`INSERT INTO folders (id, root, encrypted, created_ms, share_id, holder) VALUES (?, ?, ?, ?, ?, ?)`,
			f.ID, f.Root, f.Encrypted, time.Now().UnixMilli(), f.ShareID, f.Holder)
		if err != nil {
			return fmt.Errorf("config: add folder: %w", err)
		}
		return nil
	})
}

// GetFolder returns the folder with the given id.
func (s *Store) GetFolder(ctx context.Context, id string) (Folder, error) {
	var f Folder
	err := s.db.QueryRow(ctx, `SELECT id, root, encrypted, share_id, holder FROM folders WHERE id = ?`, id).
		Scan(&f.ID, &f.Root, &f.Encrypted, &f.ShareID, &f.Holder)
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
	rows, err := s.db.Query(ctx, `SELECT id, root, encrypted, share_id, holder FROM folders ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("config: list folders: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []Folder
	for rows.Next() {
		var f Folder
		if err := rows.Scan(&f.ID, &f.Root, &f.Encrypted, &f.ShareID, &f.Holder); err != nil {
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

// SetFolderShareID binds a folder to its group id.
func (s *Store) SetFolderShareID(ctx context.Context, id, shareID string) error {
	return s.db.WithTx(ctx, func(tx *storage.Tx) error {
		res, err := tx.Exec(ctx, `UPDATE folders SET share_id = ? WHERE id = ?`, shareID, id)
		if err != nil {
			return fmt.Errorf("config: set share id: %w", err)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return ErrFolderNotFound
		}
		return nil
	})
}

// FirstKeyGeneration is the epoch stamped when a folder key is first established.
const FirstKeyGeneration = 1

// GenerateFolderKey mints a random master key for a folder and stores it. It
// refuses to overwrite an existing key.
func (s *Store) GenerateFolderKey(ctx context.Context, id string) ([MasterKeyLen]byte, error) {
	var key [MasterKeyLen]byte
	if _, err := rand.Read(key[:]); err != nil {
		return [MasterKeyLen]byte{}, fmt.Errorf("config: generate key: %w", err)
	}
	if err := s.setKeyIfAbsent(ctx, id, key[:], FirstKeyGeneration, nil, nil, nil, nil); err != nil {
		return [MasterKeyLen]byte{}, err
	}
	return key, nil
}

// DeliverFolderKey stores a key received from a trusted member, only if the folder
// has none yet. A replayed or duplicate delivery returns ErrKeyExists, which the
// caller treats as already-keyed; it never clobbers a stored key.
func (s *Store) DeliverFolderKey(ctx context.Context, id string, key [MasterKeyLen]byte, generation int) error {
	return s.setKeyIfAbsent(ctx, id, key[:], generation, nil, nil, nil, nil)
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
	if err := s.setKeyIfAbsent(ctx, id, key[:], FirstKeyGeneration, salt, &t, &m, &p); err != nil {
		return [MasterKeyLen]byte{}, err
	}
	return key, nil
}

// setKeyIfAbsent writes a key only if the folder exists and has none, in one
// transaction so concurrent callers cannot both succeed.
func (s *Store) setKeyIfAbsent(ctx context.Context, id string, key []byte, generation int, salt []byte, t, m, p *int64) error {
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
			`UPDATE folders SET master_key = ?, key_generation = ?, kdf_salt = ?, kdf_time = ?, kdf_mem_kib = ?, kdf_threads = ? WHERE id = ?`,
			key, generation, salt, t, m, p, id)
		if err != nil {
			return fmt.Errorf("config: set key: %w", err)
		}
		return nil
	})
}

// GetFolderKey returns a folder's master key and its generation. It returns
// ErrNoKey if the folder exists but has no key.
func (s *Store) GetFolderKey(ctx context.Context, id string) ([MasterKeyLen]byte, int, error) {
	var (
		raw []byte
		gen int
	)
	err := s.db.QueryRow(ctx, `SELECT master_key, key_generation FROM folders WHERE id = ?`, id).Scan(&raw, &gen)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return [MasterKeyLen]byte{}, 0, ErrFolderNotFound
	case err != nil:
		return [MasterKeyLen]byte{}, 0, fmt.Errorf("config: get key: %w", err)
	case raw == nil:
		return [MasterKeyLen]byte{}, 0, ErrNoKey
	case len(raw) != MasterKeyLen:
		return [MasterKeyLen]byte{}, 0, fmt.Errorf("config: stored key length %d, want %d", len(raw), MasterKeyLen)
	}
	var key [MasterKeyLen]byte
	copy(key[:], raw)
	return key, gen, nil
}

var recoveryEncoding = base32.StdEncoding.WithPadding(base32.NoPadding)

// EncodeRecoveryCode renders a folder master key as a base32 recovery code.
func EncodeRecoveryCode(key [MasterKeyLen]byte) string {
	return recoveryEncoding.EncodeToString(key[:])
}

// DecodeRecoveryCode parses a base32 recovery code back into a master key.
func DecodeRecoveryCode(code string) ([MasterKeyLen]byte, error) {
	raw, err := recoveryEncoding.DecodeString(strings.ToUpper(strings.TrimSpace(code)))
	if err != nil {
		return [MasterKeyLen]byte{}, fmt.Errorf("config: decode recovery code: %w", err)
	}
	if len(raw) != MasterKeyLen {
		return [MasterKeyLen]byte{}, fmt.Errorf("config: recovery code decodes to %d bytes, want %d", len(raw), MasterKeyLen)
	}
	var key [MasterKeyLen]byte
	copy(key[:], raw)
	return key, nil
}
