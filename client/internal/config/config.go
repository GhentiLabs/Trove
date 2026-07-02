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

// SchemaVersion is the config database layout version. Open refuses a database
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
	// ErrKeyExists is returned when minting a key for a folder that already has one.
	ErrKeyExists = errors.New("config: folder already has a key")
	// ErrSchemaTooNew is returned when the database was written by a newer binary.
	ErrSchemaTooNew = errors.New("config: database schema newer than this binary")
	// ErrNoSecret is returned when a folder has neither a master key nor a recovery secret.
	ErrNoSecret = errors.New("config: folder has no recovery secret")
	// ErrSecretExists is returned when minting a recovery secret for a folder that has one.
	ErrSecretExists = errors.New("config: folder already has a recovery secret")
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
	created_ms      INTEGER NOT NULL,
	share_id        TEXT    NOT NULL DEFAULT '',
	holder          INTEGER NOT NULL DEFAULT 0,
	holder_verifier BLOB,
	recovery_secret BLOB,
	keep_history    INTEGER NOT NULL DEFAULT 1,
	quota_bytes     INTEGER NOT NULL DEFAULT 0,
	CHECK (holder = 0 OR encrypted = 1)
);`

// Folder is a registered sync folder.
type Folder struct {
	ID        string
	Root      string
	Encrypted bool
	// ShareID is the cross-node match key agreed at pairing, distinct from ID and the
	// encryption key. Empty until the folder is paired.
	ShareID string
	// Holder marks a folder this node stores only as untrusted ciphertext, never the
	// key or plaintext.
	Holder bool
	// KeepHistory retains snapshots of old versions (a backup folder). When false the
	// folder is sync-only: no snapshots, so superseded data is reclaimed and the
	// folder stays at ~1x disk.
	KeepHistory bool
	// QuotaBytes caps the storage this node lends the folder; 0 means unlimited.
	QuotaBytes int64
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
	if stored > SchemaVersion {
		return fmt.Errorf("%w: found %d, support %d", ErrSchemaTooNew, stored, SchemaVersion)
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
			`INSERT INTO folders (id, root, encrypted, created_ms, share_id, holder, keep_history, quota_bytes) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			f.ID, f.Root, f.Encrypted, time.Now().UnixMilli(), f.ShareID, f.Holder, f.KeepHistory, f.QuotaBytes)
		if err != nil {
			return fmt.Errorf("config: add folder: %w", err)
		}
		return nil
	})
}

// GetFolder returns the folder with the given id.
func (s *Store) GetFolder(ctx context.Context, id string) (Folder, error) {
	var f Folder
	err := s.db.QueryRow(ctx, `SELECT id, root, encrypted, share_id, holder, keep_history, quota_bytes FROM folders WHERE id = ?`, id).
		Scan(&f.ID, &f.Root, &f.Encrypted, &f.ShareID, &f.Holder, &f.KeepHistory, &f.QuotaBytes)
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
	rows, err := s.db.Query(ctx, `SELECT id, root, encrypted, share_id, holder, keep_history, quota_bytes FROM folders ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("config: list folders: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []Folder
	for rows.Next() {
		var f Folder
		if err := rows.Scan(&f.ID, &f.Root, &f.Encrypted, &f.ShareID, &f.Holder, &f.KeepHistory, &f.QuotaBytes); err != nil {
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

// SetFolderQuota changes a folder's storage cap; 0 means unlimited.
func (s *Store) SetFolderQuota(ctx context.Context, id string, quota int64) error {
	return s.db.WithTx(ctx, func(tx *storage.Tx) error {
		res, err := tx.Exec(ctx, `UPDATE folders SET quota_bytes = ? WHERE id = ?`, quota, id)
		if err != nil {
			return fmt.Errorf("config: set quota: %w", err)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return ErrFolderNotFound
		}
		return nil
	})
}

// SetHolderVerifier records the non-secret folder verifier a holder learned from a trusted
// member's advertisement, so a later restore can be proven against it without the key.
func (s *Store) SetHolderVerifier(ctx context.Context, id string, verifier []byte) error {
	return s.db.WithTx(ctx, func(tx *storage.Tx) error {
		res, err := tx.Exec(ctx, `UPDATE folders SET holder_verifier = ? WHERE id = ?`, verifier, id)
		if err != nil {
			return fmt.Errorf("config: set holder verifier: %w", err)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return ErrFolderNotFound
		}
		return nil
	})
}

// GetHolderVerifier returns a folder's stored holder verifier, or nil if none is set.
func (s *Store) GetHolderVerifier(ctx context.Context, id string) ([]byte, error) {
	var v []byte
	err := s.db.QueryRow(ctx, `SELECT holder_verifier FROM folders WHERE id = ?`, id).Scan(&v)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, ErrFolderNotFound
	case err != nil:
		return nil, fmt.Errorf("config: get holder verifier: %w", err)
	}
	return v, nil
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
// has none yet. A replay or duplicate returns ErrKeyExists.
func (s *Store) DeliverFolderKey(ctx context.Context, id string, key [MasterKeyLen]byte, generation int) error {
	return s.setKeyIfAbsent(ctx, id, key[:], generation, nil, nil, nil, nil)
}

// GenerateRecoverySecret mints and stores an unencrypted folder's recovery secret, refusing to
// overwrite an existing one. (An encrypted folder uses its master key; see FolderSecret.)
func (s *Store) GenerateRecoverySecret(ctx context.Context, id string) ([MasterKeyLen]byte, error) {
	var secret [MasterKeyLen]byte
	if _, err := rand.Read(secret[:]); err != nil {
		return [MasterKeyLen]byte{}, fmt.Errorf("config: generate recovery secret: %w", err)
	}
	if err := s.setRecoverySecretIfAbsent(ctx, id, secret[:]); err != nil {
		return [MasterKeyLen]byte{}, err
	}
	return secret, nil
}

// DeliverRecoverySecret stores a recovery secret received from a trusted member of an
// unencrypted folder, only if the folder has none yet. A replay returns ErrSecretExists.
func (s *Store) DeliverRecoverySecret(ctx context.Context, id string, secret [MasterKeyLen]byte) error {
	return s.setRecoverySecretIfAbsent(ctx, id, secret[:])
}

func (s *Store) setRecoverySecretIfAbsent(ctx context.Context, id string, secret []byte) error {
	return s.db.WithTx(ctx, func(tx *storage.Tx) error {
		var existing []byte
		err := tx.QueryRow(ctx, `SELECT recovery_secret FROM folders WHERE id = ?`, id).Scan(&existing)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return ErrFolderNotFound
		case err != nil:
			return fmt.Errorf("config: check recovery secret: %w", err)
		case existing != nil:
			return ErrSecretExists
		}
		if _, err := tx.Exec(ctx, `UPDATE folders SET recovery_secret = ? WHERE id = ?`, secret, id); err != nil {
			return fmt.Errorf("config: set recovery secret: %w", err)
		}
		return nil
	})
}

// FolderSecret returns the secret a folder's recovery verifier derives from — the master key if
// encrypted, the recovery secret if not — or ErrNoSecret if it has neither yet.
func (s *Store) FolderSecret(ctx context.Context, id string) ([MasterKeyLen]byte, error) {
	var (
		encrypted bool
		master    []byte
		recovery  []byte
	)
	err := s.db.QueryRow(ctx, `SELECT encrypted, master_key, recovery_secret FROM folders WHERE id = ?`, id).
		Scan(&encrypted, &master, &recovery)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return [MasterKeyLen]byte{}, ErrFolderNotFound
	case err != nil:
		return [MasterKeyLen]byte{}, fmt.Errorf("config: get folder secret: %w", err)
	}
	raw := recovery
	if encrypted {
		raw = master
	}
	switch {
	case raw == nil:
		return [MasterKeyLen]byte{}, ErrNoSecret
	case len(raw) != MasterKeyLen:
		return [MasterKeyLen]byte{}, fmt.Errorf("config: stored secret length %d, want %d", len(raw), MasterKeyLen)
	}
	var secret [MasterKeyLen]byte
	copy(secret[:], raw)
	return secret, nil
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
