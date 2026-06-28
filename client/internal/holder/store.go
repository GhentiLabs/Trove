// Package holder implements the untrusted storage tier: a node that keeps a folder's
// data as opaque, key-blinded ciphertext blobs and serves them back without ever
// holding the folder key. It learns only blob sizes — never names, paths, or content.
package holder

import (
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/GhentiLabs/Trove/client/internal/crypto"
)

// ErrNotFound is returned when no blob is stored under a blinded id.
var ErrNotFound = errors.New("holder: blob not found")

// Store is a per-folder blind blob store: it maps a blinded id to opaque bytes on disk,
// one file per blob. It never sees plaintext or the folder key.
type Store struct {
	dir string
}

// Open creates the store directory if needed and returns a handle.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("holder: store dir: %w", err)
	}
	return &Store{dir: dir}, nil
}

// path shards blobs by the first byte of the blinded id so no single directory holds the
// whole folder's chunks.
func (s *Store) path(blinded [crypto.BlindLen]byte) (dir, file string) {
	name := hex.EncodeToString(blinded[:])
	return filepath.Join(s.dir, name[:2]), filepath.Join(s.dir, name[:2], name)
}

// Put stores data under the blinded id, replacing any existing blob atomically.
func (s *Store) Put(blinded [crypto.BlindLen]byte, data []byte) error {
	shard, final := s.path(blinded)
	if err := os.MkdirAll(shard, 0o700); err != nil {
		return fmt.Errorf("holder: shard dir: %w", err)
	}
	tmp, err := os.CreateTemp(shard, "put-*")
	if err != nil {
		return fmt.Errorf("holder: temp: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("holder: write: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("holder: sync: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("holder: close: %w", err)
	}
	if err := os.Rename(tmpName, final); err != nil {
		return fmt.Errorf("holder: rename: %w", err)
	}
	return nil
}

// Get returns the blob stored under the blinded id, or ErrNotFound.
func (s *Store) Get(blinded [crypto.BlindLen]byte) ([]byte, error) {
	_, file := s.path(blinded)
	data, err := os.ReadFile(file)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("holder: read: %w", err)
	}
	return data, nil
}

// Has reports whether a blob is stored under the blinded id.
func (s *Store) Has(blinded [crypto.BlindLen]byte) bool {
	_, file := s.path(blinded)
	_, err := os.Stat(file)
	return err == nil
}

// Delete removes the blob stored under the blinded id, if present. It is the primitive a
// future sweep uses to reclaim blobs no longer referenced by the catalog.
func (s *Store) Delete(blinded [crypto.BlindLen]byte) error {
	_, file := s.path(blinded)
	if err := os.Remove(file); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("holder: delete: %w", err)
	}
	return nil
}
