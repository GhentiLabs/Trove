package model

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/manifest"
	"github.com/GhentiLabs/Trove/client/internal/snapshot"
	"github.com/GhentiLabs/Trove/client/internal/storage"
)

// RemoteManifest is an owner's manifest to be applied verbatim on a replica: its
// content, the owner's identity and version vector, the owner sequence that drives
// the replica's cursor, and the tombstone state.
type RemoteManifest struct {
	Manifest  manifest.Manifest
	ID        manifest.ID
	Version   manifest.VersionVector
	OwnerSeq  int64
	Deleted   bool
	DeletedAt time.Time
	Metadata  Metadata
}

// FolderEpoch returns this folder's stable epoch, allocating and persisting a
// random nonzero value on first call. The epoch identifies the owner's sequence
// lineage so a replica can detect a rebuild and force a full resync.
func (s *Store) FolderEpoch(ctx context.Context) (uint64, error) {
	var epoch uint64
	err := s.db.WithTx(ctx, func(tx *storage.Tx) error {
		var stored int64
		err := tx.QueryRow(ctx, `SELECT epoch FROM folder_epoch WHERE id = 1`).Scan(&stored)
		switch {
		case err == nil:
			epoch = uint64(stored)
			return nil
		case !errors.Is(err, sql.ErrNoRows):
			return fmt.Errorf("model: read epoch: %w", err)
		}
		epoch = newEpoch()
		if _, err := tx.Exec(ctx,
			`INSERT INTO folder_epoch (id, epoch, created_ms) VALUES (1, ?, ?)`,
			int64(epoch), time.Now().UnixMilli()); err != nil {
			return fmt.Errorf("model: write epoch: %w", err)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return epoch, nil
}

func newEpoch() uint64 {
	var b [8]byte
	_, _ = rand.Read(b[:])
	// Keep the high bit clear so the value round-trips through SQLite's signed
	// INTEGER as a positive number; nonzero distinguishes "unset".
	if e := binary.BigEndian.Uint64(b[:]) &^ (1 << 63); e != 0 {
		return e
	}
	return 1
}

// HighWater is the maximum manifest sequence currently stored, or zero if the
// folder is empty. It is the owner's announce high-water.
func (s *Store) HighWater(ctx context.Context) (int64, error) {
	var hw sql.NullInt64
	if err := s.db.QueryRow(ctx, `SELECT MAX(seq) FROM manifests`).Scan(&hw); err != nil {
		return 0, fmt.Errorf("model: high water: %w", err)
	}
	return hw.Int64, nil
}

// CurrentRoot is the Merkle root of the current manifest set, computed without
// cutting a snapshot. It is the convergence check: two nodes holding the same set
// share a root.
func (s *Store) CurrentRoot(ctx context.Context) (snapshot.Root, error) {
	var root snapshot.Root
	err := s.db.WithTx(ctx, func(tx *storage.Tx) error {
		leaves, err := currentLeaves(ctx, tx)
		if err != nil {
			return err
		}
		root = leaves.Root()
		return nil
	})
	if err != nil {
		return snapshot.Root{}, err
	}
	return root, nil
}

// ApplyRemoteAndAdvance stores batch and advances the (folder, owner) cursor in one
// transaction. The owner's version vector is stored verbatim; this node is never
// added to it. Returns ErrCorruptModel if a manifest does not hash to its supplied id.
func (s *Store) ApplyRemoteAndAdvance(ctx context.Context, batch []RemoteManifest, folderID, ownerPeerID string, epoch uint64, highWater int64) error {
	return s.db.WithTx(ctx, func(tx *storage.Tx) error {
		for _, rm := range batch {
			m := rm.Manifest
			m.Path = manifest.NormalizePath(m.Path)
			m.SymlinkTarget = manifest.NormalizePath(m.SymlinkTarget)
			if err := validate(m); err != nil {
				return err
			}
			if m.ID() != rm.ID {
				return fmt.Errorf("%w: path %q", ErrCorruptModel, m.Path)
			}
			if err := writeRow(ctx, tx, m, rm.Metadata, rm.ID, rm.Version, rm.OwnerSeq, rm.Deleted, rm.DeletedAt); err != nil {
				return err
			}
			if err := writeChunks(ctx, tx, rm.ID, m.Chunks); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO replica_cursors (folder_id, owner_peer_id, epoch, high_water, updated_ms)
			 VALUES (?, ?, ?, ?, ?)
			 ON CONFLICT(folder_id, owner_peer_id) DO UPDATE SET
				epoch=excluded.epoch, high_water=excluded.high_water, updated_ms=excluded.updated_ms`,
			folderID, ownerPeerID, int64(epoch), highWater, time.Now().UnixMilli()); err != nil {
			return fmt.Errorf("model: save cursor: %w", err)
		}
		return nil
	})
}

// LoadCursor returns the consumed (epoch, high_water) for a (folder, owner). ok is
// false if the replica has never applied from that owner for that folder.
func (s *Store) LoadCursor(ctx context.Context, folderID, ownerPeerID string) (epoch uint64, highWater int64, ok bool, err error) {
	var ep int64
	err = s.db.QueryRow(ctx,
		`SELECT epoch, high_water FROM replica_cursors WHERE folder_id = ? AND owner_peer_id = ?`,
		folderID, ownerPeerID).Scan(&ep, &highWater)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, 0, false, nil
	case err != nil:
		return 0, 0, false, fmt.Errorf("model: load cursor: %w", err)
	}
	return uint64(ep), highWater, true, nil
}
