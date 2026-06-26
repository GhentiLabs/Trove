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
	"github.com/GhentiLabs/Trove/pkg/identity"
)

// RemoteManifest is a peer's manifest to be reconciled into the local model: its
// content, the authoring node's version vector and edit stamp, and the tombstone
// state. Its version vector decides causality against the local version; the local
// sequence number is assigned on commit, not carried from the peer.
type RemoteManifest struct {
	Manifest   manifest.Manifest
	ID         manifest.ID
	Version    manifest.VersionVector
	Author     string
	AuthoredAt time.Time
	Deleted    bool
	DeletedAt  time.Time
	Metadata   Metadata
}

// FolderEpoch returns this folder's stable epoch, allocating and persisting a random
// nonzero value on first call. The epoch identifies this node's sequence lineage so a peer
// can detect a rebuild and force a full resync.
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

// HighWater is the maximum manifest sequence currently stored, or zero if the folder is
// empty. It is this node's announce high-water.
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

// ApplyRemote reconciles a peer's delta into the local model. Under the folder's
// apply lock it resolves each incoming manifest against the local version (fast
// forward, ignore, or merge concurrent edits), invokes materialize so the caller can
// stage the winners to disk, then writes them with fresh local sequence numbers and
// advances the (folder, peer) cursor. Incoming version vectors are stored verbatim.
// materialize must durably place the winners before it returns, since the model
// commit follows it and is the point of no return.
func (s *Store) ApplyRemote(ctx context.Context, folderID, peerID string, epoch uint64, highWater int64, batch []RemoteManifest, materialize func(apply []RemoteManifest) error) error {
	notify := false
	err := func() error {
		s.applyMu.Lock()
		defer s.applyMu.Unlock()
		apply, err := s.resolveRemote(ctx, batch)
		if err != nil {
			return err
		}
		if len(apply) > 0 {
			if err := materialize(apply); err != nil {
				return err
			}
		}
		if err := s.commitRemote(ctx, folderID, peerID, epoch, highWater, apply); err != nil {
			return err
		}
		notify = len(apply) > 0
		return nil
	}()
	if err != nil {
		return err
	}
	if notify {
		s.notifyChange()
	}
	return nil
}

// resolveRemote returns the manifests to apply after comparing each to the local version.
// It only reads; the caller holds applyMu so local state is stable across resolve and commit.
func (s *Store) resolveRemote(ctx context.Context, batch []RemoteManifest) ([]RemoteManifest, error) {
	out := make([]RemoteManifest, 0, len(batch))
	for _, rm := range batch {
		rm.Manifest.Path = manifest.NormalizePath(rm.Manifest.Path)
		rm.Manifest.SymlinkTarget = manifest.NormalizePath(rm.Manifest.SymlinkTarget)
		if err := ValidateManifest(rm.Manifest); err != nil {
			return nil, err
		}
		if rm.Manifest.ID() != rm.ID {
			return nil, fmt.Errorf("%w: path %q", ErrCorruptModel, rm.Manifest.Path)
		}
		// The author is carried verbatim into a conflict copy's path, so a malformed one
		// must never reach ConflictPath.
		if !identity.ValidNodeID(rm.Author) {
			return nil, fmt.Errorf("%w: author %q for path %q", ErrInvalidManifest, rm.Author, rm.Manifest.Path)
		}
		local, ok, err := loadRow(ctx, s.db, rm.Manifest.Path)
		if err != nil {
			return nil, err
		}
		if !ok {
			out = append(out, rm)
			continue
		}
		switch local.Version.Compare(rm.Version) {
		case manifest.Less:
			out = append(out, rm)
		case manifest.Greater, manifest.Equal:
		case manifest.Concurrent:
			resolved, err := s.resolveConcurrent(ctx, local, rm)
			if err != nil {
				return nil, err
			}
			out = append(out, resolved...)
		}
	}
	return out, nil
}

// resolveConcurrent reconciles concurrent versions of a path: an edit beats a delete,
// identical content merges, otherwise the deterministic winner takes the path with the
// joined vector (dominating the loser so re-detection is idempotent) and a live loser is
// preserved as a conflict copy carrying its own vector verbatim.
func (s *Store) resolveConcurrent(ctx context.Context, local Record, rm RemoteManifest) ([]RemoteManifest, error) {
	joined := manifest.Join(local.Version, rm.Version)

	if local.Deleted != rm.Deleted {
		if rm.Deleted {
			winner, err := s.localAsRemote(ctx, local, joined)
			return []RemoteManifest{winner}, err
		}
		rm.Version = joined
		return []RemoteManifest{rm}, nil
	}

	if local.ID == rm.ID {
		rm.Version = joined
		if ConflictWinner(local.Author, local.AuthoredAt, rm.Author, rm.AuthoredAt) {
			rm.Author, rm.AuthoredAt = local.Author, local.AuthoredAt
		}
		return []RemoteManifest{rm}, nil
	}

	localRM, err := s.localAsRemote(ctx, local, local.Version)
	if err != nil {
		return nil, err
	}
	if ConflictWinner(rm.Author, rm.AuthoredAt, local.Author, local.AuthoredAt) {
		winner := rm
		winner.Version = joined
		return keepBoth(winner, localRM), nil
	}
	winner := localRM
	winner.Version = joined
	return keepBoth(winner, rm), nil
}

// keepBoth returns the winner plus, for a live loser, its conflict copy. A deleted loser
// (two concurrent deletes) leaves only the winning tombstone.
func keepBoth(winner, loser RemoteManifest) []RemoteManifest {
	out := []RemoteManifest{winner}
	if loser.Deleted {
		return out
	}
	cc := loser
	cc.Manifest.Path = ConflictPath(loser.Manifest.Path, loser.Author, loser.AuthoredAt)
	cc.ID = cc.Manifest.ID()
	out = append(out, cc)
	return out
}

func (s *Store) localAsRemote(ctx context.Context, local Record, vv manifest.VersionVector) (RemoteManifest, error) {
	chunks, err := loadChunks(ctx, s.db, local.ID)
	if err != nil {
		return RemoteManifest{}, err
	}
	m := local.Manifest
	m.Chunks = chunks
	return RemoteManifest{
		Manifest:   m,
		ID:         local.ID,
		Version:    vv,
		Author:     local.Author,
		AuthoredAt: local.AuthoredAt,
		Deleted:    local.Deleted,
		DeletedAt:  local.DeletedAt,
		Metadata:   local.Metadata,
	}, nil
}

func (s *Store) commitRemote(ctx context.Context, folderID, peerID string, epoch uint64, highWater int64, apply []RemoteManifest) error {
	return s.db.WithTx(ctx, func(tx *storage.Tx) error {
		for _, rm := range apply {
			seq, err := allocate(ctx, tx, counterManifestSeq)
			if err != nil {
				return err
			}
			if err := writeRow(ctx, tx, rm.Manifest, rm.Metadata, rm.ID, rm.Version, seq, rm.Author, rm.AuthoredAt, rm.Deleted, rm.DeletedAt); err != nil {
				return err
			}
			if err := writeChunks(ctx, tx, rm.ID, rm.Manifest.Chunks); err != nil {
				return err
			}
		}
		return saveCursor(ctx, tx, folderID, peerID, epoch, highWater)
	})
}

// AdvanceCursor records that this node consumed peer's lineage up to (epoch,
// highWater) with no manifest to apply, so an already-converged folder still records
// its progress.
func (s *Store) AdvanceCursor(ctx context.Context, folderID, peerID string, epoch uint64, highWater int64) error {
	s.applyMu.Lock()
	defer s.applyMu.Unlock()
	return s.db.WithTx(ctx, func(tx *storage.Tx) error {
		return saveCursor(ctx, tx, folderID, peerID, epoch, highWater)
	})
}

func saveCursor(ctx context.Context, tx *storage.Tx, folderID, peerID string, epoch uint64, highWater int64) error {
	if _, err := tx.Exec(ctx,
		`INSERT INTO peer_cursors (folder_id, peer_id, epoch, high_water, updated_ms)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(folder_id, peer_id) DO UPDATE SET
			epoch=excluded.epoch, high_water=excluded.high_water, updated_ms=excluded.updated_ms`,
		folderID, peerID, int64(epoch), highWater, time.Now().UnixMilli()); err != nil {
		return fmt.Errorf("model: save cursor: %w", err)
	}
	return nil
}

// LoadCursor returns the consumed (epoch, high_water) for a (folder, peer). ok is
// false if this node has never applied from that peer for that folder.
func (s *Store) LoadCursor(ctx context.Context, folderID, peerID string) (epoch uint64, highWater int64, ok bool, err error) {
	var ep int64
	err = s.db.QueryRow(ctx,
		`SELECT epoch, high_water FROM peer_cursors WHERE folder_id = ? AND peer_id = ?`,
		folderID, peerID).Scan(&ep, &highWater)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, 0, false, nil
	case err != nil:
		return 0, 0, false, fmt.Errorf("model: load cursor: %w", err)
	}
	return uint64(ep), highWater, true, nil
}
