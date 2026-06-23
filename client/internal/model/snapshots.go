package model

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/manifest"
	"github.com/GhentiLabs/Trove/client/internal/snapshot"
	"github.com/GhentiLabs/Trove/client/internal/storage"
)

// Snapshot is a retained folder state: its content root, the parent it descends
// from, when and by whom it was cut, and its manifest leaf set.
type Snapshot struct {
	Root snapshot.Root
	// Parent is the root this snapshot descends from, or the zero Root for the
	// first snapshot (the empty-set root is nonzero, so there is no ambiguity).
	Parent    snapshot.Root
	Seq       int64
	CreatedAt time.Time
	CreatedBy string
	Leaves    []snapshot.Leaf
}

// Cut records the current folder state as an immutable snapshot and returns its
// root. If the state is identical to the most recent snapshot, no new snapshot is
// created and that root is returned.
func (s *Store) Cut(ctx context.Context) (snapshot.Root, error) {
	var root snapshot.Root
	err := s.db.WithTx(ctx, func(tx *storage.Tx) error {
		leaves, err := currentLeaves(ctx, tx)
		if err != nil {
			return err
		}
		root = snapshot.Set(leaves).Root()

		parentSeq, parentRoot, hasParent, err := latestSnapshot(ctx, tx)
		if err != nil {
			return err
		}
		if hasParent && parentRoot == root {
			return nil
		}

		snapSeq, err := allocate(ctx, tx, counterSnapshotSeq)
		if err != nil {
			return err
		}
		var parent sql.NullInt64
		if hasParent {
			parent = sql.NullInt64{Int64: parentSeq, Valid: true}
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO snapshots (snap_seq, root_hash, parent_seq, created_ms, created_by, manifest_n)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			snapSeq, root.Bytes(), parent, time.Now().UnixMilli(), s.node, len(leaves)); err != nil {
			return fmt.Errorf("model: write snapshot: %w", err)
		}
		for _, l := range leaves {
			if _, err := tx.Exec(ctx,
				`INSERT INTO snapshot_manifests (snap_seq, path, manifest_id, deleted) VALUES (?, ?, ?, ?)`,
				snapSeq, l.Path, l.ManifestID.Bytes(), l.Deleted); err != nil {
				return fmt.Errorf("model: write snapshot leaf: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return snapshot.Root{}, err
	}
	return root, nil
}

// GetSnapshot returns the snapshot with the given root. If a state recurred (the
// same root was cut more than once), the most recent occurrence is returned.
func (s *Store) GetSnapshot(ctx context.Context, root snapshot.Root) (Snapshot, error) {
	var snap Snapshot
	err := s.db.WithTx(ctx, func(tx *storage.Tx) error {
		var err error
		snap, err = loadSnapshot(ctx, tx, root)
		return err
	})
	if err != nil {
		return Snapshot{}, err
	}
	return snap, nil
}

func loadSnapshot(ctx context.Context, q querier, root snapshot.Root) (Snapshot, error) {
	var (
		snap      Snapshot
		parentSeq sql.NullInt64
		createdMs int64
	)
	err := q.QueryRow(ctx,
		`SELECT snap_seq, parent_seq, created_ms, created_by FROM snapshots WHERE root_hash = ? ORDER BY snap_seq DESC LIMIT 1`,
		root.Bytes()).Scan(&snap.Seq, &parentSeq, &createdMs, &snap.CreatedBy)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return Snapshot{}, ErrSnapshotNotFound
	case err != nil:
		return Snapshot{}, fmt.Errorf("model: load snapshot: %w", err)
	}
	snap.Root = root
	snap.CreatedAt = time.UnixMilli(createdMs)
	if parentSeq.Valid {
		pr, err := rootOfSeq(ctx, q, parentSeq.Int64)
		if err != nil {
			return Snapshot{}, err
		}
		snap.Parent = pr
	}
	leaves, err := leavesOfSeq(ctx, q, snap.Seq)
	if err != nil {
		return Snapshot{}, err
	}
	snap.Leaves = leaves
	return snap, nil
}

// ListSnapshots returns every snapshot's header (without its leaf set) ordered
// oldest to newest. Use GetSnapshot for a snapshot's manifests.
func (s *Store) ListSnapshots(ctx context.Context) ([]Snapshot, error) {
	rows, err := s.db.Query(ctx,
		`SELECT s.snap_seq, s.root_hash, p.root_hash, s.created_ms, s.created_by
		 FROM snapshots s LEFT JOIN snapshots p ON s.parent_seq = p.snap_seq
		 ORDER BY s.snap_seq`)
	if err != nil {
		return nil, fmt.Errorf("model: list snapshots: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []Snapshot
	for rows.Next() {
		var (
			snap      Snapshot
			rootRaw   []byte
			parentRaw []byte
			createdMs int64
		)
		if err := rows.Scan(&snap.Seq, &rootRaw, &parentRaw, &createdMs, &snap.CreatedBy); err != nil {
			return nil, fmt.Errorf("model: scan snapshot: %w", err)
		}
		if snap.Root, err = snapshot.RootFromBytes(rootRaw); err != nil {
			return nil, err
		}
		if parentRaw != nil {
			if snap.Parent, err = snapshot.RootFromBytes(parentRaw); err != nil {
				return nil, err
			}
		}
		snap.CreatedAt = time.UnixMilli(createdMs)
		out = append(out, snap)
	}
	return out, rows.Err()
}

// DiffSnapshots reports the changes from snapshot a to snapshot b.
func (s *Store) DiffSnapshots(ctx context.Context, a, b snapshot.Root) (snapshot.DiffResult, error) {
	var d snapshot.DiffResult
	err := s.db.WithTx(ctx, func(tx *storage.Tx) error {
		sa, err := leavesOfRoot(ctx, tx, a)
		if err != nil {
			return err
		}
		sb, err := leavesOfRoot(ctx, tx, b)
		if err != nil {
			return err
		}
		d = snapshot.Diff(sa, sb)
		return nil
	})
	if err != nil {
		return snapshot.DiffResult{}, err
	}
	return d, nil
}

// Forget drops a retained snapshot (and any others sharing its root), making the
// chunks it alone pinned eligible for the next sweep. It returns
// ErrSnapshotNotFound if no snapshot has that root.
func (s *Store) Forget(ctx context.Context, root snapshot.Root) error {
	return s.db.WithTx(ctx, func(tx *storage.Tx) error {
		if _, err := tx.Exec(ctx,
			`DELETE FROM snapshot_manifests WHERE snap_seq IN (SELECT snap_seq FROM snapshots WHERE root_hash = ?)`,
			root.Bytes()); err != nil {
			return fmt.Errorf("model: forget leaves: %w", err)
		}
		res, err := tx.Exec(ctx, `DELETE FROM snapshots WHERE root_hash = ?`, root.Bytes())
		if err != nil {
			return fmt.Errorf("model: forget snapshot: %w", err)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return ErrSnapshotNotFound
		}
		return nil
	})
}

// SweepTombstones removes tombstones whose retention has expired as of now,
// dropping them from the current state. Retained snapshots keep their historical
// record of the deletion. It returns the number of tombstones removed.
func (s *Store) SweepTombstones(ctx context.Context, now time.Time) (int, error) {
	var n int64
	err := s.db.WithTx(ctx, func(tx *storage.Tx) error {
		res, err := tx.Exec(ctx,
			`DELETE FROM manifests WHERE deleted = 1 AND expires_ms IS NOT NULL AND expires_ms <= ?`,
			now.UnixMilli())
		if err != nil {
			return fmt.Errorf("model: sweep tombstones: %w", err)
		}
		n, _ = res.RowsAffected()
		return nil
	})
	return int(n), err
}

func currentLeaves(ctx context.Context, q querier) (snapshot.Set, error) {
	return scanLeaves(ctx, q, `SELECT path, manifest_id, deleted FROM manifests ORDER BY path`)
}

func leavesOfSeq(ctx context.Context, q querier, seq int64) (snapshot.Set, error) {
	return scanLeaves(ctx, q, `SELECT path, manifest_id, deleted FROM snapshot_manifests WHERE snap_seq = ? ORDER BY path`, seq)
}

func leavesOfRoot(ctx context.Context, q querier, root snapshot.Root) (snapshot.Set, error) {
	var seq int64
	err := q.QueryRow(ctx, `SELECT snap_seq FROM snapshots WHERE root_hash = ? ORDER BY snap_seq DESC LIMIT 1`, root.Bytes()).Scan(&seq)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, ErrSnapshotNotFound
	case err != nil:
		return nil, fmt.Errorf("model: resolve snapshot: %w", err)
	}
	return leavesOfSeq(ctx, q, seq)
}

func scanLeaves(ctx context.Context, q querier, query string, args ...any) (snapshot.Set, error) {
	rows, err := q.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("model: load leaves: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out snapshot.Set
	for rows.Next() {
		var (
			path    string
			idRaw   []byte
			deleted bool
		)
		if err := rows.Scan(&path, &idRaw, &deleted); err != nil {
			return nil, fmt.Errorf("model: scan leaf: %w", err)
		}
		id, err := manifest.IDFromBytes(idRaw)
		if err != nil {
			return nil, fmt.Errorf("model: scan leaf id: %w", err)
		}
		out = append(out, snapshot.Leaf{Path: path, ManifestID: id, Deleted: deleted})
	}
	return out, rows.Err()
}

func latestSnapshot(ctx context.Context, q querier) (seq int64, root snapshot.Root, ok bool, err error) {
	var raw []byte
	err = q.QueryRow(ctx, `SELECT snap_seq, root_hash FROM snapshots ORDER BY snap_seq DESC LIMIT 1`).Scan(&seq, &raw)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, snapshot.Root{}, false, nil
	case err != nil:
		return 0, snapshot.Root{}, false, fmt.Errorf("model: latest snapshot: %w", err)
	}
	root, err = snapshot.RootFromBytes(raw)
	if err != nil {
		return 0, snapshot.Root{}, false, err
	}
	return seq, root, true, nil
}

func rootOfSeq(ctx context.Context, q querier, seq int64) (snapshot.Root, error) {
	var raw []byte
	if err := q.QueryRow(ctx, `SELECT root_hash FROM snapshots WHERE snap_seq = ?`, seq).Scan(&raw); err != nil {
		return snapshot.Root{}, fmt.Errorf("model: resolve parent: %w", err)
	}
	return snapshot.RootFromBytes(raw)
}
