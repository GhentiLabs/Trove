package model

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/snapshot"
)

// Receipt records that peer reached snapshot Root at the owner's (Epoch, HighWater)
// as of SyncedAt.
type Receipt struct {
	PeerID    string
	Root      snapshot.Root
	Epoch     uint64
	HighWater int64
	SyncedAt  time.Time
}

// RecordReceipt upserts a convergence receipt for one peer.
func (s *Store) RecordReceipt(ctx context.Context, r Receipt) error {
	if _, err := s.db.Exec(ctx,
		`INSERT INTO sync_receipts (peer_id, snapshot_root, epoch, high_water, synced_ms)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(peer_id) DO UPDATE SET
			snapshot_root=excluded.snapshot_root, epoch=excluded.epoch,
			high_water=excluded.high_water, synced_ms=excluded.synced_ms`,
		r.PeerID, r.Root.Bytes(), int64(r.Epoch), r.HighWater, r.SyncedAt.UnixMilli()); err != nil {
		return fmt.Errorf("model: record receipt: %w", err)
	}
	return nil
}

// Receipt returns the stored receipt for peer; ok is false if none exists.
func (s *Store) Receipt(ctx context.Context, peerID string) (r Receipt, ok bool, err error) {
	row := s.db.QueryRow(ctx,
		`SELECT peer_id, snapshot_root, epoch, high_water, synced_ms FROM sync_receipts WHERE peer_id = ?`,
		peerID)
	r, err = scanReceipt(row)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return Receipt{}, false, nil
	case err != nil:
		return Receipt{}, false, err
	}
	return r, true, nil
}

// Receipts returns every stored receipt, ordered by peer.
func (s *Store) Receipts(ctx context.Context) ([]Receipt, error) {
	rows, err := s.db.Query(ctx,
		`SELECT peer_id, snapshot_root, epoch, high_water, synced_ms FROM sync_receipts ORDER BY peer_id`)
	if err != nil {
		return nil, fmt.Errorf("model: list receipts: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []Receipt
	for rows.Next() {
		r, err := scanReceipt(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ConvergedHighWater is the minimum high-water across receipts at epoch — the sequence
// every reporting replica has converged past. ok is false if no receipt is at epoch.
func (s *Store) ConvergedHighWater(ctx context.Context, epoch uint64) (hw int64, ok bool, err error) {
	row := s.db.QueryRow(ctx,
		`SELECT MIN(high_water) FROM sync_receipts WHERE epoch = ?`, int64(epoch))
	var min sql.NullInt64
	if err := row.Scan(&min); err != nil {
		return 0, false, fmt.Errorf("model: converged high water: %w", err)
	}
	if !min.Valid {
		return 0, false, nil
	}
	return min.Int64, true, nil
}

type scanner interface {
	Scan(dest ...any) error
}

func scanReceipt(sc scanner) (Receipt, error) {
	var (
		r       Receipt
		rawRoot []byte
		epoch   int64
		synced  int64
	)
	if err := sc.Scan(&r.PeerID, &rawRoot, &epoch, &r.HighWater, &synced); err != nil {
		return Receipt{}, err
	}
	root, err := snapshot.RootFromBytes(rawRoot)
	if err != nil {
		return Receipt{}, fmt.Errorf("model: receipt root: %w", err)
	}
	r.Root = root
	r.Epoch = uint64(epoch)
	r.SyncedAt = time.UnixMilli(synced)
	return r, nil
}
