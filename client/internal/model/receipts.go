package model

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/snapshot"
)

// ReceiptKind is the direction of a convergence receipt: a peer acking this node's lineage
// (InboundAck) or this node acking the peer's (LocalSync). A node holds both per peer.
type ReceiptKind int

const (
	// InboundAck records that a peer reported converging this node's lineage. It drives
	// the tombstone-reaping gate.
	InboundAck ReceiptKind = iota
	// LocalSync records that this node converged a peer's lineage. It drives "last synced"
	// reporting.
	LocalSync
)

// Receipt records that peer reached snapshot Root at (Epoch, HighWater) of the acked
// lineage as of SyncedAt.
type Receipt struct {
	PeerID    string
	Root      snapshot.Root
	Epoch     uint64
	HighWater int64
	SyncedAt  time.Time
}

// RecordReceipt upserts a convergence receipt of the given kind for one peer.
func (s *Store) RecordReceipt(ctx context.Context, kind ReceiptKind, r Receipt) error {
	if _, err := s.db.Exec(ctx,
		`INSERT INTO sync_receipts (peer_id, direction, snapshot_root, epoch, high_water, synced_ms)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT(peer_id, direction) DO UPDATE SET
			snapshot_root=excluded.snapshot_root, epoch=excluded.epoch,
			high_water=excluded.high_water, synced_ms=excluded.synced_ms`,
		r.PeerID, int(kind), r.Root.Bytes(), int64(r.Epoch), r.HighWater, r.SyncedAt.UnixMilli()); err != nil {
		return fmt.Errorf("model: record receipt: %w", err)
	}
	return nil
}

// Receipt returns the stored receipt of kind for peer; ok is false if none exists.
func (s *Store) Receipt(ctx context.Context, kind ReceiptKind, peerID string) (r Receipt, ok bool, err error) {
	row := s.db.QueryRow(ctx,
		`SELECT peer_id, snapshot_root, epoch, high_water, synced_ms FROM sync_receipts WHERE peer_id = ? AND direction = ?`,
		peerID, int(kind))
	r, err = scanReceipt(row)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return Receipt{}, false, nil
	case err != nil:
		return Receipt{}, false, err
	}
	return r, true, nil
}

// Receipts returns every stored receipt of kind, ordered by peer.
func (s *Store) Receipts(ctx context.Context, kind ReceiptKind) ([]Receipt, error) {
	rows, err := s.db.Query(ctx,
		`SELECT peer_id, snapshot_root, epoch, high_water, synced_ms FROM sync_receipts WHERE direction = ? ORDER BY peer_id`,
		int(kind))
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

// ConvergedHighWater is the minimum high-water that every member has acked converging
// past at epoch — the tombstone-reaping gate. members is the set of peers that could still
// hold the deleted file (the roster minus this node). It returns (0, true) to block reaping
// while any member has not confirmed at the current epoch (never connected, or stale since
// an epoch rebuild), so a partitioned member cannot resurrect a reaped delete. ok is false
// only when there are no members at all, so the caller falls back to retention alone.
func (s *Store) ConvergedHighWater(ctx context.Context, epoch uint64, members []string) (hw int64, ok bool, err error) {
	if len(members) == 0 {
		return 0, false, nil
	}
	hw = math.MaxInt64
	for _, m := range members {
		r, ok, err := s.Receipt(ctx, InboundAck, m)
		if err != nil {
			return 0, false, err
		}
		if !ok || r.Epoch != epoch {
			return 0, true, nil
		}
		hw = min(hw, r.HighWater)
	}
	return hw, true, nil
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
