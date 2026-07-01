package model

import (
	"context"
	"errors"
)

// ErrQuotaExceeded reports that a folder's current data alone exceeds its quota.
var ErrQuotaExceeded = errors.New("model: folder quota exceeded")

// ReachableLogicalBytes sums the plaintext length of the distinct chunks the current
// manifests or any retained snapshot reference: the folder's usage against its quota.
func (s *Store) ReachableLogicalBytes(ctx context.Context) (int64, error) {
	return s.logicalBytesFor(ctx, reachableManifests)
}

// PruneHistoryToFit forgets retained snapshots oldest-first until the folder fits its
// quota, run after a write so it measures committed state. It never evicts current files,
// so it returns ErrQuotaExceeded when current data alone exceeds the quota. A no-op when
// the quota is unlimited (<= 0) or the data already fits.
func (s *Store) PruneHistoryToFit(ctx context.Context) error {
	if s.quota <= 0 {
		return nil
	}
	used, err := s.ReachableLogicalBytes(ctx)
	if err != nil {
		return err
	}
	if used <= s.quota {
		return nil
	}
	snaps, err := s.ListSnapshots(ctx)
	if err != nil {
		return err
	}
	for _, snap := range snaps {
		// Forget is root-keyed, so a recurring root or a concurrent prune may already be gone.
		if err := s.Forget(ctx, snap.Root); err != nil && !errors.Is(err, ErrSnapshotNotFound) {
			return err
		}
		used, err = s.ReachableLogicalBytes(ctx)
		if err != nil {
			return err
		}
		if used <= s.quota {
			return nil
		}
	}
	return ErrQuotaExceeded
}
