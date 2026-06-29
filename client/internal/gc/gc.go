// Package gc reclaims chunks no longer reachable from the current folder state or
// any retained snapshot. Reclamation is mark-and-sweep: the model is the sole
// authority on reachability, and a per-chunk grace age
// over last_seen_ms protects chunks a concurrent ingest is in the middle of
// referencing — so the sweep needs no lock spanning the separate chunk and
// sync-state databases. Forgetting a snapshot is a separate, cheap model
// operation; sweeping is the expensive, infrequent reclaim.
package gc

import (
	"context"
	"fmt"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/chunkstore"
	"github.com/GhentiLabs/Trove/client/internal/hasher"
	"github.com/GhentiLabs/Trove/client/internal/model"
)

// DefaultGraceAge is how long an unreferenced chunk is kept before a sweep may
// delete it. It must exceed the longest plausible mark-to-sweep gap so a chunk
// being referenced by an in-flight ingest is never collected.
const DefaultGraceAge = time.Hour

// Result reports what a sweep reclaimed.
type Result struct {
	ChunksDeleted    int
	BlobsReclaimed   int
	ObjectsReclaimed int
}

// Sweep deletes every chunk that is unreachable from the model and was last seen
// before now minus graceAge, then reclaims any blob or clone object no longer
// backing a chunk. now is the mark point: the cutoff is now-graceAge, so any
// chunk touched (Put, even a dedup) after the mark survives. Sweeping is
// crash-safe — it only ever deletes provably-unreachable, past-grace chunks, in
// independent transactions, so an interrupted sweep just leaves slack for the next run.
func Sweep(ctx context.Context, m *model.Store, cs *chunkstore.Store, graceAge time.Duration, now time.Time) (Result, error) {
	reachable, err := m.ReachableChunkIDs(ctx)
	if err != nil {
		return Result{}, err
	}
	cutoff := now.Add(-graceAge).UnixMilli()

	victims := make([]hasher.ChunkID, 0, 64)
	for stat, err := range cs.IterChunks(ctx) {
		if err != nil {
			return Result{}, err
		}
		if _, live := reachable[stat.ID]; live {
			continue
		}
		if stat.LastSeen >= cutoff {
			continue // within grace: a recent or in-flight reference
		}
		victims = append(victims, stat.ID)
	}

	deleted, err := cs.DeleteChunks(ctx, victims, cutoff)
	if err != nil {
		return Result{}, fmt.Errorf("gc: delete chunks: %w", err)
	}
	// Reclaim blobs then clone objects, both after the grace-checked chunk delete,
	// so a backing is freed only once its last referencing chunk row is gone.
	blobs, err := cs.ReclaimBlobs(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("gc: reclaim blobs: %w", err)
	}
	objects, err := cs.ReclaimObjects(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("gc: reclaim objects: %w", err)
	}
	return Result{ChunksDeleted: deleted, BlobsReclaimed: blobs, ObjectsReclaimed: objects}, nil
}
