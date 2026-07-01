package model

import (
	"context"
	"fmt"
	"strings"

	"github.com/GhentiLabs/Trove/client/internal/hasher"
)

// reachableManifests is the set of manifest ids that must keep their chunks: every
// live (non-tombstone) current manifest, plus every live leaf of a retained
// snapshot. Tombstones do not keep content; a deleted file's chunks survive only
// while a snapshot still holds a live version of it.
const reachableManifests = `SELECT manifest_id FROM manifests WHERE deleted = 0
                            UNION
                            SELECT manifest_id FROM snapshot_manifests WHERE deleted = 0`

// ReachableChunkIDs returns the set of chunk ids reachable from the current state
// and all retained snapshots — the live set the garbage collector must never
// sweep.
func (s *Store) ReachableChunkIDs(ctx context.Context) (map[hasher.ChunkID]struct{}, error) {
	rows, err := s.db.Query(ctx,
		`SELECT DISTINCT chunk_id FROM manifest_chunks WHERE manifest_id IN (`+reachableManifests+`)`)
	if err != nil {
		return nil, fmt.Errorf("model: reachable chunks: %w", err)
	}
	defer func() { _ = rows.Close() }()

	set := make(map[hasher.ChunkID]struct{})
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("model: scan chunk id: %w", err)
		}
		id, err := hasher.FromBytes(raw)
		if err != nil {
			return nil, fmt.Errorf("model: chunk id: %w", err)
		}
		set[id] = struct{}{}
	}
	return set, rows.Err()
}

// SupersededHistory returns the subset of candidates that a retained snapshot still
// references but no current live manifest does: chunks that have just become history
// and can be moved out of a clone into deduplicated storage.
func (s *Store) SupersededHistory(ctx context.Context, candidates []hasher.ChunkID) ([]hasher.ChunkID, error) {
	out := make([]hasher.ChunkID, 0, len(candidates))
	const batch = 900 // under SQLite's default bound-parameter limit
	for start := 0; start < len(candidates); start += batch {
		group := candidates[start:min(start+batch, len(candidates))]
		args := make([]any, len(group))
		for i, id := range group {
			args[i] = id.Bytes()
		}
		q := `SELECT DISTINCT chunk_id FROM manifest_chunks
		      WHERE chunk_id IN (` + strings.Repeat("?,", len(group)-1) + `?)
		        AND manifest_id IN (SELECT manifest_id FROM snapshot_manifests WHERE deleted = 0)
		        AND chunk_id NOT IN (
		          SELECT chunk_id FROM manifest_chunks
		          WHERE manifest_id IN (SELECT manifest_id FROM manifests WHERE deleted = 0))`
		rows, err := s.db.Query(ctx, q, args...)
		if err != nil {
			return nil, fmt.Errorf("model: superseded history: %w", err)
		}
		err = func() error {
			defer func() { _ = rows.Close() }()
			for rows.Next() {
				var raw []byte
				if err := rows.Scan(&raw); err != nil {
					return err
				}
				id, err := hasher.FromBytes(raw)
				if err != nil {
					return err
				}
				out = append(out, id)
			}
			return rows.Err()
		}()
		if err != nil {
			return nil, fmt.Errorf("model: superseded history: %w", err)
		}
	}
	return out, nil
}

// LogicalBytes is the folder's accounted size: the summed plaintext length of the
// distinct chunks referenced by the current live manifests. Deduplicated content
// is counted once.
func (s *Store) LogicalBytes(ctx context.Context) (int64, error) {
	return s.logicalBytesFor(ctx, `SELECT manifest_id FROM manifests WHERE deleted = 0`)
}

// logicalBytesFor sums the distinct plaintext chunk lengths the selected manifests reference.
func (s *Store) logicalBytesFor(ctx context.Context, manifestIDs string) (int64, error) {
	var total int64
	err := s.db.QueryRow(ctx,
		`SELECT COALESCE(SUM(length), 0) FROM (
			SELECT length FROM manifest_chunks
			WHERE manifest_id IN (`+manifestIDs+`)
			GROUP BY chunk_id
		)`).Scan(&total)
	if err != nil {
		return 0, fmt.Errorf("model: logical bytes: %w", err)
	}
	return total, nil
}
