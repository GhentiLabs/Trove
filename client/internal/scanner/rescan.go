package scanner

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/GhentiLabs/Trove/client/internal/hasher"
	"github.com/GhentiLabs/Trove/client/internal/model"
)

// Rescan does a full reconcile: it ingests every changed path (the stat fast path
// skips unchanged ones), then tombstones any live manifest whose path no longer
// exists on disk. It is the correctness backstop for changes the watcher missed —
// out-of-band edits, metadata-only changes, deletions — and the crash-recovery
// path that runs on startup.
func (s *Scanner) Rescan(ctx context.Context) error {
	if err := s.ScanAll(ctx); err != nil {
		return err
	}
	return s.detectDeletions(ctx)
}

// detectDeletions tombstones live manifests whose paths have vanished from disk.
// It streams the model's live paths rather than holding them in memory, and a
// transient stat error leaves a path alone rather than risking a false deletion.
func (s *Scanner) detectDeletions(ctx context.Context) error {
	var missing []string
	for path, err := range s.model.LivePaths(ctx) {
		if err != nil {
			return err
		}
		abs := filepath.Join(s.root, filepath.FromSlash(path))
		switch _, statErr := os.Lstat(abs); {
		case errors.Is(statErr, fs.ErrNotExist):
			missing = append(missing, path)
		case statErr != nil:
			s.log.Warn("rescan lstat", "path", path, "err", statErr)
		}
	}
	var superseded []hasher.ChunkID
	for _, path := range missing {
		old := s.priorChunks(ctx, path) // read before the tombstone clears the manifest
		if _, err := s.model.DeleteManifest(ctx, path); err != nil {
			if !errors.Is(err, model.ErrManifestNotFound) {
				s.log.Warn("rescan tombstone", "path", path, "err", err)
			}
			continue
		}
		superseded = append(superseded, old...)
	}
	s.promoteSuperseded(ctx, superseded)
	return ctx.Err()
}
