package syncengine

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/GhentiLabs/Trove/client/internal/manifest"
)

// RepairFolder re-materializes any of a replica folder's tracked files that have gone
// missing on disk, sourcing bytes from the local chunk store. A replica physically
// backs every chunk it references, so repair never touches the network. It runs once at
// startup so a file deleted out-of-band under the replica is restored without waiting
// for the owner's next delta.
//
// Existing regular files are left untouched: their content is trusted here and a
// corruption is instead caught by the hash-verified pull path. Repair is best-effort —
// a single file that cannot be restored is logged and skipped rather than blocking
// startup; only an enumeration failure is returned.
func RepairFolder(ctx context.Context, cfg FolderConfig, log *slog.Logger) error {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	recs, err := cfg.Model.ListManifests(ctx)
	if err != nil {
		return fmt.Errorf("syncengine: repair list manifests: %w", err)
	}
	stage := stageDir(cfg.Root, "repair")
	_ = os.RemoveAll(stage)
	defer func() { _ = os.RemoveAll(stage) }()

	repaired := 0
	for _, rec := range recs {
		if rec.Deleted {
			continue
		}
		dest, err := resolveDest(cfg.Root, rec.Manifest.Path)
		if err != nil {
			// Our own model should never hold an escaping path; skip defensively.
			log.Warn("syncengine: repair skip bad path", "folder", cfg.FolderID, "path", rec.Manifest.Path, "err", err)
			continue
		}
		ok, err := repairOne(ctx, cfg, stage, rec.Manifest, dest)
		if err != nil {
			log.Warn("syncengine: repair file", "folder", cfg.FolderID, "path", rec.Manifest.Path, "err", err)
			continue
		}
		if ok {
			repaired++
		}
	}
	if repaired > 0 {
		log.Info("syncengine: repaired files from local chunks", "folder", cfg.FolderID, "count", repaired)
	}
	return nil
}

// repairOne restores one manifest's on-disk form if it is missing, returning whether it
// did any work.
func repairOne(ctx context.Context, cfg FolderConfig, stage string, m manifest.Manifest, dest string) (bool, error) {
	switch m.Kind {
	case manifest.KindDir:
		if isDir(dest) {
			return false, nil
		}
		if err := os.MkdirAll(dest, fileMode(m.Mode)); err != nil {
			return false, fmt.Errorf("mkdir: %w", err)
		}
		return true, nil
	case manifest.KindSymlink:
		if linkTargetIs(dest, m.SymlinkTarget) {
			return false, nil
		}
		if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
			return false, fmt.Errorf("mkdir parent: %w", err)
		}
		if err := materializeSymlink(dest, m.SymlinkTarget); err != nil {
			return false, err
		}
		return true, nil
	case manifest.KindRegular:
		if isRegular(dest) {
			return false, nil
		}
		if err := os.MkdirAll(stage, 0o700); err != nil {
			return false, fmt.Errorf("stage dir: %w", err)
		}
		sp := filepath.Join(stage, "f")
		if err := stageRegular(ctx, cfg.Chunks, cfg.FolderCtx, sp, m); err != nil {
			return false, err
		}
		if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
			return false, fmt.Errorf("mkdir parent: %w", err)
		}
		if err := os.Rename(sp, dest); err != nil {
			return false, fmt.Errorf("rename: %w", err)
		}
		return true, nil
	}
	return false, nil
}

func isDir(path string) bool {
	fi, err := os.Lstat(path)
	return err == nil && fi.IsDir()
}

func isRegular(path string) bool {
	fi, err := os.Lstat(path)
	return err == nil && fi.Mode().IsRegular()
}

func linkTargetIs(path, want string) bool {
	got, err := os.Readlink(path)
	return err == nil && got == want
}
