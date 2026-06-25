package syncengine

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/GhentiLabs/Trove/client/internal/manifest"
)

// RepairFolder re-materializes a replica folder's tracked files that are missing on
// disk, reassembling them from the local chunk store (a replica backs every chunk it
// references, so repair needs no network). Existing files are left as-is. It is
// best-effort: a file it cannot restore is logged and skipped; only enumeration fails.
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
