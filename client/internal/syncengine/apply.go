package syncengine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/GhentiLabs/Trove/client/internal/hasher"
	"github.com/GhentiLabs/Trove/client/internal/manifest"
	"github.com/GhentiLabs/Trove/client/internal/model"
	"github.com/GhentiLabs/Trove/client/internal/wire/wirepb"
)

// apply materializes a delta crash-safely: stage files under tmpDirName, rename into
// place, then commit the model. The destination is never written directly, and a
// crash before the commit re-applies idempotently on restart.
func (fs *folderState) apply(ctx context.Context, batch []model.RemoteManifest, delta *wirepb.ManifestDelta) error {
	stage := filepath.Join(fs.cfg.Root, tmpDirName)
	_ = os.RemoveAll(stage) // discard any debris from a previous failed attempt
	if err := os.MkdirAll(stage, 0o700); err != nil {
		return fmt.Errorf("syncengine: stage dir: %w", err)
	}

	staged := make(map[string]string, len(batch))
	for i, rm := range batch {
		if rm.Deleted || rm.Manifest.Kind != manifest.KindRegular {
			continue
		}
		sp := filepath.Join(stage, fmt.Sprintf("f%d", i))
		if err := fs.stageFile(ctx, sp, rm); err != nil {
			return err
		}
		staged[rm.Manifest.Path] = sp
	}

	parents := make(map[string]struct{}, len(batch))
	for _, rm := range batch {
		if err := fs.materialize(rm, staged); err != nil {
			return err
		}
		parents[filepath.Dir(filepath.Join(fs.cfg.Root, filepath.FromSlash(rm.Manifest.Path)))] = struct{}{}
	}
	// Fsync touched directories so the renames are durable before the model commit
	// makes them visible; otherwise a power loss could lose a converged file.
	for dir := range parents {
		if err := syncDir(dir); err != nil {
			return err
		}
	}

	if err := fs.cfg.Model.ApplyRemoteAndAdvance(ctx, batch, fs.cfg.FolderID, fs.eng.sess.PeerNodeID(), delta.GetIndexEpochId(), delta.GetHighWaterSequence()); err != nil {
		return err
	}
	_ = os.RemoveAll(stage)
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("syncengine: open dir %q: %w", dir, err)
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return fmt.Errorf("syncengine: fsync dir %q: %w", dir, err)
	}
	return d.Close()
}

func (fs *folderState) stageFile(ctx context.Context, path string, rm model.RemoteManifest) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("syncengine: create temp: %w", err)
	}
	if err := fs.cfg.Chunks.Reassemble(ctx, fs.cfg.FolderCtx, chunkIDs(rm.Manifest.Chunks), f); err != nil {
		_ = f.Close()
		return fmt.Errorf("syncengine: reassemble %q: %w", rm.Manifest.Path, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("syncengine: fsync %q: %w", rm.Manifest.Path, err)
	}
	if err := f.Chmod(fileMode(rm.Manifest.Mode)); err != nil {
		_ = f.Close()
		return fmt.Errorf("syncengine: chmod %q: %w", rm.Manifest.Path, err)
	}
	return f.Close()
}

func (fs *folderState) materialize(rm model.RemoteManifest, staged map[string]string) error {
	dest := filepath.Join(fs.cfg.Root, filepath.FromSlash(rm.Manifest.Path))
	if rm.Deleted {
		if err := os.Remove(dest); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("syncengine: remove %q: %w", rm.Manifest.Path, err)
		}
		return nil
	}
	switch rm.Manifest.Kind {
	case manifest.KindDir:
		if err := os.MkdirAll(dest, fileMode(rm.Manifest.Mode)); err != nil {
			return fmt.Errorf("syncengine: mkdir %q: %w", rm.Manifest.Path, err)
		}
		return nil
	case manifest.KindSymlink:
		return materializeSymlink(dest, rm.Manifest.SymlinkTarget)
	case manifest.KindRegular:
		if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
			return fmt.Errorf("syncengine: mkdir parent of %q: %w", rm.Manifest.Path, err)
		}
		if err := os.Rename(staged[rm.Manifest.Path], dest); err != nil {
			return fmt.Errorf("syncengine: rename %q: %w", rm.Manifest.Path, err)
		}
		return nil
	}
	return nil
}

func materializeSymlink(dest, target string) error {
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return fmt.Errorf("syncengine: mkdir parent of %q: %w", dest, err)
	}
	tmp := dest + ".trovelink"
	_ = os.Remove(tmp)
	if err := os.Symlink(target, tmp); err != nil {
		return fmt.Errorf("syncengine: symlink %q: %w", dest, err)
	}
	if err := os.Rename(tmp, dest); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("syncengine: rename symlink %q: %w", dest, err)
	}
	return nil
}

func chunkIDs(refs []manifest.ChunkRef) []hasher.ChunkID {
	ids := make([]hasher.ChunkID, len(refs))
	for i, r := range refs {
		ids[i] = r.ID
	}
	return ids
}

func fileMode(mode uint32) os.FileMode {
	return os.FileMode(mode).Perm()
}
