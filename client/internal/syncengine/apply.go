package syncengine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/GhentiLabs/Trove/client/internal/chunkstore"
	"github.com/GhentiLabs/Trove/client/internal/hasher"
	"github.com/GhentiLabs/Trove/client/internal/manifest"
	"github.com/GhentiLabs/Trove/client/internal/model"
	"github.com/GhentiLabs/Trove/client/internal/wire/wirepb"
)

// apply reconciles a delta: the model resolves the incoming batch against local
// versions, this stages the winners under tmpDirName and renames them into place,
// then the model commits and advances the cursor. The destination is never written
// directly, and a crash before the commit re-applies idempotently on restart.
func (fs *folderState) apply(ctx context.Context, batch []model.RemoteManifest, delta *wirepb.ManifestDelta) error {
	batch = fs.writerAuthored(ctx, batch)
	var superseded []hasher.ChunkID
	err := fs.cfg.Model.ApplyRemote(ctx, fs.cfg.FolderID, fs.eng.sess.PeerNodeID(),
		delta.GetIndexEpochId(), delta.GetHighWaterSequence(), batch, func(apply []model.RemoteManifest) error {
			superseded = fs.supersededChunks(ctx, apply)
			return fs.materializeBatch(ctx, apply)
		})
	if err != nil {
		return err
	}
	if err := fs.cfg.Model.PruneHistoryToFit(ctx); err != nil && !errors.Is(err, model.ErrQuotaExceeded) {
		fs.eng.log.Warn("syncengine: prune history to fit quota", "folder", fs.cfg.FolderID, "err", err)
	}
	// Best-effort: a crash here leaves the superseded chunks as clones until their
	// snapshot is forgotten and GC reclaims them; the apply does not re-run.
	fs.promoteSuperseded(ctx, superseded)
	return nil
}

// supersededChunks collects the chunk ids the applied versions drop from the prior
// local versions of their paths. Read inside the apply callback, before the model
// commits, so GetManifest still returns the versions being replaced. A deletion
// drops all of the prior version's chunks (its tombstone still carries them).
func (fs *folderState) supersededChunks(ctx context.Context, apply []model.RemoteManifest) []hasher.ChunkID {
	var out []hasher.ChunkID
	for _, rm := range apply {
		rec, err := fs.cfg.Model.GetManifest(ctx, rm.Manifest.Path)
		switch {
		case errors.Is(err, model.ErrManifestNotFound):
			continue
		case err != nil:
			fs.eng.log.Warn("syncengine: prior manifest", "path", rm.Manifest.Path, "err", err)
			continue
		}
		var newChunks []hasher.ChunkID
		if !rm.Deleted {
			newChunks = chunkIDs(rm.Manifest.Chunks)
		}
		out = append(out, hasher.SetMinus(chunkIDs(rec.Manifest.Chunks), newChunks)...)
	}
	return out
}

// promoteSuperseded moves the superseded chunks a retained snapshot still keeps out
// of their clones into deduplicated history (sealed when the folder is encrypted).
func (fs *folderState) promoteSuperseded(ctx context.Context, superseded []hasher.ChunkID) {
	if len(superseded) == 0 {
		return
	}
	history, err := fs.cfg.Model.SupersededHistory(ctx, superseded)
	if err != nil {
		fs.eng.log.Warn("syncengine: superseded history", "err", err)
		return
	}
	if len(history) == 0 {
		return
	}
	if _, err := fs.cfg.Chunks.Promote(ctx, fs.cfg.FolderCtx, history); err != nil {
		fs.eng.log.Warn("syncengine: promote history", "err", err)
	}
}

// writerAuthored drops every manifest whose author lacks write access per the roster,
// failing closed on a lookup error.
func (fs *folderState) writerAuthored(ctx context.Context, batch []model.RemoteManifest) []model.RemoteManifest {
	if fs.cfg.AuthorWriter == nil {
		return batch
	}
	out := make([]model.RemoteManifest, 0, len(batch))
	for _, rm := range batch {
		ok, err := fs.cfg.AuthorWriter(ctx, rm.Author)
		if err != nil {
			fs.eng.log.Warn("syncengine: author check failed, rejecting", "folder", fs.cfg.FolderID, "author", rm.Author, "err", err)
			continue
		}
		if !ok {
			fs.eng.log.Warn("syncengine: rejecting manifest from non-writer", "folder", fs.cfg.FolderID, "author", rm.Author, "path", rm.Manifest.Path)
			continue
		}
		out = append(out, rm)
	}
	return out
}

func (fs *folderState) materializeBatch(ctx context.Context, batch []model.RemoteManifest) error {
	// Validate the whole batch against the folder boundary before touching the
	// filesystem: a hostile peer must not be able to delete the root, escape it, or
	// plant an escaping symlink. The model commit re-validates, but only after the disk
	// is mutated, so this guard has to run first.
	dests := make([]string, len(batch))
	for i, rm := range batch {
		dest, err := fs.destPath(rm.Manifest.Path)
		if err != nil {
			return err
		}
		if !rm.Deleted {
			if err := model.ValidateManifest(rm.Manifest); err != nil {
				return fmt.Errorf("syncengine: reject manifest: %w", err)
			}
		}
		dests[i] = dest
	}

	stage := fs.stageDir
	_ = os.RemoveAll(stage)
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
	for i, rm := range batch {
		dest := dests[i]
		if err := fs.materialize(rm, dest, staged); err != nil {
			return err
		}
		parents[filepath.Dir(dest)] = struct{}{}
	}
	for dir := range parents {
		if err := syncDir(dir); err != nil {
			return err
		}
	}

	// Clone each materialized file so current data settles to ~1x; the pulled chunks
	// are then unreferenced and GC reclaims them. Best-effort: on failure the file
	// stays servable from those chunks.
	if !fs.cfg.SkipClone {
		for i, rm := range batch {
			if rm.Deleted || rm.Manifest.Kind != manifest.KindRegular {
				continue
			}
			if _, err := fs.cfg.Chunks.IngestClone(ctx, dests[i]); err != nil {
				fs.eng.log.Warn("syncengine: clone materialized file", "folder", fs.cfg.FolderID, "path", rm.Manifest.Path, "err", err)
			}
		}
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
	return stageRegular(ctx, fs.cfg.Chunks, fs.cfg.FolderCtx, path, rm.Manifest)
}

// stageRegular reassembles m's chunks into a fresh, fsynced staging file at path with
// m's mode, for the caller to rename into place.
func stageRegular(ctx context.Context, chunks *chunkstore.Store, fc chunkstore.FolderContext, path string, m manifest.Manifest) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("syncengine: create temp: %w", err)
	}
	if err := chunks.Reassemble(ctx, fc, chunkIDs(m.Chunks), f); err != nil {
		_ = f.Close()
		return fmt.Errorf("syncengine: reassemble %q: %w", m.Path, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("syncengine: fsync %q: %w", m.Path, err)
	}
	if err := f.Chmod(fileMode(m.Mode)); err != nil {
		_ = f.Close()
		return fmt.Errorf("syncengine: chmod %q: %w", m.Path, err)
	}
	return f.Close()
}

// destPath resolves a folder-relative path to an absolute path under the folder root,
// rejecting any path that escapes the root (defends against a hostile owner on every OS).
func (fs *folderState) destPath(rel string) (string, error) {
	return resolveDest(fs.cfg.Root, rel)
}

func resolveDest(root, rel string) (string, error) {
	p := filepath.Join(root, filepath.FromSlash(rel))
	r, err := filepath.Rel(root, p)
	if err != nil || r == "." || r == ".." || strings.HasPrefix(r, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("syncengine: path escapes folder root: %q", rel)
	}
	return p, nil
}

// clearTypeConflict removes an existing entry at dest whose kind (dir vs non-dir)
// differs from the target, so a file→dir or dir→file change at a path applies cleanly
// instead of failing MkdirAll/Rename forever.
func clearTypeConflict(dest string, kind manifest.Kind) error {
	fi, err := os.Lstat(dest)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if fi.IsDir() == (kind == manifest.KindDir) {
		return nil
	}
	return os.RemoveAll(dest)
}

func (fs *folderState) materialize(rm model.RemoteManifest, dest string, staged map[string]string) error {
	if rm.Deleted {
		if err := os.RemoveAll(dest); err != nil {
			return fmt.Errorf("syncengine: remove %q: %w", rm.Manifest.Path, err)
		}
		return nil
	}
	if err := clearTypeConflict(dest, rm.Manifest.Kind); err != nil {
		return fmt.Errorf("syncengine: clear %q: %w", rm.Manifest.Path, err)
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
		sp, ok := staged[rm.Manifest.Path]
		if !ok {
			return fmt.Errorf("syncengine: no staged file for %q", rm.Manifest.Path)
		}
		if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
			return fmt.Errorf("syncengine: mkdir parent of %q: %w", rm.Manifest.Path, err)
		}
		if err := os.Rename(sp, dest); err != nil {
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
