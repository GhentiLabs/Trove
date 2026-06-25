package node

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/GhentiLabs/Trove/client/internal/chunkstore"
	"github.com/GhentiLabs/Trove/client/internal/model"
	"github.com/GhentiLabs/Trove/client/internal/scanner"
	"github.com/GhentiLabs/Trove/client/internal/session"
	"github.com/GhentiLabs/Trove/client/internal/storage"
	"github.com/GhentiLabs/Trove/client/internal/syncengine"
	"github.com/GhentiLabs/Trove/client/internal/watcher"
)

// syncRuntime is the per-folder stores and role backing the sync engine.
type syncRuntime struct {
	role    syncengine.Role
	folders []syncengine.FolderConfig
	closers []func() error
}

// buildSyncRuntime opens per-folder model and chunk stores under StateDir.
func (s *Service) buildSyncRuntime(ctx context.Context) (*syncRuntime, error) {
	folders, err := s.opts.Config.ListFolders(ctx)
	if err != nil {
		return nil, err
	}
	rt := &syncRuntime{role: s.opts.SyncRole}
	ok := false
	defer func() {
		if !ok {
			rt.close()
		}
	}()
	for _, f := range folders {
		if f.ShareID == "" {
			continue
		}
		if f.Root == "" {
			return nil, fmt.Errorf("node: folder %q has no root configured", f.ShareID)
		}
		if f.Encrypted {
			return nil, fmt.Errorf("node: folder %q is encrypted; encrypted folders are not yet supported", f.ShareID)
		}
		dir := filepath.Join(s.opts.StateDir, "folders", f.ID)
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, fmt.Errorf("node: folder state dir %q: %w", dir, err)
		}
		mdb, err := storage.Open(storage.Options{Path: filepath.Join(dir, "model.db"), MaxOpenConns: 4})
		if err != nil {
			return nil, fmt.Errorf("node: open model db %q: %w", dir, err)
		}
		rt.closers = append(rt.closers, mdb.Close)
		ms, err := model.Open(model.Options{DB: mdb, NodeID: s.opts.NodeID})
		if err != nil {
			return nil, fmt.Errorf("node: open model %q: %w", f.ShareID, err)
		}
		cdb, err := storage.Open(storage.Options{Path: filepath.Join(dir, "chunk.db"), MaxOpenConns: 4})
		if err != nil {
			return nil, fmt.Errorf("node: open chunk db %q: %w", dir, err)
		}
		rt.closers = append(rt.closers, cdb.Close)
		cs, err := chunkstore.Open(chunkstore.Options{DB: cdb, BlobDir: filepath.Join(dir, "blobs"), Logger: s.log})
		if err != nil {
			return nil, fmt.Errorf("node: open chunk store %q: %w", f.ShareID, err)
		}
		rt.closers = append(rt.closers, cs.Close)
		fc := syncengine.FolderConfig{
			FolderID: f.ShareID, Role: s.opts.SyncRole, Root: f.Root, Model: ms, Chunks: cs,
		}
		if s.opts.SyncRole == syncengine.RoleReplica {
			fc.Coord = syncengine.NewCoordinator(f.ShareID, fc.FolderCtx, cs, 0, s.log)
		}
		rt.folders = append(rt.folders, fc)
	}
	ok = true
	return rt, nil
}

func (rt *syncRuntime) close() {
	for i := len(rt.closers) - 1; i >= 0; i-- {
		_ = rt.closers[i]()
	}
	rt.closers = nil
}

// onSession returns a peermgr hook that attaches a sync engine to each session.
func (rt *syncRuntime) onSession(log *slog.Logger) func(context.Context, *session.Session) func() {
	return func(ctx context.Context, sess *session.Session) func() {
		shared := make(map[string]bool, len(sess.SharedFolders()))
		for _, id := range sess.SharedFolders() {
			shared[id] = true
		}
		var fcs []syncengine.FolderConfig
		for _, fc := range rt.folders {
			if shared[fc.FolderID] {
				fcs = append(fcs, fc)
			}
		}
		if len(fcs) == 0 {
			return func() {}
		}
		eng, err := syncengine.New(syncengine.Options{Session: sess, Folders: fcs, Logger: log})
		if err != nil {
			log.Warn("node: sync engine", "err", err)
			return func() {}
		}
		sess.SetControlHandler(eng.Handle)
		dctx, cancel := context.WithCancel(ctx)
		go func() { _ = eng.Drive(dctx) }()
		return cancel
	}
}

// runScanners maintains each owner folder's model from disk until ctx ends. A
// replica never scans its root, so it never originates.
func (rt *syncRuntime) runScanners(ctx context.Context, log *slog.Logger) {
	if rt.role != syncengine.RoleOwner {
		<-ctx.Done()
		return
	}
	var wg sync.WaitGroup
	for _, fc := range rt.folders {
		w, err := watcher.New(fc.Root)
		if err != nil {
			log.Warn("node: watcher", "folder", fc.FolderID, "err", err)
			continue
		}
		sc, err := scanner.New(scanner.Options{
			Root: fc.Root, FolderCtx: fc.FolderCtx, Chunks: fc.Chunks, Model: fc.Model, Watcher: w, Logger: log,
		})
		if err != nil {
			_ = w.Close()
			log.Warn("node: scanner", "folder", fc.FolderID, "err", err)
			continue
		}
		wg.Go(func() {
			defer func() { _ = w.Close() }()
			_ = sc.Run(ctx)
		})
	}
	wg.Wait()
}
