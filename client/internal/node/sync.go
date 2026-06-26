package node

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/GhentiLabs/Trove/client/internal/chunkstore"
	"github.com/GhentiLabs/Trove/client/internal/membership"
	"github.com/GhentiLabs/Trove/client/internal/model"
	"github.com/GhentiLabs/Trove/client/internal/scanner"
	"github.com/GhentiLabs/Trove/client/internal/session"
	"github.com/GhentiLabs/Trove/client/internal/storage"
	"github.com/GhentiLabs/Trove/client/internal/syncengine"
	"github.com/GhentiLabs/Trove/client/internal/watcher"
	"github.com/GhentiLabs/Trove/client/internal/wire"
	"github.com/GhentiLabs/Trove/client/internal/wire/wirepb"
)

// syncRuntime is the per-folder stores backing the sync engine.
type syncRuntime struct {
	folders []syncengine.FolderConfig
	closers []func() error
}

// folderRole is this node's tier for a group: a writer originates local edits, a
// reader only pulls and relays. The founder is always a writer; any other member is a
// writer iff the roster grants it the writer tier.
func folderRole(ctx context.Context, store *membership.Store, self, groupID string) syncengine.Role {
	if founder, ok := membership.Founder(groupID); ok && founder == self {
		return syncengine.RoleWriter
	}
	if role, ok, err := store.RoleOf(ctx, groupID, self); err == nil && ok && role == membership.RoleWriter {
		return syncengine.RoleWriter
	}
	return syncengine.RoleReader
}

// buildSyncRuntime opens per-folder model and chunk stores under StateDir.
func (s *Service) buildSyncRuntime(ctx context.Context) (*syncRuntime, error) {
	folders, err := s.opts.Config.ListFolders(ctx)
	if err != nil {
		return nil, err
	}
	rt := &syncRuntime{}
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
		role := folderRole(ctx, s.members, s.opts.NodeID, f.ShareID)
		fc := syncengine.FolderConfig{
			FolderID: f.ShareID, Role: role, Root: f.Root, Model: ms, Chunks: cs,
		}
		fc.Coord = syncengine.NewCoordinator(f.ShareID, fc.FolderCtx, cs, 0, s.log)
		rt.folders = append(rt.folders, fc)
	}
	ok = true
	return rt, nil
}

// repairFolders re-materializes out-of-band-deleted files for each folder from its
// local chunk store, once at startup before peers attach.
func (rt *syncRuntime) repairFolders(ctx context.Context, log *slog.Logger) {
	for _, fc := range rt.folders {
		if err := syncengine.RepairFolder(ctx, fc, log); err != nil {
			log.Warn("node: startup repair", "folder", fc.FolderID, "err", err)
		}
	}
}

func (rt *syncRuntime) close() {
	for i := len(rt.closers) - 1; i >= 0; i-- {
		_ = rt.closers[i]()
	}
	rt.closers = nil
}

// onSession returns a peermgr hook that, for each session, registers the peer with the
// gossiper and (when the two share folders) attaches a sync engine. A composite control
// handler routes membership gossip to the gossiper and everything else to the engine.
func (rt *syncRuntime) onSession(log *slog.Logger, gossip *gossiper) func(context.Context, *session.Session) func() {
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
		var eng *syncengine.Engine
		if len(fcs) > 0 {
			e, err := syncengine.New(syncengine.Options{Session: sess, Folders: fcs, Logger: log})
			if err != nil {
				log.Warn("node: sync engine", "err", err)
			} else {
				eng = e
			}
		}

		gossip.addPeer(ctx, sess)
		peerID := sess.PeerNodeID()
		sess.SetControlHandler(func(hctx context.Context, typ wire.MessageType, msg proto.Message) error {
			if typ == wire.TypeMembershipGossip {
				gm, ok := msg.(*wirepb.MembershipGossip)
				if !ok {
					log.Warn("node: membership gossip with unexpected payload", "peer", peerID)
					return nil
				}
				return gossip.handle(hctx, peerID, gm)
			}
			if eng != nil {
				return eng.Handle(hctx, typ, msg)
			}
			return nil
		})

		var driveCancel context.CancelFunc
		var driveWg sync.WaitGroup
		if eng != nil {
			dctx, cancel := context.WithCancel(ctx)
			driveCancel = cancel
			driveWg.Add(1)
			go func() {
				defer driveWg.Done()
				_ = eng.Drive(dctx)
			}()
		}
		return func() {
			if driveCancel != nil {
				driveCancel()
				driveWg.Wait()
			}
			gossip.removePeer(peerID, sess)
		}
	}
}

const tombstoneSweepInterval = time.Hour

// runTombstoneSweeper periodically reaps each owned folder's expired, converged
// tombstones until ctx ends.
func (rt *syncRuntime) runTombstoneSweeper(ctx context.Context, log *slog.Logger) {
	ownsAny := false
	for _, fc := range rt.folders {
		if fc.Role == syncengine.RoleWriter {
			ownsAny = true
			break
		}
	}
	if !ownsAny {
		return
	}
	t := time.NewTicker(tombstoneSweepInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			rt.sweepTombstones(ctx, log)
		}
	}
}

func (rt *syncRuntime) sweepTombstones(ctx context.Context, log *slog.Logger) {
	now := time.Now()
	for _, fc := range rt.folders {
		if fc.Role != syncengine.RoleWriter {
			continue
		}
		epoch, err := fc.Model.FolderEpoch(ctx)
		if err != nil {
			log.Warn("node: tombstone sweep epoch", "folder", fc.FolderID, "err", err)
			continue
		}
		safeSeq := int64(math.MaxInt64)
		if hw, ok, err := fc.Model.ConvergedHighWater(ctx, epoch); err != nil {
			log.Warn("node: tombstone sweep gate", "folder", fc.FolderID, "err", err)
			continue
		} else if ok {
			safeSeq = hw
		}
		if safeSeq == 0 {
			log.Debug("node: tombstone reaping gated; awaiting replica convergence", "folder", fc.FolderID)
		}
		n, err := fc.Model.SweepTombstones(ctx, now, safeSeq)
		if err != nil {
			log.Warn("node: tombstone sweep", "folder", fc.FolderID, "err", err)
			continue
		}
		if n > 0 {
			log.Info("node: reaped tombstones", "folder", fc.FolderID, "count", n)
		}
	}
}

// runScanners maintains each owned folder's model from disk until ctx ends. A folder
// this node only reads is never scanned, so it never originates.
func (rt *syncRuntime) runScanners(ctx context.Context, log *slog.Logger) {
	var wg sync.WaitGroup
	for _, fc := range rt.folders {
		if fc.Role != syncengine.RoleWriter {
			continue
		}
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
