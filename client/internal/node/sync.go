package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/GhentiLabs/Trove/client/internal/chunkstore"
	"github.com/GhentiLabs/Trove/client/internal/config"
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
	self    string
	members *membership.Store
	cfg     *config.Store
	folders []syncengine.FolderConfig
	byShare map[string]config.Folder
	closers []func() error
}

// effectiveRole returns nodeID's role in groupID, treating the founder as a writer
// even without an explicit roster entry. ok is false for a non-member.
func (rt *syncRuntime) effectiveRole(ctx context.Context, groupID, nodeID string) (membership.Role, bool, error) {
	if nodeID == "" {
		return 0, false, nil
	}
	if founder, ok := membership.Founder(groupID); ok && founder == nodeID {
		return membership.RoleWriter, true, nil
	}
	return rt.members.RoleOf(ctx, groupID, nodeID)
}

// isWriter reports whether nodeID may originate edits in groupID.
func (rt *syncRuntime) isWriter(ctx context.Context, groupID, nodeID string) (bool, error) {
	role, ok, err := rt.effectiveRole(ctx, groupID, nodeID)
	return ok && role == membership.RoleWriter, err
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
	rt := &syncRuntime{self: s.opts.NodeID, members: s.members, cfg: s.opts.Config, byShare: map[string]config.Folder{}}
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
		fctx, err := rt.folderContext(ctx, f)
		if err != nil {
			return nil, err
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
		shareID := f.ShareID
		fc := syncengine.FolderConfig{
			FolderID: shareID, Role: role, Root: f.Root, Model: ms, Chunks: cs, FolderCtx: fctx,
			AuthorWriter: func(ctx context.Context, nodeID string) (bool, error) {
				return rt.isWriter(ctx, shareID, nodeID)
			},
		}
		fc.Coord = syncengine.NewCoordinator(shareID, fc.FolderCtx, cs, 0, s.log)
		rt.folders = append(rt.folders, fc)
		rt.byShare[shareID] = f
	}
	ok = true
	return rt, nil
}

// folderContext resolves a folder's encryption context. An encrypted folder whose key
// has not yet been delivered gets an Encrypted context with no key.
func (rt *syncRuntime) folderContext(ctx context.Context, f config.Folder) (chunkstore.FolderContext, error) {
	if !f.Encrypted {
		return chunkstore.FolderContext{}, nil
	}
	switch key, _, err := rt.cfg.GetFolderKey(ctx, f.ID); {
	case err == nil:
		return chunkstore.FolderContext{Encrypted: true, MasterKey: key}, nil
	case errors.Is(err, config.ErrNoKey):
		return chunkstore.FolderContext{Encrypted: true}, nil
	default:
		return chunkstore.FolderContext{}, fmt.Errorf("node: folder %q key: %w", f.ShareID, err)
	}
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

// otherMembers returns the node ids of every member of groupID except this node — the
// peers that could still hold a file this node has tombstoned.
func (rt *syncRuntime) otherMembers(ctx context.Context, groupID string) ([]string, error) {
	roster, err := rt.members.Roster(ctx, groupID)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(roster))
	for _, e := range roster {
		if e.NodeID != rt.self {
			out = append(out, e.NodeID)
		}
	}
	return out, nil
}

func (rt *syncRuntime) close() {
	for i := len(rt.closers) - 1; i >= 0; i-- {
		_ = rt.closers[i]()
	}
	rt.closers = nil
}

// errReattachAfterKey ends a session once a folder key has just been delivered.
var errReattachAfterKey = errors.New("node: reattach after folder key delivery")

// onSession returns a peermgr hook that, for each session, registers the peer with the
// gossiper and (when the two share folders) attaches a sync engine. A composite control
// handler routes membership gossip to the gossiper, folder-key delivery to the key sink,
// and everything else to the engine. Encrypted folders attach only once keyed.
func (rt *syncRuntime) onSession(log *slog.Logger, gossip *gossiper) func(context.Context, *session.Session) func() {
	return func(ctx context.Context, sess *session.Session) func() {
		peerID := sess.PeerNodeID()
		shared := make(map[string]bool, len(sess.SharedFolders()))
		for _, id := range sess.SharedFolders() {
			shared[id] = true
		}
		fcs, deliveries := rt.attachFolders(ctx, log, peerID, shared)

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
		sess.SetControlHandler(func(hctx context.Context, typ wire.MessageType, msg proto.Message) error {
			switch typ {
			case wire.TypeMembershipGossip:
				gm, ok := msg.(*wirepb.MembershipGossip)
				if !ok {
					log.Warn("node: membership gossip with unexpected payload", "peer", peerID)
					return nil
				}
				return gossip.handle(hctx, peerID, gm)
			case wire.TypeFolderKey:
				fk, ok := msg.(*wirepb.FolderKey)
				if !ok {
					log.Warn("node: folder key with unexpected payload", "peer", peerID)
					return nil
				}
				return rt.receiveFolderKey(hctx, log, peerID, fk)
			default:
				if eng != nil {
					return eng.Handle(hctx, typ, msg)
				}
				return nil
			}
		})

		for _, d := range deliveries {
			if err := sess.Send(d); err != nil {
				log.Debug("node: deliver folder key", "folder", d.GetFolderId(), "peer", peerID, "err", err)
				break
			}
		}

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

// attachFolders resolves the shared folders for a session: it returns the engine
// configs to attach (an encrypted folder is included only once its key is known, with
// the live key threaded into its coordinator) and the folder keys to deliver to a
// trusted peer that may still lack them.
func (rt *syncRuntime) attachFolders(ctx context.Context, log *slog.Logger, peerID string, shared map[string]bool) (fcs []syncengine.FolderConfig, deliveries []*wirepb.FolderKey) {
	for _, fc := range rt.folders {
		if !shared[fc.FolderID] {
			continue
		}
		cf := rt.byShare[fc.FolderID]
		if !cf.Encrypted {
			fcs = append(fcs, fc)
			continue
		}
		key, gen, err := rt.cfg.GetFolderKey(ctx, cf.ID)
		if errors.Is(err, config.ErrNoKey) {
			continue
		}
		if err != nil {
			log.Warn("node: folder key", "folder", fc.FolderID, "err", err)
			continue
		}
		fctx := chunkstore.FolderContext{Encrypted: true, MasterKey: key}
		fc.FolderCtx = fctx
		fc.Coord.SetFolderContext(fctx)
		fcs = append(fcs, fc)
		if d := rt.deliverable(ctx, log, cf, peerID, key, gen); d != nil {
			deliveries = append(deliveries, d)
		}
	}
	return fcs, deliveries
}

// deliverable returns a folder-key message for peerID when this node is a writer able
// to share the key and the peer is a trusted member; a holder or non-member gets nil.
// The key is re-offered on every reconnect; that is safe because the receiver stores it
// only if absent (DeliverFolderKey), and it never leaves the authenticated session.
func (rt *syncRuntime) deliverable(ctx context.Context, log *slog.Logger, cf config.Folder, peerID string, key [config.MasterKeyLen]byte, gen int) *wirepb.FolderKey {
	mine, err := rt.isWriter(ctx, cf.ShareID, rt.self)
	if err != nil {
		log.Warn("node: folder key delivery skipped", "folder", cf.ShareID, "err", err)
		return nil
	}
	if !mine {
		return nil
	}
	trusted, err := rt.peerTrusted(ctx, cf.ShareID, peerID)
	if err != nil {
		log.Warn("node: folder key delivery skipped", "folder", cf.ShareID, "peer", peerID, "err", err)
		return nil
	}
	if !trusted {
		return nil
	}
	return &wirepb.FolderKey{FolderId: cf.ShareID, Key: key[:], KeyGeneration: uint64(gen)}
}

// receiveFolderKey persists a key delivered by a roster writer for a folder this node
// still lacks, then ends the session so it reconnects and attaches the folder. A
// delivery from a non-writer, for an unknown or already-keyed folder, is ignored.
func (rt *syncRuntime) receiveFolderKey(ctx context.Context, log *slog.Logger, peerID string, fk *wirepb.FolderKey) error {
	cf, ok := rt.byShare[fk.GetFolderId()]
	if !ok || !cf.Encrypted {
		return nil
	}
	switch okWriter, err := rt.isWriter(ctx, cf.ShareID, peerID); {
	case err != nil:
		log.Warn("node: folder key sender check", "folder", cf.ShareID, "peer", peerID, "err", err)
		return nil
	case !okWriter:
		log.Warn("node: ignoring folder key from non-writer", "folder", cf.ShareID, "peer", peerID)
		return nil
	}
	if len(fk.GetKey()) != config.MasterKeyLen {
		log.Warn("node: folder key wrong length", "folder", cf.ShareID, "peer", peerID)
		return nil
	}
	var key [config.MasterKeyLen]byte
	copy(key[:], fk.GetKey())
	switch err := rt.cfg.DeliverFolderKey(ctx, cf.ID, key, int(fk.GetKeyGeneration())); {
	case err == nil:
		log.Info("node: folder key received; reattaching", "folder", cf.ShareID, "peer", peerID)
		return errReattachAfterKey
	case errors.Is(err, config.ErrKeyExists):
		return nil
	default:
		log.Warn("node: store delivered key", "folder", cf.ShareID, "peer", peerID, "err", err)
		return nil
	}
}

// peerTrusted reports whether nodeID is a member entitled to the folder key — a reader,
// writer, or founder. A holder or non-member is not.
func (rt *syncRuntime) peerTrusted(ctx context.Context, groupID, nodeID string) (bool, error) {
	role, ok, err := rt.effectiveRole(ctx, groupID, nodeID)
	return ok && (role == membership.RoleWriter || role == membership.RoleReader), err
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
		members, err := rt.otherMembers(ctx, fc.FolderID)
		if err != nil {
			log.Warn("node: tombstone sweep roster", "folder", fc.FolderID, "err", err)
			continue
		}
		safeSeq := int64(math.MaxInt64)
		if hw, ok, err := fc.Model.ConvergedHighWater(ctx, epoch, members); err != nil {
			log.Warn("node: tombstone sweep gate", "folder", fc.FolderID, "err", err)
			continue
		} else if ok {
			safeSeq = hw
		}
		if safeSeq == 0 {
			log.Debug("node: tombstone reaping gated; awaiting member convergence", "folder", fc.FolderID)
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
