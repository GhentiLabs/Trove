package node

import (
	"bytes"
	"context"
	"crypto/subtle"
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
	"github.com/GhentiLabs/Trove/client/internal/crypto"
	"github.com/GhentiLabs/Trove/client/internal/holder"
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
	log     *slog.Logger
	folders []syncengine.FolderConfig
	holders map[string]*holder.Store
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
	rt := &syncRuntime{self: s.opts.NodeID, members: s.members, cfg: s.opts.Config, log: s.log, byShare: map[string]config.Folder{}, holders: map[string]*holder.Store{}}
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
		if f.Holder {
			hs, err := holder.Open(filepath.Join(s.opts.StateDir, "folders", f.ID, "holder"))
			if err != nil {
				return nil, fmt.Errorf("node: open holder store %q: %w", f.ShareID, err)
			}
			rt.holders[f.ShareID] = hs
			rt.byShare[f.ShareID] = f
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
		cs, err := chunkstore.Open(chunkstore.Options{DB: cdb, BlobDir: filepath.Join(dir, "blobs"), ObjectDir: filepath.Join(dir, "objects"), Logger: s.log})
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
	if len(rt.holders) > 0 && len(rt.folders) > 0 {
		return nil, fmt.Errorf("node: a holder node cannot also sync folders; run a dedicated holder")
	}
	s.serves = len(rt.holders) > 0 || len(rt.folders) > 0
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

const holderGCGraceMillis = int64(time.Hour / time.Millisecond)

// onSession returns a peermgr hook that registers each session's peer with the gossiper
// and attaches a sync engine when the two share folders.
func (rt *syncRuntime) onSession(log *slog.Logger, gossip *gossiper) func(context.Context, *session.Session) func() {
	return func(ctx context.Context, sess *session.Session) func() {
		peerID := sess.PeerNodeID()
		shared := make(map[string]bool, len(sess.SharedFolders()))
		for _, id := range sess.SharedFolders() {
			shared[id] = true
		}

		member, err := rt.peerIsMember(ctx, peerID)
		if err != nil {
			log.Warn("node: peer membership check", "peer", peerID, "err", err)
		}
		// A non-member that shares nothing failed the verifier gate; close it rather than keep an
		// idle session a stranger could pile up. Gossip and key delivery below are member-only.
		if !member && len(shared) == 0 {
			_ = sess.Close()
			return func() {}
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

		if member {
			gossip.addPeer(ctx, sess)
		}
		sess.SetControlHandler(func(hctx context.Context, typ wire.MessageType, msg proto.Message) error {
			switch typ {
			case wire.TypeMembershipGossip:
				// Drop a non-member's gossip before Merge: deny a stranger the Ed25519 work.
				if !member {
					return nil
				}
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
				return rt.receiveFolderKey(hctx, log, peerID, fk, sess.PeerEncryptionVerifier(fk.GetFolderId()))
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

		sctx, cancel := context.WithCancel(ctx)
		var wg sync.WaitGroup
		if eng != nil {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = eng.Drive(sctx)
			}()
		}
		// A node is a dedicated holder or a sync member, never both, so the holder server and the
		// engine never contend for the connection.
		if served := rt.holderStoresForPeer(shared); len(served) > 0 {
			// persistHolderVerifiers re-checks membership per folder, so calling it unconditionally
			// is safe and still records the verifier when peerIsMember errored above.
			rt.persistHolderVerifiers(ctx, log, sess, served)
			srv := holder.NewServer(served, rt.holderPutAllowed, log)
			wg.Add(1)
			go func() {
				defer wg.Done()
				srv.Serve(sctx, sess.Conn())
			}()
		}
		var stopHolderPush func()
		if len(rt.folders) > 0 {
			stopHolderPush = rt.startHolderPush(sctx, log, sess, shared)
		}
		return func() {
			cancel()
			if stopHolderPush != nil {
				stopHolderPush()
			}
			wg.Wait()
			if member {
				gossip.removePeer(peerID, sess)
			}
		}
	}
}

// peerIsMember reports whether nodeID is a member or founder of any group this node tracks.
func (rt *syncRuntime) peerIsMember(ctx context.Context, nodeID string) (bool, error) {
	groups, err := rt.members.Groups(ctx)
	if err != nil {
		return false, err
	}
	for _, g := range groups {
		switch _, ok, err := rt.effectiveRole(ctx, g, nodeID); {
		case err != nil:
			return false, err
		case ok:
			return true, nil
		}
	}
	return false, nil
}

// persistHolderVerifiers records, per held folder, the verifier a roster member advertised, so a
// later non-member restore can be proven against it. Only a member's advert is trusted — a
// non-member could otherwise poison the token.
func (rt *syncRuntime) persistHolderVerifiers(ctx context.Context, log *slog.Logger, sess *session.Session, served map[string]*holder.Store) {
	peerID := sess.PeerNodeID()
	for shareID := range served {
		vb := sess.PeerEncryptionVerifier(shareID)
		if len(vb) == 0 {
			continue
		}
		cf, ok := rt.byShare[shareID]
		if !ok {
			continue
		}
		switch _, member, err := rt.effectiveRole(ctx, shareID, peerID); {
		case err != nil:
			log.Debug("node: holder verifier role check", "folder", shareID, "peer", peerID, "err", err)
			continue
		case !member:
			continue
		}
		existing, err := rt.cfg.GetHolderVerifier(ctx, cf.ID)
		if err != nil {
			log.Warn("node: read holder verifier; restore may be denied", "folder", shareID, "err", err)
			continue
		}
		if bytes.Equal(existing, vb) {
			continue
		}
		if err := rt.cfg.SetHolderVerifier(ctx, cf.ID, vb); err != nil {
			log.Warn("node: persist holder verifier; restore will be denied until it succeeds", "folder", shareID, "err", err)
		}
	}
}

// holderStoresForPeer returns the holder stores for the folders shared on a session (the
// responsive offer already gated a non-member by verifier).
func (rt *syncRuntime) holderStoresForPeer(shared map[string]bool) map[string]*holder.Store {
	out := map[string]*holder.Store{}
	for shareID, store := range rt.holders {
		if shared[shareID] {
			out[shareID] = store
		}
	}
	return out
}

// responsiveOffer offers a folder back to a recovery peer (one this node granted nothing) only
// when the peer's advertised verifier constant-time-matches this node's. It fails closed, so a
// stranger that can't prove the verifier is shared nothing and learns no folder ids.
func (rt *syncRuntime) responsiveOffer(ctx context.Context, peerID string, peerOffered []session.Folder) ([]session.Folder, error) {
	var out []session.Folder
	for _, pf := range peerOffered {
		if len(pf.EncryptionVerifier) == 0 {
			continue
		}
		cf, ok := rt.byShare[pf.ShareID]
		if !ok {
			continue
		}
		mine, held := rt.folderVerifier(ctx, cf)
		if len(mine) == 0 || subtle.ConstantTimeCompare(mine, pf.EncryptionVerifier) != 1 {
			continue
		}
		if held {
			rt.log.Info("node: holder restore authorized", "folder", pf.ShareID, "peer", peerID)
		} else {
			rt.log.Info("node: recovery access authorized", "folder", pf.ShareID, "peer", peerID)
		}
		out = append(out, session.Folder{ShareID: pf.ShareID, Encrypted: cf.Encrypted, EncryptionVerifier: mine, Holder: held})
	}
	return out, nil
}

// folderVerifier returns this node's recovery verifier for a folder and whether it is held. A
// holder uses the verifier persisted from a writer; a member derives it from the folder secret.
func (rt *syncRuntime) folderVerifier(ctx context.Context, cf config.Folder) (verifier []byte, held bool) {
	if cf.Holder {
		v, err := rt.cfg.GetHolderVerifier(ctx, cf.ID)
		if err != nil {
			rt.log.Warn("node: holder verifier lookup; denying recovery", "folder", cf.ShareID, "err", err)
			return nil, true
		}
		return v, true
	}
	switch secret, err := rt.cfg.FolderSecret(ctx, cf.ID); {
	case errors.Is(err, config.ErrNoSecret):
		return nil, false
	case err != nil:
		rt.log.Warn("node: folder secret lookup; denying recovery", "folder", cf.ShareID, "err", err)
		return nil, false
	default:
		return crypto.FolderVerifier(secret, cf.ShareID), false
	}
}

// holderPutAllowed authorizes a peer to store blobs on this holder: only a roster writer.
func (rt *syncRuntime) holderPutAllowed(ctx context.Context, folderID, peerID string) (bool, error) {
	return rt.isWriter(ctx, folderID, peerID)
}

// startHolderPush mirrors each shared encrypted folder this node writes to a peer that holds
// it, re-reconciling on every local change. The returned stop unsubscribes and waits for
// in-flight pushes; cancel the session ctx before calling it so reconciles abort promptly.
func (rt *syncRuntime) startHolderPush(ctx context.Context, log *slog.Logger, sess *session.Session, shared map[string]bool) func() {
	set := &holderPusherSet{ctx: ctx}
	var cancels []func()
	peerID := sess.PeerNodeID()
	conn := sess.Conn()
	for _, fc := range rt.folders {
		if !shared[fc.FolderID] {
			continue
		}
		cf := rt.byShare[fc.FolderID]
		if !cf.Encrypted {
			continue
		}
		switch role, ok, err := rt.effectiveRole(ctx, cf.ShareID, peerID); {
		case err != nil:
			log.Warn("node: holder push role check", "folder", cf.ShareID, "peer", peerID, "err", err)
			continue
		case !ok || role != membership.RoleHolder:
			continue
		}
		if mine, err := rt.isWriter(ctx, cf.ShareID, rt.self); err != nil || !mine {
			continue
		}
		key, _, err := rt.cfg.GetFolderKey(ctx, cf.ID)
		if err != nil {
			continue
		}
		target, shareID, folderKey := fc, cf.ShareID, key
		p := &holderPusher{folder: shareID, log: log, do: func(ctx context.Context) error {
			exportCtx := chunkstore.FolderContext{Encrypted: true, MasterKey: folderKey}
			return holder.Reconcile(ctx, folderKey, target.Model, target.Chunks, exportCtx, holder.HasBlobsOverConn(conn, shareID), holder.PutBlobOverConn(conn, shareID))
		}}
		set.trigger(p)
		cancels = append(cancels, target.Coord.OnAnnounce(func() { set.trigger(p) }))
		set.wg.Go(func() {
			err := holder.Collect(ctx, folderKey, target.Model, holder.ListBlobsOverConn(conn, shareID), holder.DeleteBlobsOverConn(conn, shareID), holderGCGraceMillis, time.Now().UnixMilli())
			if err != nil && ctx.Err() == nil {
				log.Warn("node: holder gc", "folder", shareID, "err", err)
			}
		})
	}
	return func() {
		for _, c := range cancels {
			c()
		}
		set.stop()
	}
}

type holderPusher struct {
	do     func(context.Context) error
	folder string
	log    *slog.Logger

	mu      sync.Mutex
	running bool
	dirty   bool
}

type holderPusherSet struct {
	ctx context.Context

	mu      sync.Mutex
	stopped bool
	wg      sync.WaitGroup
}

func (s *holderPusherSet) trigger(p *holderPusher) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return
	}
	p.mu.Lock()
	if p.running {
		p.dirty = true
		p.mu.Unlock()
		return
	}
	p.running = true
	p.mu.Unlock()
	s.wg.Go(func() { s.run(p) })
}

func (s *holderPusherSet) run(p *holderPusher) {
	for {
		if err := p.do(s.ctx); err != nil && s.ctx.Err() == nil {
			p.log.Warn("node: push to holder", "folder", p.folder, "err", err)
		}
		p.mu.Lock()
		if !p.dirty || s.ctx.Err() != nil {
			p.running = false
			p.mu.Unlock()
			return
		}
		p.dirty = false
		p.mu.Unlock()
	}
}

func (s *holderPusherSet) stop() {
	s.mu.Lock()
	s.stopped = true
	s.mu.Unlock()
	s.wg.Wait()
}

// attachFolders returns the engine configs to attach for a session and the folder keys
// to deliver to the peer. An encrypted folder is attached only once its key is known.
func (rt *syncRuntime) attachFolders(ctx context.Context, log *slog.Logger, peerID string, shared map[string]bool) (fcs []syncengine.FolderConfig, deliveries []*wirepb.FolderKey) {
	for _, fc := range rt.folders {
		if !shared[fc.FolderID] {
			continue
		}
		cf := rt.byShare[fc.FolderID]
		if !cf.Encrypted {
			fcs = append(fcs, fc)
			if d := rt.recoverySecretForPeer(ctx, log, cf, peerID); d != nil {
				deliveries = append(deliveries, d)
			}
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
		if d := rt.folderKeyForPeer(ctx, log, cf, peerID, key, gen); d != nil {
			deliveries = append(deliveries, d)
		}
	}
	return fcs, deliveries
}

// folderKeyForPeer returns a folder-key message for peerID when this node is a writer and the
// peer is a trusted member; a holder or non-member gets nil.
func (rt *syncRuntime) folderKeyForPeer(ctx context.Context, log *slog.Logger, cf config.Folder, peerID string, key [config.MasterKeyLen]byte, gen int) *wirepb.FolderKey {
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

// recoverySecretForPeer returns an unencrypted folder's recovery secret to deliver (reusing the
// FolderKey message) when this node is a writer and the peer is a trusted member, else nil.
func (rt *syncRuntime) recoverySecretForPeer(ctx context.Context, log *slog.Logger, cf config.Folder, peerID string) *wirepb.FolderKey {
	switch mine, err := rt.isWriter(ctx, cf.ShareID, rt.self); {
	case err != nil:
		log.Warn("node: recovery secret delivery skipped", "folder", cf.ShareID, "err", err)
		return nil
	case !mine:
		return nil
	}
	switch trusted, err := rt.peerTrusted(ctx, cf.ShareID, peerID); {
	case err != nil:
		log.Warn("node: recovery secret delivery skipped", "folder", cf.ShareID, "peer", peerID, "err", err)
		return nil
	case !trusted:
		return nil
	}
	switch secret, err := rt.cfg.FolderSecret(ctx, cf.ID); {
	case errors.Is(err, config.ErrNoSecret):
		return nil
	case err != nil:
		log.Warn("node: recovery secret lookup", "folder", cf.ShareID, "err", err)
		return nil
	default:
		return &wirepb.FolderKey{FolderId: cf.ShareID, Key: secret[:]}
	}
}

// receiveFolderKey persists a key delivered by a roster writer for a folder this node
// still lacks, then ends the session to reattach. A delivery from a non-writer or for an
// unknown or already-keyed folder is ignored.
func (rt *syncRuntime) receiveFolderKey(ctx context.Context, log *slog.Logger, peerID string, fk *wirepb.FolderKey, peerVerifier []byte) error {
	cf, ok := rt.byShare[fk.GetFolderId()]
	if !ok || cf.Holder {
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
	if len(peerVerifier) > 0 && !bytes.Equal(crypto.FolderVerifier(key, cf.ShareID), peerVerifier) {
		log.Warn("node: rejecting folder key inconsistent with the sender's verifier", "folder", cf.ShareID, "peer", peerID)
		return nil
	}
	if !cf.Encrypted {
		// An unencrypted folder's "key" is its recovery secret; store it but don't reattach
		// (the engine doesn't need it).
		switch err := rt.cfg.DeliverRecoverySecret(ctx, cf.ID, key); {
		case err == nil:
			log.Info("node: recovery secret received", "folder", cf.ShareID, "peer", peerID)
		case errors.Is(err, config.ErrSecretExists):
		default:
			log.Warn("node: store recovery secret", "folder", cf.ShareID, "peer", peerID, "err", err)
		}
		return nil
	}
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

// peerTrusted reports whether nodeID is a member entitled to the folder key: a reader,
// writer, or founder.
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
	var zeroKey [config.MasterKeyLen]byte
	for _, fc := range rt.folders {
		if fc.Role != syncengine.RoleWriter {
			continue
		}
		fctx, err := rt.folderContext(ctx, rt.byShare[fc.FolderID])
		if err != nil {
			log.Warn("node: scanner folder key", "folder", fc.FolderID, "err", err)
			continue
		}
		if fctx.Encrypted && fctx.MasterKey == zeroKey {
			log.Info("node: scanner awaiting folder key; restart after it is delivered", "folder", fc.FolderID)
			continue
		}
		w, err := watcher.New(fc.Root)
		if err != nil {
			log.Warn("node: watcher", "folder", fc.FolderID, "err", err)
			continue
		}
		sc, err := scanner.New(scanner.Options{
			Root: fc.Root, Chunks: fc.Chunks, Model: fc.Model, Watcher: w, Logger: log,
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
