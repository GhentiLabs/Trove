// Package syncengine drives one-way folder convergence over an Active session: an
// owner serves its manifests and chunks; a replica pulls them and applies them
// crash-safely to disk. One Engine is bound to one session and covers the folders
// shared on it. It depends on model, chunkstore, session, and wire, and nothing
// depends back on it. Control messages ride the session control stream as protobuf;
// chunk payloads ride dedicated data streams as raw bytes (see codec.go).
package syncengine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/chunkstore"
	"github.com/GhentiLabs/Trove/client/internal/manifest"
	"github.com/GhentiLabs/Trove/client/internal/model"
	"github.com/GhentiLabs/Trove/client/internal/netio"
	"github.com/GhentiLabs/Trove/client/internal/session"
	"github.com/GhentiLabs/Trove/client/internal/snapshot"
	"github.com/GhentiLabs/Trove/client/internal/wire"
	"github.com/GhentiLabs/Trove/client/internal/wire/wirepb"
	"google.golang.org/protobuf/proto"
)

const (
	// DefaultInFlight bounds a replica's concurrent chunk data streams.
	DefaultInFlight = 16

	maxServeStreams   = 64
	maxManifestServes = 8
	tmpDirName        = ".trove-tmp"
	deltaTimeout      = 60 * time.Second
	announceInterval  = 5 * time.Second

	// protoRepeatedOverhead bounds a repeated field's tag (1) + length varint (up to 3
	// for values below 16 KiB) when budgeting a delta page, so the estimate never
	// undercounts the wire size.
	protoRepeatedOverhead = 4

	// chunkRefWireSize over-estimates one ChunkRef on the wire (32-byte id + length +
	// framing), used to decide how many of a manifest's chunk refs fit in one page.
	chunkRefWireSize = 48

	// defaultMaxDeltaBytes caps one ManifestDelta page below the control-frame cap,
	// leaving headroom for the envelope; larger folders span multiple pages.
	defaultMaxDeltaBytes = wire.MaxControlMessageSize - 4096
	// maxDeltaPages bounds a single reconcile's paging so a peer cannot loop it.
	maxDeltaPages = 1 << 20
)

var (
	// ErrNoSession is returned when New is given no session.
	ErrNoSession = errors.New("syncengine: nil session")

	errChunkUnavailable = errors.New("syncengine: chunk unavailable from owner")
	errChunkVerify      = errors.New("syncengine: chunk failed hash verification")
)

// Role is a folder's one-way direction on a session.
type Role uint8

const (
	// RoleOwner serves manifests and chunks and never pulls.
	RoleOwner Role = iota
	// RoleReplica pulls and applies and never originates.
	RoleReplica
)

// FolderConfig binds one shared folder to its local stores and on-disk root. Coord is
// the node's per-folder multi-source coordinator, shared across the node's sessions; a
// replica sets it, an owner leaves it nil.
type FolderConfig struct {
	FolderID  string
	Role      Role
	Root      string
	FolderCtx chunkstore.FolderContext
	Model     *model.Store
	Chunks    *chunkstore.Store
	Coord     *Coordinator
}

// Options configures an Engine bound to one Active session.
type Options struct {
	Session       *session.Session
	Folders       []FolderConfig
	Logger        *slog.Logger
	MaxDeltaBytes int
}

// Engine runs one-way sync for one session's shared folders.
type Engine struct {
	sess          *session.Session
	log           *slog.Logger
	maxDeltaBytes int
	folders       map[string]*folderState
	ownsAny       bool
	serveSem      chan struct{}
	manifestSem   chan struct{}

	servedChunks atomic.Int64
	servedDeltas atomic.Int64
}

type folderState struct {
	eng      *Engine
	cfg      FolderConfig
	stageDir string

	mu     sync.Mutex
	busy   bool
	dirty  bool
	latest announce
	reply  chan *wirepb.ManifestDelta
}

type announce struct {
	root      snapshot.Root
	epoch     uint64
	highWater int64
}

// stageDir is a per-session staging directory, so engines for the same folder on
// different sessions never clobber each other's in-progress apply.
func stageDir(root, peer string) string {
	if len(peer) > 8 {
		peer = peer[:8]
	}
	return filepath.Join(root, tmpDirName+"-"+peer)
}

// New builds an Engine for the given session and folders.
func New(opts Options) (*Engine, error) {
	if opts.Session == nil {
		return nil, ErrNoSession
	}
	log := opts.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	maxDelta := opts.MaxDeltaBytes
	if maxDelta <= 0 {
		maxDelta = defaultMaxDeltaBytes
	}
	shared := make(map[string]struct{}, len(opts.Session.SharedFolders()))
	for _, id := range opts.Session.SharedFolders() {
		shared[id] = struct{}{}
	}
	e := &Engine{
		sess:          opts.Session,
		log:           log,
		maxDeltaBytes: maxDelta,
		folders:       make(map[string]*folderState, len(opts.Folders)),
	}
	for _, fc := range opts.Folders {
		if _, ok := shared[fc.FolderID]; !ok {
			return nil, fmt.Errorf("syncengine: folder %q not shared on this session", fc.FolderID)
		}
		if fc.Model == nil || fc.Chunks == nil {
			return nil, fmt.Errorf("syncengine: folder %q missing stores", fc.FolderID)
		}
		if fc.Role == RoleReplica && fc.Coord == nil {
			return nil, fmt.Errorf("syncengine: replica folder %q missing coordinator", fc.FolderID)
		}
		if fc.Role == RoleOwner && fc.Coord != nil {
			return nil, fmt.Errorf("syncengine: owner folder %q must not have a coordinator", fc.FolderID)
		}
		e.folders[fc.FolderID] = &folderState{eng: e, cfg: fc, stageDir: stageDir(fc.Root, opts.Session.PeerNodeID())}
		if fc.Role == RoleOwner {
			e.ownsAny = true
		}
	}
	if len(e.folders) > 0 {
		e.serveSem = make(chan struct{}, maxServeStreams)
	}
	if e.ownsAny {
		e.manifestSem = make(chan struct{}, maxManifestServes)
	}
	return e, nil
}

// Drive runs the sync engine until ctx is cancelled. It registers this session as a
// source with each folder's coordinator (so a replica can pull from this peer) and
// serves inbound chunk requests; an owner also announces.
func (e *Engine) Drive(ctx context.Context) error {
	peer := e.sess.PeerNodeID()
	conn := e.sess.Conn()
	for _, fs := range e.folders {
		if fs.cfg.Role == RoleReplica {
			_ = os.RemoveAll(fs.stageDir)
		}
		if fs.cfg.Coord != nil {
			fs.cfg.Coord.addSource(peer, conn)
			defer fs.cfg.Coord.removeSource(peer, conn)
		}
	}
	var wg sync.WaitGroup
	wg.Go(func() { e.serveLoop(ctx) })
	if e.ownsAny {
		wg.Go(func() { e.announceLoop(ctx) })
		e.Announce(ctx)
	}
	<-ctx.Done()
	wg.Wait()
	return ctx.Err()
}

func (e *Engine) announceLoop(ctx context.Context) {
	t := time.NewTicker(announceInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			e.Announce(ctx)
		}
	}
}

// Announce sends a FolderSummary for every owned folder.
func (e *Engine) Announce(ctx context.Context) {
	for _, fs := range e.folders {
		if fs.cfg.Role != RoleOwner {
			continue
		}
		if err := e.announceFolder(ctx, fs); err != nil {
			e.log.Warn("syncengine: announce", "folder", fs.cfg.FolderID, "err", err)
		}
	}
}

func (e *Engine) announceFolder(ctx context.Context, fs *folderState) error {
	root, err := fs.cfg.Model.CurrentRoot(ctx)
	if err != nil {
		return err
	}
	epoch, err := fs.cfg.Model.FolderEpoch(ctx)
	if err != nil {
		return err
	}
	hw, err := fs.cfg.Model.HighWater(ctx)
	if err != nil {
		return err
	}
	return e.sess.Send(&wirepb.FolderSummary{
		FolderId:          fs.cfg.FolderID,
		SnapshotRoot:      root.Bytes(),
		IndexEpochId:      epoch,
		HighWaterSequence: hw,
	})
}

// Handle is the session's control handler for syncengine messages. It must not
// block the read loop, so owner replies and reconciliation run in their own goroutines.
func (e *Engine) Handle(ctx context.Context, typ wire.MessageType, msg proto.Message) error {
	switch typ {
	case wire.TypeFolderSummary:
		sum := msg.(*wirepb.FolderSummary)
		fs := e.folders[sum.GetFolderId()]
		if fs == nil || fs.cfg.Role != RoleReplica {
			return nil
		}
		root, err := snapshot.RootFromBytes(sum.GetSnapshotRoot())
		if err != nil {
			e.log.Warn("syncengine: bad summary root", "folder", sum.GetFolderId(), "err", err)
			return nil
		}
		fs.trigger(ctx, announce{root: root, epoch: sum.GetIndexEpochId(), highWater: sum.GetHighWaterSequence()})
	case wire.TypeManifestRequest:
		req := msg.(*wirepb.ManifestRequest)
		fs := e.folders[req.GetFolderId()]
		if fs == nil || fs.cfg.Role != RoleOwner {
			return nil
		}
		select {
		case e.manifestSem <- struct{}{}:
		default:
			e.log.Warn("syncengine: manifest serve backlog, dropping request", "folder", req.GetFolderId())
			return nil
		}
		go func() {
			defer func() { <-e.manifestSem }()
			e.serveManifest(ctx, fs, req)
		}()
	case wire.TypeManifestDelta:
		d := msg.(*wirepb.ManifestDelta)
		if fs := e.folders[d.GetFolderId()]; fs != nil {
			fs.deliver(d)
		}
	case wire.TypeSyncReceipt:
		// Off the read loop: the model write can block on the SQLite write lock, and
		// the receipt is best-effort. The peer re-sends on its next reconcile.
		go e.recordReceipt(ctx, msg.(*wirepb.SyncReceipt))
	}
	return nil
}

// recordReceipt stores a replica's convergence acknowledgement, stamped with the owner's
// clock so "last synced" never depends on a replica's. A receipt is trusted only as far
// as the owner can check it: its epoch must be the owner's current one and its
// high-water is capped to what the owner has actually produced, so a buggy or hostile
// replica cannot move the tombstone-reaping gate past a sequence it never reached.
// Ignored unless this node owns the folder.
func (e *Engine) recordReceipt(ctx context.Context, rec *wirepb.SyncReceipt) {
	fs := e.folders[rec.GetFolderId()]
	if fs == nil || fs.cfg.Role != RoleOwner {
		return
	}
	peer := e.sess.PeerNodeID()
	epoch, err := fs.cfg.Model.FolderEpoch(ctx)
	if err != nil {
		e.log.Warn("syncengine: receipt epoch", "folder", rec.GetFolderId(), "peer", peer, "err", err)
		return
	}
	if rec.GetIndexEpochId() != epoch {
		return
	}
	root, err := snapshot.RootFromBytes(rec.GetSnapshotRoot())
	if err != nil {
		e.log.Warn("syncengine: bad receipt root", "folder", rec.GetFolderId(), "peer", peer, "err", err)
		return
	}
	hw := rec.GetHighWaterSequence()
	if ownerMax, err := fs.cfg.Model.HighWater(ctx); err == nil && hw > ownerMax {
		hw = ownerMax
	}
	if err := fs.cfg.Model.RecordReceipt(ctx, model.Receipt{
		PeerID: peer, Root: root, Epoch: epoch, HighWater: hw, SyncedAt: time.Now(),
	}); err != nil {
		e.log.Warn("syncengine: record peer receipt", "folder", rec.GetFolderId(), "peer", peer, "err", err)
	}
}

// ServedChunks is the number of chunks this engine has served as an owner.
func (e *Engine) ServedChunks() int64 { return e.servedChunks.Load() }

func (e *Engine) serveLoop(ctx context.Context) {
	for {
		s, err := e.sess.Conn().AcceptStream(ctx)
		if err != nil {
			return
		}
		select {
		case e.serveSem <- struct{}{}:
		case <-ctx.Done():
			_ = s.Close()
			return
		}
		go func() {
			defer func() { <-e.serveSem }()
			e.serveOneStream(ctx, s)
		}()
	}
}

func (e *Engine) serveOneStream(ctx context.Context, s netio.Stream) {
	defer func() { _ = s.Close() }()
	folderID, id, err := readChunkRequest(s)
	if err != nil {
		e.log.Debug("syncengine: read chunk request", "err", err)
		return
	}
	fs := e.folders[folderID]
	if fs == nil {
		_ = writeChunkResponseHeader(s, StatusError, 0)
		return
	}
	data, err := fs.cfg.Chunks.Get(ctx, fs.cfg.FolderCtx, id)
	if err != nil {
		_ = writeChunkResponseHeader(s, StatusNotFound, 0)
		return
	}
	if err := writeChunkResponseHeader(s, StatusOK, uint32(len(data))); err != nil {
		return
	}
	if _, err := s.Write(data); err != nil {
		return
	}
	e.servedChunks.Add(1)
}

func (e *Engine) serveManifest(ctx context.Context, fs *folderState, req *wirepb.ManifestRequest) {
	delta, err := e.buildDelta(ctx, fs, req)
	if err != nil {
		e.log.Warn("syncengine: build delta", "folder", fs.cfg.FolderID, "err", err)
		return
	}
	if err := e.sess.Send(delta); err != nil {
		e.log.Debug("syncengine: send delta", "folder", fs.cfg.FolderID, "err", err)
		return
	}
	e.servedDeltas.Add(1)
}

// buildDelta returns one page of manifests past the request cursor, capped at
// maxDeltaBytes. The cursor is (since_sequence, since_chunk_offset): a manifest whose
// chunk list does not fit one page is split across pages, so even a huge file converges.
// While a manifest is mid-delivery, high_water_sequence holds at the last fully-sent
// sequence and high_water_chunk_offset counts the chunks delivered; both reset when it
// completes.
func (e *Engine) buildDelta(ctx context.Context, fs *folderState, req *wirepb.ManifestRequest) (*wirepb.ManifestDelta, error) {
	epoch, err := fs.cfg.Model.FolderEpoch(ctx)
	if err != nil {
		return nil, err
	}
	since := req.GetSinceSequence()
	offset := req.GetSinceChunkOffset()
	if req.GetIndexEpochId() != epoch {
		since, offset = 0, 0
	}
	recs, err := fs.cfg.Model.ManifestsSince(ctx, since)
	if err != nil {
		return nil, err
	}

	delta := &wirepb.ManifestDelta{FolderId: fs.cfg.FolderID, IndexEpochId: epoch, HighWaterSequence: since}

	// Resuming a manifest whose chunk list spilled across pages: recs[0] is it.
	if offset > 0 {
		if len(recs) == 0 { // the in-progress manifest is gone (e.g. epoch change); restart clean.
			delta.Complete = true
			return delta, nil
		}
		rec := recs[0]
		rm, delivered, more := sliceManifest(rec, offset, e.maxDeltaBytes)
		delta.Manifests = []*wirepb.RemoteManifest{rm}
		if more {
			delta.HighWaterChunkOffset = delivered
		} else {
			delta.HighWaterSequence = rec.Seq
			delta.Complete = len(recs) == 1
		}
		return delta, nil
	}

	// Normal page: pack whole manifests until the budget is hit. A manifest too large for
	// an otherwise-empty page begins a continuation.
	size := 0
	for _, rec := range recs {
		meta := recordToWireMeta(rec)
		s := proto.Size(meta) + protoRepeatedOverhead + len(rec.Manifest.Chunks)*chunkRefWireSize
		if size+s > e.maxDeltaBytes {
			if len(delta.Manifests) > 0 {
				return delta, nil // complete stays false; HighWaterSequence is the last full seq
			}
			rm, delivered, _ := sliceManifest(rec, 0, e.maxDeltaBytes)
			delta.Manifests = []*wirepb.RemoteManifest{rm}
			delta.HighWaterChunkOffset = delivered
			return delta, nil
		}
		delta.Manifests = append(delta.Manifests, recordToWire(rec))
		size += s
		delta.HighWaterSequence = rec.Seq
	}
	delta.Complete = true
	return delta, nil
}

// sliceManifest builds a wire manifest carrying rec's chunks starting at offset, taking
// as many as fit one page. It returns the entry, the total chunks now delivered, and
// whether more remain (MoreChunks set accordingly).
func sliceManifest(rec model.Record, offset int64, budget int) (*wirepb.RemoteManifest, int64, bool) {
	rm := recordToWireMeta(rec)
	all := rec.Manifest.Chunks
	if offset > int64(len(all)) {
		offset = int64(len(all))
	}
	remaining := all[offset:]
	fit := (budget - proto.Size(rm) - protoRepeatedOverhead) / chunkRefWireSize
	if fit < 1 {
		fit = 1
	}
	if fit > len(remaining) {
		fit = len(remaining)
	}
	rm.Chunks = wireChunks(remaining[:fit])
	more := int(offset)+fit < len(all)
	rm.MoreChunks = more
	return rm, offset + int64(fit), more
}

// DeltasSent is the number of manifest-delta pages this engine has sent as an owner.
func (e *Engine) DeltasSent() int64 { return e.servedDeltas.Load() }

func recordToWireMeta(rec model.Record) *wirepb.RemoteManifest {
	rm := &wirepb.RemoteManifest{
		Kind:          uint32(rec.Manifest.Kind),
		Path:          rec.Manifest.Path,
		Mode:          rec.Manifest.Mode,
		SymlinkTarget: rec.Manifest.SymlinkTarget,
		ManifestId:    rec.ID.Bytes(),
		VersionVector: rec.Version.Canonical(),
		OwnerSequence: rec.Seq,
		Deleted:       rec.Deleted,
	}
	if rec.Deleted {
		rm.DeletedMs = rec.DeletedAt.UnixMilli()
	}
	return rm
}

func recordToWire(rec model.Record) *wirepb.RemoteManifest {
	rm := recordToWireMeta(rec)
	rm.Chunks = wireChunks(rec.Manifest.Chunks)
	return rm
}

func wireChunks(chunks []manifest.ChunkRef) []*wirepb.ChunkRef {
	out := make([]*wirepb.ChunkRef, len(chunks))
	for i, c := range chunks {
		out[i] = &wirepb.ChunkRef{ChunkId: c.ID.Bytes(), Length: c.Length}
	}
	return out
}
