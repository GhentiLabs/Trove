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
	InFlight      int
	MaxDeltaBytes int
}

// Engine runs one-way sync for one session's shared folders.
type Engine struct {
	sess          *session.Session
	log           *slog.Logger
	inflight      int
	maxDeltaBytes int
	folders       map[string]*folderState
	ownsAny       bool
	serveSem      chan struct{}
	manifestSem   chan struct{}

	servedChunks atomic.Int64
	servedDeltas atomic.Int64
}

type folderState struct {
	eng *Engine
	cfg FolderConfig

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

// New builds an Engine for the given session and folders.
func New(opts Options) (*Engine, error) {
	if opts.Session == nil {
		return nil, ErrNoSession
	}
	log := opts.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	inflight := opts.InFlight
	if inflight <= 0 {
		inflight = DefaultInFlight
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
		inflight:      inflight,
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
		e.folders[fc.FolderID] = &folderState{eng: e, cfg: fc}
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
			_ = os.RemoveAll(filepath.Join(fs.cfg.Root, tmpDirName))
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
	}
	return nil
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

// buildDelta returns one page of manifests with owner_sequence greater than the
// request cursor, capped at maxDeltaBytes; complete is false when more remain.
func (e *Engine) buildDelta(ctx context.Context, fs *folderState, req *wirepb.ManifestRequest) (*wirepb.ManifestDelta, error) {
	epoch, err := fs.cfg.Model.FolderEpoch(ctx)
	if err != nil {
		return nil, err
	}
	since := req.GetSinceSequence()
	if req.GetIndexEpochId() != epoch {
		since = 0
	}
	recs, err := fs.cfg.Model.ManifestsSince(ctx, since)
	if err != nil {
		return nil, err
	}
	manifests := make([]*wirepb.RemoteManifest, 0, len(recs))
	pageHigh := since
	complete := true
	size := 0
	for _, rec := range recs {
		rm := recordToWire(rec)
		s := proto.Size(rm) + protoRepeatedOverhead
		if size+s > e.maxDeltaBytes {
			if len(manifests) == 0 {
				// A single manifest too large for one control frame would be sent as an
				// undeliverable page that the replica rejects and re-requests forever.
				// Fail loudly instead; splitting a manifest's chunk refs across pages is
				// the real fix (large-file follow-up).
				return nil, fmt.Errorf("syncengine: manifest %q (%d bytes) exceeds the delta page cap", rec.Manifest.Path, s)
			}
			complete = false
			break
		}
		manifests = append(manifests, rm)
		size += s
		pageHigh = rec.Seq
	}
	return &wirepb.ManifestDelta{
		FolderId:          fs.cfg.FolderID,
		IndexEpochId:      epoch,
		HighWaterSequence: pageHigh,
		Manifests:         manifests,
		Complete:          complete,
	}, nil
}

// DeltasSent is the number of manifest-delta pages this engine has sent as an owner.
func (e *Engine) DeltasSent() int64 { return e.servedDeltas.Load() }

func recordToWire(rec model.Record) *wirepb.RemoteManifest {
	chunks := make([]*wirepb.ChunkRef, 0, len(rec.Manifest.Chunks))
	for _, c := range rec.Manifest.Chunks {
		chunks = append(chunks, &wirepb.ChunkRef{ChunkId: c.ID.Bytes(), Length: c.Length})
	}
	rm := &wirepb.RemoteManifest{
		Kind:          uint32(rec.Manifest.Kind),
		Path:          rec.Manifest.Path,
		Mode:          rec.Manifest.Mode,
		SymlinkTarget: rec.Manifest.SymlinkTarget,
		Chunks:        chunks,
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
