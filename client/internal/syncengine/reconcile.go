package syncengine

import (
	"context"
	"fmt"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/hasher"
	"github.com/GhentiLabs/Trove/client/internal/manifest"
	"github.com/GhentiLabs/Trove/client/internal/model"
	"github.com/GhentiLabs/Trove/client/internal/snapshot"
	"github.com/GhentiLabs/Trove/client/internal/wire/wirepb"
)

// trigger records the latest announce and starts a reconcile if one is not already
// running; a concurrent announce just marks the folder dirty so the running loop
// reconciles again with the newest state.
func (fs *folderState) trigger(ctx context.Context, a announce) {
	fs.mu.Lock()
	fs.latest = a
	if fs.busy {
		fs.dirty = true
		fs.mu.Unlock()
		return
	}
	fs.busy = true
	fs.mu.Unlock()
	go fs.reconcileLoop(ctx)
}

func (fs *folderState) reconcileLoop(ctx context.Context) {
	for {
		fs.mu.Lock()
		a := fs.latest
		fs.dirty = false
		fs.mu.Unlock()

		if err := fs.runReconcile(ctx, a); err != nil && ctx.Err() == nil {
			fs.eng.log.Warn("syncengine: reconcile", "folder", fs.cfg.FolderID, "err", err)
		}

		fs.mu.Lock()
		if !fs.dirty || ctx.Err() != nil {
			fs.busy = false
			fs.mu.Unlock()
			return
		}
		fs.mu.Unlock()
	}
}

func (fs *folderState) runReconcile(ctx context.Context, a announce) error {
	m := fs.cfg.Model
	owner := fs.eng.sess.PeerNodeID()

	cur, err := m.CurrentRoot(ctx)
	if err != nil {
		return err
	}
	if cur == a.root {
		ep, hw, ok, err := m.LoadCursor(ctx, fs.cfg.FolderID, owner)
		if err != nil {
			return err
		}
		if !ok || ep != a.epoch || hw != a.highWater {
			if err := m.AdvanceCursor(ctx, fs.cfg.FolderID, owner, a.epoch, a.highWater); err != nil {
				return err
			}
		}
		fs.markConverged(ctx, cur, a.epoch, a.highWater)
		return nil
	}

	ep, hw, ok, err := m.LoadCursor(ctx, fs.cfg.FolderID, owner)
	if err != nil {
		return err
	}
	var since int64
	if ok && ep == a.epoch {
		since = hw
	}

	// Pull the delta a page at a time. The cursor (sequence, chunk-offset) must strictly
	// advance and the page count is capped, so a buggy or hostile owner cannot loop the
	// replica forever. A manifest whose chunk list spans pages is accumulated in carry
	// and applied only once fully received.
	var offset int64
	var carry *carryState
	for pages := 0; ; pages++ {
		if pages >= maxDeltaPages {
			return fmt.Errorf("syncengine: delta exceeded %d pages for %q", maxDeltaPages, fs.cfg.FolderID)
		}
		res, err := fs.applyPage(ctx, a.epoch, since, offset, carry)
		if err != nil {
			return err
		}
		if res.complete {
			root, err := m.CurrentRoot(ctx)
			if err != nil {
				return err
			}
			fs.markConverged(ctx, root, a.epoch, res.since)
			return nil
		}
		if res.since < since || (res.since == since && res.offset <= offset) {
			return fmt.Errorf("syncengine: delta cursor did not advance past (%d,%d) for %q", since, offset, fs.cfg.FolderID)
		}
		since, offset, carry = res.since, res.offset, res.carry
	}
}

// markConverged records that this replica reached root at (epoch, highWater) and reports
// it to the owner. Both writes are best-effort; a dropped receipt is re-sent on the next
// reconcile, so it never fails the reconcile.
func (fs *folderState) markConverged(ctx context.Context, root snapshot.Root, epoch uint64, highWater int64) {
	owner := fs.eng.sess.PeerNodeID()
	if err := fs.cfg.Model.RecordReceipt(ctx, model.LocalSync, model.Receipt{
		PeerID: owner, Root: root, Epoch: epoch, HighWater: highWater, SyncedAt: time.Now(),
	}); err != nil {
		fs.eng.log.Warn("syncengine: record receipt", "folder", fs.cfg.FolderID, "err", err)
	}
	if err := fs.eng.sess.Send(&wirepb.SyncReceipt{
		FolderId:          fs.cfg.FolderID,
		SnapshotRoot:      root.Bytes(),
		IndexEpochId:      epoch,
		HighWaterSequence: highWater,
	}); err != nil {
		fs.eng.log.Debug("syncengine: send receipt", "folder", fs.cfg.FolderID, "peer", owner, "err", err)
	}
}

// carryState accumulates the chunk refs of a manifest whose list spans several pages,
// until the final page completes it.
type carryState struct {
	meta   *wirepb.RemoteManifest
	chunks []*wirepb.ChunkRef
}

type pageResult struct {
	since    int64
	offset   int64
	carry    *carryState
	complete bool
}

func (fs *folderState) applyPage(ctx context.Context, epoch uint64, since, offset int64, carry *carryState) (pageResult, error) {
	ctx, cancel := context.WithTimeout(ctx, deltaTimeout)
	defer cancel()

	delta, err := fs.request(ctx, epoch, since, offset)
	if err != nil {
		return pageResult{}, err
	}
	wms := delta.GetManifests()
	newSince := delta.GetHighWaterSequence()
	owner := fs.eng.sess.PeerNodeID()

	// A continuation page carries the single in-progress manifest's next chunk slice; its
	// chunks are pulled now but the file is applied only when the manifest completes.
	if offset > 0 || carry != nil || (len(wms) == 1 && wms[0].GetMoreChunks()) {
		if len(wms) == 0 {
			return pageResult{since: newSince, offset: delta.GetHighWaterChunkOffset(), complete: delta.GetComplete()}, nil
		}
		wm := wms[0]
		_, pull, err := convertManifests([]*wirepb.RemoteManifest{wm})
		if err != nil {
			return pageResult{}, err
		}
		if err := fs.cfg.Coord.pull(ctx, pull, owner); err != nil {
			return pageResult{}, err
		}
		if carry == nil {
			carry = &carryState{meta: wm}
		}
		carry.chunks = append(carry.chunks, wm.GetChunks()...)
		if wm.GetMoreChunks() {
			return pageResult{since: newSince, offset: delta.GetHighWaterChunkOffset(), carry: carry, complete: false}, nil
		}
		carry.meta.Chunks = carry.chunks
		carry.meta.MoreChunks = false
		batch, _, err := convertManifests([]*wirepb.RemoteManifest{carry.meta})
		if err != nil {
			return pageResult{}, err
		}
		if err := fs.apply(ctx, batch, delta); err != nil {
			return pageResult{}, err
		}
		return pageResult{since: newSince, complete: delta.GetComplete()}, nil
	}

	batch, pull, err := convertManifests(wms)
	if err != nil {
		return pageResult{}, err
	}
	if err := fs.cfg.Coord.pull(ctx, pull, owner); err != nil {
		return pageResult{}, err
	}
	if err := fs.apply(ctx, batch, delta); err != nil {
		return pageResult{}, err
	}
	return pageResult{since: newSince, complete: delta.GetComplete()}, nil
}

// request registers a one-shot reply sink, sends a ManifestRequest, and waits for
// the matching ManifestDelta.
func (fs *folderState) request(ctx context.Context, epoch uint64, since, offset int64) (*wirepb.ManifestDelta, error) {
	ch := make(chan *wirepb.ManifestDelta, 1)
	fs.mu.Lock()
	fs.reply = ch
	fs.mu.Unlock()
	defer func() {
		fs.mu.Lock()
		fs.reply = nil
		fs.mu.Unlock()
	}()

	req := &wirepb.ManifestRequest{FolderId: fs.cfg.FolderID, IndexEpochId: epoch, SinceSequence: since, SinceChunkOffset: offset}
	if err := fs.eng.sess.Send(req); err != nil {
		return nil, fmt.Errorf("syncengine: send manifest request: %w", err)
	}
	select {
	case d := <-ch:
		return d, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// deliver hands a ManifestDelta to the waiting reconcile, dropping it if none is
// outstanding (at most one request per folder is ever in flight).
func (fs *folderState) deliver(d *wirepb.ManifestDelta) {
	fs.mu.Lock()
	ch := fs.reply
	fs.mu.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- d:
	default:
	}
}

// convertManifests decodes a delta into the model apply batch and the chunks to pull.
func convertManifests(wms []*wirepb.RemoteManifest) ([]model.RemoteManifest, []manifest.ChunkRef, error) {
	batch := make([]model.RemoteManifest, 0, len(wms))
	var pull []manifest.ChunkRef
	for _, wm := range wms {
		chunks := make([]manifest.ChunkRef, len(wm.GetChunks()))
		for i, c := range wm.GetChunks() {
			id, err := hasher.FromBytes(c.GetChunkId())
			if err != nil {
				return nil, nil, fmt.Errorf("syncengine: chunk id: %w", err)
			}
			chunks[i] = manifest.ChunkRef{ID: id, Length: c.GetLength()}
		}
		id, err := manifest.IDFromBytes(wm.GetManifestId())
		if err != nil {
			return nil, nil, fmt.Errorf("syncengine: manifest id: %w", err)
		}
		vv, err := parseVector(wm.GetVersionVector())
		if err != nil {
			return nil, nil, fmt.Errorf("syncengine: version vector: %w", err)
		}
		m := manifest.Manifest{
			Kind:          manifest.Kind(wm.GetKind()),
			Path:          wm.GetPath(),
			Mode:          wm.GetMode(),
			SymlinkTarget: wm.GetSymlinkTarget(),
			Chunks:        chunks,
		}
		rm := model.RemoteManifest{
			Manifest:   m,
			ID:         id,
			Version:    vv,
			Author:     wm.GetAuthor(),
			AuthoredAt: time.UnixMilli(wm.GetAuthoredMs()),
			Deleted:    wm.GetDeleted(),
			Metadata:   model.Metadata{Size: totalLength(chunks), Mtime: time.Now()},
		}
		if rm.Deleted {
			rm.DeletedAt = time.UnixMilli(wm.GetDeletedMs())
			if wm.GetDeletedMs() == 0 {
				rm.DeletedAt = time.Now() // a missing timestamp must not expire in the past
			}
		}
		batch = append(batch, rm)
		if !rm.Deleted && m.Kind == manifest.KindRegular {
			pull = append(pull, chunks...)
		}
	}
	return batch, pull, nil
}

func parseVector(b []byte) (manifest.VersionVector, error) {
	if len(b) == 0 {
		return manifest.VersionVector{}, nil
	}
	return manifest.ParseVector(b)
}

func totalLength(chunks []manifest.ChunkRef) int64 {
	var n int64
	for _, c := range chunks {
		n += c.Length
	}
	return n
}
