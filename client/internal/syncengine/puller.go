package syncengine

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/GhentiLabs/Trove/client/internal/hasher"
	"github.com/GhentiLabs/Trove/client/internal/manifest"
)

// pull fetches every chunk in refs the replica does not already hold, from the owner,
// over data streams bounded by the in-flight window. The first error cancels the rest.
func (e *Engine) pull(ctx context.Context, fs *folderState, refs []manifest.ChunkRef) error {
	want, err := e.missingChunks(ctx, fs, refs)
	if err != nil {
		return err
	}
	if len(want) == 0 {
		return nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	sem := make(chan struct{}, e.inflight)
	var wg sync.WaitGroup
	var once sync.Once
	var firstErr error

	for _, id := range want {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			wg.Wait()
			if firstErr != nil {
				return firstErr
			}
			return ctx.Err()
		}
		wg.Go(func() {
			defer func() { <-sem }()
			if err := e.fetchOne(ctx, fs, id); err != nil {
				once.Do(func() { firstErr = err; cancel() })
			}
		})
	}
	wg.Wait()
	return firstErr
}

// missingChunks returns the deduplicated chunk ids in refs not already in the store.
func (e *Engine) missingChunks(ctx context.Context, fs *folderState, refs []manifest.ChunkRef) ([]hasher.ChunkID, error) {
	seen := make(map[hasher.ChunkID]struct{}, len(refs))
	var want []hasher.ChunkID
	for _, ref := range refs {
		if _, ok := seen[ref.ID]; ok {
			continue
		}
		seen[ref.ID] = struct{}{}
		has, err := fs.cfg.Chunks.Has(ctx, ref.ID)
		if err != nil {
			return nil, fmt.Errorf("syncengine: chunk lookup: %w", err)
		}
		if !has {
			want = append(want, ref.ID)
		}
	}
	return want, nil
}

// fetchOne pulls and stores one verified chunk on a fresh data stream. Storing
// before any manifest references it keeps it above the sweep's grace cutoff.
func (e *Engine) fetchOne(ctx context.Context, fs *folderState, id hasher.ChunkID) error {
	s, err := e.sess.Conn().OpenStream(ctx)
	if err != nil {
		return fmt.Errorf("syncengine: open data stream: %w", err)
	}
	defer func() { _ = s.Close() }()

	if err := writeChunkRequest(s, fs.cfg.FolderID, id); err != nil {
		return err
	}
	status, length, err := readChunkResponseHeader(s)
	if err != nil {
		return err
	}
	if status != StatusOK {
		return fmt.Errorf("%w: %s (status %d)", errChunkUnavailable, id, status)
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(s, buf); err != nil {
		return fmt.Errorf("syncengine: read chunk %s: %w", id, err)
	}
	if hasher.Sum(buf) != id {
		return fmt.Errorf("%w: %s", errChunkVerify, id)
	}
	if _, err := fs.cfg.Chunks.Put(ctx, fs.cfg.FolderCtx, buf); err != nil {
		return fmt.Errorf("syncengine: store chunk %s: %w", id, err)
	}
	return nil
}
