package syncengine

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/chunkstore"
	"github.com/GhentiLabs/Trove/client/internal/hasher"
	"github.com/GhentiLabs/Trove/client/internal/manifest"
	"github.com/GhentiLabs/Trove/client/internal/netio"
)

// chunkAttemptTimeout bounds one chunk request to one source before trying the next.
const chunkAttemptTimeout = 15 * time.Second

// Coordinator schedules multi-source chunk pulls for one folder on a replica node. It
// is shared by all the node's sessions for the folder; each session registers its peer
// as a source. Pulls spread across peer sources and fall back to the owner, which
// holds every chunk.
type Coordinator struct {
	folderID string
	fc       chunkstore.FolderContext
	chunks   *chunkstore.Store
	inflight int
	log      *slog.Logger

	mu      sync.Mutex
	sources map[string]netio.Conn

	rotate atomic.Uint64
}

// NewCoordinator builds a coordinator for one folder's chunk store.
func NewCoordinator(folderID string, fc chunkstore.FolderContext, chunks *chunkstore.Store, inflight int, log *slog.Logger) *Coordinator {
	if inflight <= 0 {
		inflight = DefaultInFlight
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Coordinator{
		folderID: folderID, fc: fc, chunks: chunks, inflight: inflight, log: log,
		sources: make(map[string]netio.Conn),
	}
}

func (c *Coordinator) addSource(peerID string, conn netio.Conn) {
	c.mu.Lock()
	c.sources[peerID] = conn
	c.mu.Unlock()
}

func (c *Coordinator) removeSource(peerID string) {
	c.mu.Lock()
	delete(c.sources, peerID)
	c.mu.Unlock()
}

func (c *Coordinator) sourceCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.sources)
}

type srcConn struct {
	peerID string
	conn   netio.Conn
}

// order returns the sources to try for one chunk: non-owner peers first (rotated for
// spread), then the owner as the guaranteed fallback.
func (c *Coordinator) order(ownerID string) []srcConn {
	c.mu.Lock()
	peers := make([]srcConn, 0, len(c.sources))
	var owner srcConn
	haveOwner := false
	for id, conn := range c.sources {
		if id == ownerID {
			owner, haveOwner = srcConn{id, conn}, true
			continue
		}
		peers = append(peers, srcConn{id, conn})
	}
	c.mu.Unlock()

	if n := len(peers); n > 1 {
		k := int(c.rotate.Add(1) % uint64(n))
		peers = append(peers[k:], peers[:k]...)
	}
	if haveOwner {
		peers = append(peers, owner)
	}
	return peers
}

// pull fetches every chunk in refs the replica lacks, spreading across sources and
// falling back to ownerID. The first hard error cancels the rest and is returned.
func (c *Coordinator) pull(ctx context.Context, refs []manifest.ChunkRef, ownerID string) error {
	want, err := c.missing(ctx, refs)
	if err != nil {
		return err
	}
	if len(want) == 0 {
		return nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	sem := make(chan struct{}, c.inflight)
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
			if err := c.fetch(ctx, id, ownerID); err != nil {
				once.Do(func() { firstErr = err; cancel() })
			}
		})
	}
	wg.Wait()
	return firstErr
}

func (c *Coordinator) missing(ctx context.Context, refs []manifest.ChunkRef) ([]hasher.ChunkID, error) {
	seen := make(map[hasher.ChunkID]struct{}, len(refs))
	var want []hasher.ChunkID
	for _, ref := range refs {
		if _, ok := seen[ref.ID]; ok {
			continue
		}
		seen[ref.ID] = struct{}{}
		has, err := c.chunks.Has(ctx, ref.ID)
		if err != nil {
			return nil, fmt.Errorf("syncengine: chunk lookup: %w", err)
		}
		if !has {
			want = append(want, ref.ID)
		}
	}
	return want, nil
}

// fetch tries each source in turn until one delivers the chunk; the owner is always
// the final fallback. A per-attempt timeout re-issues a stalled chunk to the next
// source.
func (c *Coordinator) fetch(ctx context.Context, id hasher.ChunkID, ownerID string) error {
	sources := c.order(ownerID)
	if len(sources) == 0 {
		return fmt.Errorf("syncengine: no sources for chunk %s", id)
	}
	var lastErr error
	for _, src := range sources {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		attempt, cancel := context.WithTimeout(ctx, chunkAttemptTimeout)
		err := c.fetchFrom(attempt, src.conn, id)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
	}
	return lastErr
}

// fetchFrom pulls one chunk from a single source over a fresh data stream, verifies
// it, and stores it. Storing before any manifest references it keeps it above the
// sweep's grace cutoff.
func (c *Coordinator) fetchFrom(ctx context.Context, conn netio.Conn, id hasher.ChunkID) error {
	s, err := conn.OpenStream(ctx)
	if err != nil {
		return fmt.Errorf("syncengine: open data stream: %w", err)
	}
	defer func() { _ = s.Close() }()
	if dl, ok := ctx.Deadline(); ok {
		_ = s.SetReadDeadline(dl)
	}

	if err := writeChunkRequest(s, c.folderID, id); err != nil {
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
	if _, err := c.chunks.Put(ctx, c.fc, buf); err != nil {
		return fmt.Errorf("syncengine: store chunk %s: %w", id, err)
	}
	return nil
}
