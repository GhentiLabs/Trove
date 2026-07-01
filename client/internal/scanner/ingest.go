package scanner

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"iter"
	"os"
	"path/filepath"
	"sync"

	"github.com/GhentiLabs/Trove/client/internal/hasher"
	"github.com/GhentiLabs/Trove/client/internal/manifest"
	"github.com/GhentiLabs/Trove/client/internal/model"
)

// syncStagingDir is the sync engine's staging directory, excluded from ingestion.
const syncStagingDir = ".trove-tmp"

// scanResult is one path's outcome from a worker. oldChunks holds the chunk ids of
// the path's prior version, so the committer can promote any that this change
// supersedes out of their clone into deduplicated history.
type scanResult struct {
	rel       string
	m         manifest.Manifest
	md        model.Metadata
	oldChunks []hasher.ChunkID
	deleted   bool
	skip      bool
}

// ScanAll walks the whole folder and ingests every changed path. It does not
// detect deletions (that is the periodic rescan's job); it is the startup
// reconcile and the basis for the full rescan. It fails fast if the root itself
// is missing or not a directory — otherwise a transient root outage would scan
// nothing and the rescan would tombstone the entire folder.
func (s *Scanner) ScanAll(ctx context.Context) error {
	fi, err := os.Stat(s.root)
	if err != nil {
		return fmt.Errorf("scanner: scan root: %w", err)
	}
	if !fi.IsDir() {
		return fmt.Errorf("scanner: scan root %q is not a directory", s.root)
	}
	return s.ingest(ctx, s.walkPaths)
}

// ingest runs the folder-relative paths through a bounded, back-pressured
// pipeline: a fixed worker pool hashes and stores changed files, and a single
// committer serializes the model writes. Memory stays flat regardless of how many
// paths are produced.
func (s *Scanner) ingest(ctx context.Context, paths iter.Seq[string]) error {
	pathCh := make(chan string, s.workers*2)
	resCh := make(chan scanResult, s.workers*2)

	var workers sync.WaitGroup
	for range s.workers {
		workers.Go(func() {
			for rel := range pathCh {
				select {
				case resCh <- s.process(ctx, rel):
				case <-ctx.Done():
					return
				}
			}
		})
	}

	var superseded []hasher.ChunkID
	committed := make(chan struct{})
	go func() {
		defer close(committed)
		for res := range resCh {
			superseded = append(superseded, s.commit(ctx, res)...)
		}
	}()

	for rel := range paths {
		select {
		case pathCh <- rel:
		case <-ctx.Done():
			close(pathCh)
			workers.Wait()
			close(resCh)
			<-committed
			return ctx.Err()
		}
	}
	close(pathCh)
	workers.Wait()
	close(resCh)
	<-committed
	if err := s.model.PruneHistoryToFit(ctx); err != nil && !errors.Is(err, model.ErrQuotaExceeded) {
		s.log.Warn("prune history to fit quota", "err", err)
	}
	// Promote once for the whole scan, after every manifest is committed: a chunk
	// that merely moved between two files in this scan is still current and is not
	// promoted, and the per-chunk work does not stall the commit pipeline.
	s.promoteSuperseded(ctx, superseded)
	return ctx.Err()
}

// walkPaths yields every folder-relative path under the root, skipping the root
// itself and unreadable subtrees (logged, not fatal — the rescan retries).
func (s *Scanner) walkPaths(yield func(string) bool) {
	_ = filepath.WalkDir(s.root, func(abs string, d fs.DirEntry, err error) error {
		if err != nil {
			s.log.Warn("walk", "path", abs, "err", err)
			return nil
		}
		if d.IsDir() && d.Name() == syncStagingDir {
			return filepath.SkipDir
		}
		rel, ok := s.relPath(abs)
		if !ok {
			return nil
		}
		if !yield(rel) {
			return filepath.SkipAll
		}
		return nil
	})
}

// process classifies and, if changed, ingests a single path. Transient errors are
// logged and skipped (never mistaken for a deletion); only a missing file becomes
// a tombstone.
func (s *Scanner) process(ctx context.Context, rel string) scanResult {
	abs := filepath.Join(s.root, filepath.FromSlash(rel))
	fi, err := os.Lstat(abs)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return scanResult{rel: rel, deleted: true, oldChunks: s.priorChunks(ctx, rel)}
	case err != nil:
		s.log.Warn("lstat", "path", rel, "err", err)
		return scanResult{skip: true}
	}

	kind, ok := classify(fi.Mode())
	if !ok {
		return scanResult{skip: true}
	}
	if sig, found, err := s.model.Stat(ctx, rel); err == nil && found && !sig.Deleted && unchanged(sig, kind, fi) {
		return scanResult{skip: true}
	}

	m := manifest.Manifest{Kind: kind, Path: rel, Mode: uint32(fi.Mode())}
	switch kind {
	case manifest.KindRegular:
		refs, err := s.chunkFile(ctx, abs)
		if err != nil {
			s.log.Warn("chunk", "path", rel, "err", err)
			return scanResult{skip: true}
		}
		m.Chunks = refs
	case manifest.KindSymlink:
		target, err := os.Readlink(abs)
		if err != nil {
			s.log.Warn("readlink", "path", rel, "err", err)
			return scanResult{skip: true}
		}
		m.SymlinkTarget = target
	}
	return scanResult{rel: rel, m: m, md: model.Metadata{Mtime: fi.ModTime(), Size: fi.Size(), Inode: inode(fi)}, oldChunks: s.priorChunks(ctx, rel)}
}

// priorChunks returns the chunk ids of the path's current manifest, or nil if the
// path is new. Used to find which chunks a change supersedes; skipped entirely for
// a sync-only folder, which keeps no history to promote.
func (s *Scanner) priorChunks(ctx context.Context, rel string) []hasher.ChunkID {
	if !s.keepHistory {
		return nil
	}
	rec, err := s.model.GetManifest(ctx, rel)
	switch {
	case errors.Is(err, model.ErrManifestNotFound):
		return nil
	case err != nil:
		s.log.Warn("prior manifest", "path", rel, "err", err)
		return nil
	}
	return chunkIDs(rec.Manifest.Chunks)
}

// chunkFile stores abs as a clone and returns its ordered chunk references.
func (s *Scanner) chunkFile(ctx context.Context, abs string) ([]manifest.ChunkRef, error) {
	return s.chunks.IngestClone(ctx, abs)
}

// commit applies one path's outcome and returns the chunk ids this change drops
// from the path's prior version (none for a new path or a skip), for the caller to
// promote in one batch after the scan.
func (s *Scanner) commit(ctx context.Context, res scanResult) []hasher.ChunkID {
	switch {
	case res.skip:
		return nil
	case res.deleted:
		if _, err := s.model.DeleteManifest(ctx, res.rel); err != nil {
			if !errors.Is(err, model.ErrManifestNotFound) {
				s.log.Warn("tombstone", "path", res.rel, "err", err)
			}
			return nil
		}
		return res.oldChunks
	default:
		if _, err := s.model.PutManifest(ctx, res.m, res.md); err != nil {
			s.log.Warn("put manifest", "path", res.rel, "err", err)
			return nil
		}
		return hasher.SetMinus(res.oldChunks, chunkIDs(res.m.Chunks))
	}
}

// promoteSuperseded moves the superseded chunks a retained snapshot still keeps out
// of their clone into deduplicated history. Chunks still in a current file, or kept
// by no snapshot, are left alone. A sync-only folder keeps no history, so nothing
// is promoted.
func (s *Scanner) promoteSuperseded(ctx context.Context, superseded []hasher.ChunkID) {
	if !s.keepHistory || len(superseded) == 0 {
		return
	}
	history, err := s.model.SupersededHistory(ctx, superseded)
	if err != nil {
		s.log.Warn("superseded history", "err", err)
		return
	}
	if len(history) == 0 {
		return
	}
	if _, err := s.chunks.Promote(ctx, s.fc, history); err != nil {
		s.log.Warn("promote history", "err", err)
	}
}

func chunkIDs(refs []manifest.ChunkRef) []hasher.ChunkID {
	ids := make([]hasher.ChunkID, len(refs))
	for i, r := range refs {
		ids[i] = r.ID
	}
	return ids
}

func classify(m fs.FileMode) (manifest.Kind, bool) {
	switch {
	case m&fs.ModeSymlink != 0:
		return manifest.KindSymlink, true
	case m.IsDir():
		return manifest.KindDir, true
	case m.IsRegular():
		return manifest.KindRegular, true
	default:
		return 0, false
	}
}

func unchanged(sig model.StatSig, kind manifest.Kind, fi fs.FileInfo) bool {
	return sig.Kind == kind &&
		sig.Mode == uint32(fi.Mode()) &&
		sig.Size == fi.Size() &&
		sig.Mtime.Equal(fi.ModTime()) &&
		sig.Inode == inode(fi)
}
