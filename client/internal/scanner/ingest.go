package scanner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"os"
	"path/filepath"
	"sync"

	"github.com/GhentiLabs/Trove/client/internal/chunker"
	"github.com/GhentiLabs/Trove/client/internal/manifest"
	"github.com/GhentiLabs/Trove/client/internal/model"
)

// scanResult is one path's outcome from a worker.
type scanResult struct {
	rel     string
	m       manifest.Manifest
	md      model.Metadata
	deleted bool
	skip    bool
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

	committed := make(chan struct{})
	go func() {
		defer close(committed)
		for res := range resCh {
			s.commit(ctx, res)
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
	return ctx.Err()
}

// walkPaths yields every folder-relative path under the root, skipping the root
// itself and unreadable subtrees (logged, not fatal — the rescan retries).
func (s *Scanner) walkPaths(yield func(string) bool) {
	_ = filepath.WalkDir(s.root, func(abs string, _ fs.DirEntry, err error) error {
		if err != nil {
			s.log.Warn("walk", "path", abs, "err", err)
			return nil
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
		return scanResult{rel: rel, deleted: true}
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
	return scanResult{rel: rel, m: m, md: model.Metadata{Mtime: fi.ModTime(), Size: fi.Size(), Inode: inode(fi)}}
}

// chunkFile streams abs through the chunker, stores each chunk physically, and
// returns the ordered chunk references. Streaming bounds memory to one chunk at a
// time per worker.
func (s *Scanner) chunkFile(ctx context.Context, abs string) ([]manifest.ChunkRef, error) {
	f, err := os.Open(abs)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	c := chunker.New(chunker.Options{Reader: f})
	var refs []manifest.ChunkRef
	for {
		_, data, err := c.NextChunk()
		if errors.Is(err, io.EOF) {
			return refs, nil
		}
		if err != nil {
			return nil, err
		}
		id, err := s.chunks.Put(ctx, s.fc, data)
		if err != nil {
			return nil, err
		}
		refs = append(refs, manifest.ChunkRef{ID: id, Length: int64(len(data))})
	}
}

func (s *Scanner) commit(ctx context.Context, res scanResult) {
	switch {
	case res.skip:
		return
	case res.deleted:
		if _, err := s.model.DeleteManifest(ctx, res.rel); err != nil && !errors.Is(err, model.ErrManifestNotFound) {
			s.log.Warn("tombstone", "path", res.rel, "err", err)
		}
	default:
		if _, err := s.model.PutManifest(ctx, res.m, res.md); err != nil {
			s.log.Warn("put manifest", "path", res.rel, "err", err)
		}
	}
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
