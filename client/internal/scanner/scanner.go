// Package scanner is the active heart of the local-state layer: it watches a
// folder, ingests changed files into the chunk store and model as content-addressed
// manifests, and cuts a snapshot once edits quiesce. A periodic full rescan is the
// correctness backstop for anything the watcher misses. The scan→hash→store path
// is a bounded, back-pressured pipeline so a large tree never grows work in memory
// proportional to its size.
package scanner

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/chunkstore"
	"github.com/GhentiLabs/Trove/client/internal/manifest"
	"github.com/GhentiLabs/Trove/client/internal/model"
	"github.com/GhentiLabs/Trove/client/internal/watcher"
)

// Default tunable intervals, overridable via Options.
const (
	DefaultDebounceWindow  = 500 * time.Millisecond
	DefaultSnapshotQuiesce = 2 * time.Second
	DefaultRescanInterval  = 5 * time.Minute
	DefaultRescanJitter    = 30 * time.Second
	DefaultWorkers         = 4
)

type reqKind uint8

const (
	reqScan     reqKind = iota // ingest the named (folder-relative) paths
	reqSnapshot                // cut a snapshot of the current state
	reqRescan                  // full reconcile against the working tree
)

type request struct {
	kind  reqKind
	paths []string
}

// Scanner drives one folder's local state.
type Scanner struct {
	root    string
	fc      chunkstore.FolderContext
	chunks  *chunkstore.Store
	model   *model.Store
	watcher watcher.Watcher
	log     *slog.Logger

	debounce   time.Duration
	quiesce    time.Duration
	workers    int
	nextRescan func() time.Duration
}

// Options configures New. Root, Chunks, Model, and Watcher are required; the rest
// default.
type Options struct {
	Root      string
	FolderCtx chunkstore.FolderContext
	Chunks    *chunkstore.Store
	Model     *model.Store
	Watcher   watcher.Watcher
	Logger    *slog.Logger

	DebounceWindow  time.Duration
	SnapshotQuiesce time.Duration
	RescanInterval  time.Duration
	RescanJitter    time.Duration
	Workers         int
}

// New builds a Scanner for one folder.
func New(opts Options) (*Scanner, error) {
	switch {
	case opts.Root == "":
		return nil, errors.New("scanner: empty root")
	case opts.Chunks == nil:
		return nil, errors.New("scanner: nil chunk store")
	case opts.Model == nil:
		return nil, errors.New("scanner: nil model store")
	case opts.Watcher == nil:
		return nil, errors.New("scanner: nil watcher")
	}
	s := &Scanner{
		root:     opts.Root,
		fc:       opts.FolderCtx,
		chunks:   opts.Chunks,
		model:    opts.Model,
		watcher:  opts.Watcher,
		log:      opts.Logger,
		debounce: opts.DebounceWindow,
		quiesce:  opts.SnapshotQuiesce,
		workers:  opts.Workers,
	}
	if s.log == nil {
		s.log = slog.New(slog.DiscardHandler)
	}
	if s.debounce <= 0 {
		s.debounce = DefaultDebounceWindow
	}
	if s.quiesce <= 0 {
		s.quiesce = DefaultSnapshotQuiesce
	}
	if s.workers <= 0 {
		s.workers = DefaultWorkers
	}
	if s.nextRescan == nil {
		interval := opts.RescanInterval
		if interval <= 0 {
			interval = DefaultRescanInterval
		}
		jitter := opts.RescanJitter
		switch {
		case jitter == 0:
			jitter = DefaultRescanJitter // unset: randomize by default to avoid synchronized rescans
		case jitter < 0:
			jitter = 0 // negative: explicitly disable jitter
		}
		s.nextRescan = func() time.Duration {
			if jitter == 0 {
				return interval
			}
			return interval + time.Duration(rand.Int64N(int64(jitter)))
		}
	}
	return s, nil
}

// Run reconciles the folder against the stored model, then watches for changes —
// debouncing edits into ingests, cutting a snapshot once edits quiesce, and
// periodically rescanning as the correctness backstop — until ctx is cancelled.
func (s *Scanner) Run(ctx context.Context) error {
	if err := s.reconcile(ctx); err != nil {
		return err
	}
	reqs := make(chan request, 8)
	var wg sync.WaitGroup
	wg.Go(func() { s.controlLoop(ctx, reqs) })
	wg.Go(func() { s.executorLoop(ctx, reqs) })
	wg.Wait()
	return ctx.Err()
}

// reconcile does a full rescan and snapshots the result. Cut is idempotent, so an
// unchanged tree produces no new snapshot.
func (s *Scanner) reconcile(ctx context.Context) error {
	if err := s.Rescan(ctx); err != nil {
		return err
	}
	if _, err := s.model.Cut(ctx); err != nil {
		return err
	}
	return nil
}

func (s *Scanner) executorLoop(ctx context.Context, reqs <-chan request) {
	for {
		select {
		case <-ctx.Done():
			return
		case r := <-reqs:
			switch r.kind {
			case reqScan:
				if err := s.ingest(ctx, slices.Values(r.paths)); err != nil {
					s.log.Warn("scan failed", "err", err)
				}
			case reqSnapshot:
				if _, err := s.model.Cut(ctx); err != nil {
					s.log.Warn("snapshot failed", "err", err)
				}
			case reqRescan:
				if err := s.reconcile(ctx); err != nil {
					s.log.Warn("rescan failed", "err", err)
				}
			}
		}
	}
}

// controlLoop translates watcher events and timers into scan and snapshot
// requests on out. It owns all timer state, so it needs no locks, and every wait
// is on a channel or a timer, so it is deterministic under testing/synctest.
func (s *Scanner) controlLoop(ctx context.Context, out chan<- request) {
	debounce := time.NewTimer(time.Hour)
	quiesce := time.NewTimer(time.Hour)
	debounce.Stop()
	quiesce.Stop()
	rescan := time.NewTimer(s.nextRescan())
	defer debounce.Stop()
	defer quiesce.Stop()
	defer rescan.Stop()

	dirty := map[string]struct{}{}
	emit := func(r request) {
		select {
		case out <- r:
		case <-ctx.Done():
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-s.watcher.Events():
			rel, ok := s.relPath(ev.Path)
			if !ok {
				continue
			}
			dirty[rel] = struct{}{}
			debounce.Reset(s.debounce)
			quiesce.Reset(s.quiesce)
		case werr := <-s.watcher.Errors():
			s.log.Warn("watcher", "err", werr)
		case <-debounce.C:
			if len(dirty) == 0 {
				continue
			}
			batch := make([]string, 0, len(dirty))
			for p := range dirty {
				batch = append(batch, p)
			}
			clear(dirty)
			emit(request{kind: reqScan, paths: batch})
		case <-quiesce.C:
			emit(request{kind: reqSnapshot})
		case <-rescan.C:
			emit(request{kind: reqRescan})
			rescan.Reset(s.nextRescan())
		}
	}
}

// relPath converts an absolute event path to a folder-relative, NFC, slash path.
// It reports false for paths outside the folder root.
func (s *Scanner) relPath(abs string) (string, bool) {
	rel, err := filepath.Rel(s.root, abs)
	if err != nil || rel == "." || strings.HasPrefix(rel, "..") {
		return "", false
	}
	return manifest.NormalizePath(filepath.ToSlash(rel)), true
}
