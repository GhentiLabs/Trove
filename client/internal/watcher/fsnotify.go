package watcher

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/fsnotify/fsnotify"
)

// eventBufferSize and errorBufferSize bound how many unconsumed watcher
// notifications are held before fsnotify overflow drops them; the periodic
// rescan is the backstop.
const (
	eventBufferSize = 256
	errorBufferSize = 16
)

// fsWatcher is the production Watcher backed by fsnotify (inotify on Linux,
// kqueue on macOS). fsnotify is not recursive, so it adds a watch per directory
// and watches new directories as they appear. Watch gaps — kqueue's per-file
// descriptors, inotify's watch limit, dropped overflow events — are tolerated
// because the scanner's periodic rescan is the correctness backstop.
type fsWatcher struct {
	w      *fsnotify.Watcher
	events chan Event
	errs   chan error
	done   chan struct{}
	closed chan struct{}
	once   sync.Once
}

// New returns a recursive Watcher rooted at root.
func New(root string) (Watcher, error) {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("watcher: new: %w", err)
	}
	fw := &fsWatcher{
		w:      w,
		events: make(chan Event, eventBufferSize),
		errs:   make(chan error, errorBufferSize),
		done:   make(chan struct{}),
		closed: make(chan struct{}),
	}
	if err := addTree(w, root); err != nil {
		_ = w.Close()
		return nil, err
	}
	go fw.loop()
	return fw, nil
}

func (fw *fsWatcher) Events() <-chan Event { return fw.events }
func (fw *fsWatcher) Errors() <-chan error { return fw.errs }

// Close stops watching and waits for the forwarding goroutine to exit, so no
// goroutine outlives a returned Close. It is idempotent.
func (fw *fsWatcher) Close() error {
	var err error
	fw.once.Do(func() {
		close(fw.done)
		err = fw.w.Close()
		<-fw.closed
	})
	return err
}

func (fw *fsWatcher) loop() {
	defer close(fw.closed)
	for {
		select {
		case ev, ok := <-fw.w.Events:
			if !ok {
				return
			}
			if ev.Op&fsnotify.Create != 0 {
				if fi, err := os.Stat(ev.Name); err == nil && fi.IsDir() {
					_ = addTree(fw.w, ev.Name) // best effort; rescan covers any gap
				}
			}
			fw.forward(Event{Path: ev.Name, Op: mapOp(ev.Op)})
		case err, ok := <-fw.w.Errors:
			if !ok {
				return
			}
			fw.forwardErr(err)
		case <-fw.done:
			return
		}
	}
}

func (fw *fsWatcher) forward(ev Event) {
	select {
	case fw.events <- ev:
	case <-fw.done:
	}
}

func (fw *fsWatcher) forwardErr(err error) {
	select {
	case fw.errs <- err:
	case <-fw.done:
	}
}

// addTree adds a watch for dir and every directory beneath it. A failure to walk
// dir itself is fatal (the caller asked to watch it); unreadable subtrees beneath
// are tolerated, since the periodic rescan backstops them.
func addTree(w *fsnotify.Watcher, dir string) error {
	return filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if path == dir {
				return fmt.Errorf("watcher: walk %s: %w", dir, err)
			}
			return nil
		}
		if !d.IsDir() {
			return nil
		}
		if err := w.Add(path); err != nil {
			return fmt.Errorf("watcher: add %s: %w", path, err)
		}
		return nil
	})
}

func mapOp(op fsnotify.Op) Op {
	var out Op
	if op&fsnotify.Create != 0 {
		out |= OpCreate
	}
	if op&fsnotify.Write != 0 {
		out |= OpWrite
	}
	if op&fsnotify.Remove != 0 {
		out |= OpRemove
	}
	if op&fsnotify.Rename != 0 {
		out |= OpRename
	}
	if op&fsnotify.Chmod != 0 {
		out |= OpChmod
	}
	return out
}
