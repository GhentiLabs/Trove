// Package watcher abstracts the OS file-system notifier the scanner consumes. The
// OS watcher cannot be driven by testing/synctest, so it is the one genuine test
// seam in the local-state layer: tests use Fake, production uses the fsnotify
// backend. Events are advisory — the scanner's periodic full rescan is the
// correctness backstop, so a missed or coalesced event is never fatal.
package watcher

// Op is the kind of change an Event reports. Values are a bitmask so a backend can
// report several at once; the scanner treats any of them as "this path may have
// changed" and re-stats to decide.
type Op uint8

const (
	OpCreate Op = 1 << iota
	OpWrite
	OpRemove
	OpRename
	OpChmod
)

// Event is one change notification for an absolute filesystem path.
type Event struct {
	Path string
	Op   Op
}

// Watcher delivers file-system change events for the trees it is watching.
type Watcher interface {
	// Events is the stream of change notifications.
	Events() <-chan Event
	// Errors is the stream of non-fatal watcher errors (e.g. a dropped watch).
	Errors() <-chan error
	// Close stops watching and releases resources.
	Close() error
}
