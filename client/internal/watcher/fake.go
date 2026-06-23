package watcher

// Fake is an in-memory Watcher for tests. Emit and EmitError inject events that
// the scanner consumes exactly as it would real OS events; because delivery is
// over a channel, a Fake is safe to drive inside a testing/synctest bubble.
type Fake struct {
	events chan Event
	errs   chan error
}

// NewFake returns a Fake with buffered event and error channels.
func NewFake() *Fake {
	return &Fake{events: make(chan Event, 64), errs: make(chan error, 8)}
}

// Events implements Watcher.
func (f *Fake) Events() <-chan Event { return f.events }

// Errors implements Watcher.
func (f *Fake) Errors() <-chan error { return f.errs }

// Close implements Watcher.
func (f *Fake) Close() error { return nil }

// Emit delivers ev to consumers.
func (f *Fake) Emit(ev Event) { f.events <- ev }

// EmitError delivers err to consumers.
func (f *Fake) EmitError(err error) { f.errs <- err }
