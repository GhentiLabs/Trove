// Package registry is the in-memory peer phone book. It maps node IDs to their
// last-announced candidate addresses with a TTL, bounded so it cannot exhaust
// memory on a small VM. It stores no file data and nothing durable.
package registry

import (
	"errors"
	"sync"
	"time"

	"github.com/GhentiLabs/Trove/pkg/discovery"
)

var (
	// ErrRegistryFull is returned when a new node cannot be admitted because
	// the entry cap is reached.
	ErrRegistryFull = errors.New("registry: full")
	// ErrTooManyAddresses is returned when an announce exceeds the per-node
	// address cap.
	ErrTooManyAddresses = errors.New("registry: too many addresses")
)

// Entry is a stored registration. It is returned by value so callers cannot
// mutate registry state.
type Entry struct {
	NodeID    string
	Addresses []discovery.Address
	LastSeen  time.Time
	ExpiresAt time.Time
}

// Options configure a Registry.
type Options struct {
	MaxEntries      int
	MaxAddrsPerNode int
	SweepInterval   time.Duration // 0 disables the background sweeper
	Clock           func() time.Time
	OnSizeChange    func(size int) // optional metrics hook
}

// Registry is a concurrency-safe TTL map.
type Registry struct {
	mu      sync.RWMutex
	entries map[string]Entry

	maxEntries int
	maxAddrs   int
	clock      func() time.Time
	onSize     func(int)

	stop    chan struct{}
	stopped sync.Once
}

// New constructs a Registry and, if SweepInterval > 0, starts a background
// goroutine that evicts expired entries.
func New(opts Options) *Registry {
	clock := opts.Clock
	if clock == nil {
		clock = time.Now
	}
	r := &Registry{
		entries:    make(map[string]Entry),
		maxEntries: opts.MaxEntries,
		maxAddrs:   opts.MaxAddrsPerNode,
		clock:      clock,
		onSize:     opts.OnSizeChange,
		stop:       make(chan struct{}),
	}
	if opts.SweepInterval > 0 {
		go r.sweepLoop(opts.SweepInterval)
	}
	return r
}

// Announce inserts or refreshes a node's registration with the given TTL and
// returns the stored entry.
func (r *Registry) Announce(nodeID string, addrs []discovery.Address, ttl time.Duration) (Entry, error) {
	if len(addrs) > r.maxAddrs {
		return Entry{}, ErrTooManyAddresses
	}
	now := r.clock()
	entry := Entry{
		NodeID:    nodeID,
		Addresses: append([]discovery.Address(nil), addrs...),
		LastSeen:  now,
		ExpiresAt: now.Add(ttl),
	}

	r.mu.Lock()
	if _, exists := r.entries[nodeID]; !exists && len(r.entries) >= r.maxEntries {
		r.mu.Unlock()
		return Entry{}, ErrRegistryFull
	}
	r.entries[nodeID] = entry
	size := len(r.entries)
	r.mu.Unlock()

	r.reportSize(size)
	return entry, nil
}

// Lookup returns the entry for nodeID if present and unexpired. Expired entries
// are treated as absent (and removed lazily).
func (r *Registry) Lookup(nodeID string) (Entry, bool) {
	now := r.clock()
	r.mu.RLock()
	entry, ok := r.entries[nodeID]
	r.mu.RUnlock()
	if !ok {
		return Entry{}, false
	}
	if !now.Before(entry.ExpiresAt) {
		r.deleteIfExpired(nodeID)
		return Entry{}, false
	}
	return entry, true
}

// Size returns the number of stored entries (including any not yet swept).
func (r *Registry) Size() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.entries)
}

// Close stops the background sweeper. It is safe to call more than once.
func (r *Registry) Close() {
	r.stopped.Do(func() { close(r.stop) })
}

func (r *Registry) deleteIfExpired(nodeID string) {
	now := r.clock()
	r.mu.Lock()
	if e, ok := r.entries[nodeID]; !ok || now.Before(e.ExpiresAt) {
		r.mu.Unlock()
		return
	}
	delete(r.entries, nodeID)
	size := len(r.entries)
	r.mu.Unlock()
	r.reportSize(size)
}

func (r *Registry) sweepLoop(interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-r.stop:
			return
		case <-t.C:
			r.sweep()
		}
	}
}

func (r *Registry) sweep() {
	now := r.clock()
	r.mu.Lock()
	for id, e := range r.entries {
		if !now.Before(e.ExpiresAt) {
			delete(r.entries, id)
		}
	}
	size := len(r.entries)
	r.mu.Unlock()
	r.reportSize(size)
}

func (r *Registry) reportSize(size int) {
	if r.onSize != nil {
		r.onSize(size)
	}
}
