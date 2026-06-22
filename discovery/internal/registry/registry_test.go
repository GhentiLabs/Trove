package registry

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GhentiLabs/Trove/pkg/discovery"
)

func addrs(n int) []discovery.Address {
	out := make([]discovery.Address, n)
	for i := range out {
		out[i] = discovery.Address{IP: "10.0.0.1", Port: 1000 + i, Type: discovery.AddressLAN}
	}
	return out
}

func TestAnnounceAndLookup(t *testing.T) {
	r := New(Options{MaxEntries: 10, MaxAddrsPerNode: 8})
	defer r.Close()

	if _, err := r.Announce("node-a", addrs(2), time.Minute); err != nil {
		t.Fatalf("Announce: %v", err)
	}
	e, ok := r.Lookup("node-a")
	if !ok {
		t.Fatal("expected to find node-a")
	}
	if len(e.Addresses) != 2 {
		t.Fatalf("unexpected entry: %+v", e)
	}
	if _, ok := r.Lookup("missing"); ok {
		t.Fatal("did not expect to find missing node")
	}
}

func TestLookupStoredCopyIsolated(t *testing.T) {
	r := New(Options{MaxEntries: 10, MaxAddrsPerNode: 8})
	defer r.Close()
	in := addrs(1)
	if _, err := r.Announce("n", in, time.Minute); err != nil {
		t.Fatal(err)
	}
	in[0].Port = 65000 // mutate caller's slice
	e, _ := r.Lookup("n")
	if e.Addresses[0].Port == 65000 {
		t.Fatal("registry retained caller's backing array")
	}
}

func TestTTLExpiryLazy(t *testing.T) {
	now := time.Unix(1000, 0)
	clock := func() time.Time { return now }
	r := New(Options{MaxEntries: 10, MaxAddrsPerNode: 8, Clock: clock})
	defer r.Close()

	if _, err := r.Announce("n", addrs(1), time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.Lookup("n"); !ok {
		t.Fatal("should be present before expiry")
	}
	now = now.Add(time.Minute) // exactly at expiry => expired
	if _, ok := r.Lookup("n"); ok {
		t.Fatal("should be gone at/after expiry")
	}
	if r.Size() != 0 {
		t.Fatalf("expired entry not removed lazily, size=%d", r.Size())
	}
}

func TestLazyDeleteSparesReannouncedEntry(t *testing.T) {
	now := time.Unix(1000, 0)
	clock := func() time.Time { return now }
	r := New(Options{MaxEntries: 10, MaxAddrsPerNode: 8, Clock: clock})
	defer r.Close()

	if _, err := r.Announce("n", addrs(1), time.Minute); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	if _, err := r.Announce("n", addrs(1), time.Minute); err != nil {
		t.Fatal(err)
	}
	r.deleteIfExpired("n")
	if _, ok := r.Lookup("n"); !ok {
		t.Fatal("re-announced entry was dropped by a stale lazy delete")
	}
}

func TestSweeperEvictsExpired(t *testing.T) {
	now := time.Unix(0, 0)
	var mu sync.Mutex
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	r := New(Options{MaxEntries: 10, MaxAddrsPerNode: 8, SweepInterval: time.Millisecond, Clock: clock})
	defer r.Close()

	if _, err := r.Announce("n", addrs(1), time.Second); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	now = now.Add(2 * time.Second)
	mu.Unlock()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if r.Size() == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("sweeper did not evict, size=%d", r.Size())
}

func TestAddressCap(t *testing.T) {
	r := New(Options{MaxEntries: 10, MaxAddrsPerNode: 2})
	defer r.Close()
	if _, err := r.Announce("n", addrs(3), time.Minute); !errors.Is(err, ErrTooManyAddresses) {
		t.Fatalf("err = %v, want ErrTooManyAddresses", err)
	}
}

func TestEntryCap(t *testing.T) {
	r := New(Options{MaxEntries: 2, MaxAddrsPerNode: 4})
	defer r.Close()
	for i := range 2 {
		if _, err := r.Announce(fmt.Sprintf("n%d", i), addrs(1), time.Minute); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := r.Announce("overflow", addrs(1), time.Minute); !errors.Is(err, ErrRegistryFull) {
		t.Fatalf("err = %v, want ErrRegistryFull", err)
	}
	// Refreshing an existing node must still succeed at capacity.
	if _, err := r.Announce("n0", addrs(1), time.Minute); err != nil {
		t.Fatalf("refresh at capacity failed: %v", err)
	}
}

func TestConcurrentAccess(t *testing.T) {
	r := New(Options{MaxEntries: 1000, MaxAddrsPerNode: 4})
	defer r.Close()
	var wg sync.WaitGroup
	var errs atomic.Int64
	for i := range 50 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("n%d", i)
			for range 100 {
				if _, err := r.Announce(id, addrs(2), time.Minute); err != nil {
					errs.Add(1)
				}
				r.Lookup(id)
				r.Size()
			}
		}(i)
	}
	wg.Wait()
	if errs.Load() != 0 {
		t.Fatalf("%d concurrent announce errors", errs.Load())
	}
}

func TestSizeHook(t *testing.T) {
	var last atomic.Int64
	r := New(Options{MaxEntries: 10, MaxAddrsPerNode: 4, OnSizeChange: func(s int) { last.Store(int64(s)) }})
	defer r.Close()
	if _, err := r.Announce("n", addrs(1), time.Minute); err != nil {
		t.Fatal(err)
	}
	if last.Load() != 1 {
		t.Fatalf("size hook = %d, want 1", last.Load())
	}
}
