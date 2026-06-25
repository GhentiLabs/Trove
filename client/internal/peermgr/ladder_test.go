package peermgr

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/discovery"
	"github.com/GhentiLabs/Trove/client/internal/netio"
	disco "github.com/GhentiLabs/Trove/pkg/discovery"
)

type stubConn struct{ id string }

func (stubConn) OpenStream(context.Context) (netio.Stream, error)   { return nil, nil }
func (stubConn) AcceptStream(context.Context) (netio.Stream, error) { return nil, nil }
func (c stubConn) PeerNodeID() string                               { return c.id }
func (stubConn) Close() error                                       { return nil }

const lo = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" // < hi
const hi = "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"

func TestLadderDialsCachedAddrFirst(t *testing.T) {
	cache := discovery.NewCache()
	cache.Put(hi, "198.51.100.9:22000")
	var dialed string
	l := NewLadder(LadderConfig{
		Self:  lo,
		Cache: cache,
		Dial: func(_ context.Context, addr, nodeID string) (netio.Conn, error) {
			dialed = addr
			return stubConn{id: nodeID}, nil
		},
		Lookup: func(context.Context, string) ([]string, error) {
			t.Fatal("lookup should not be called")
			return nil, nil
		},
	})
	c, err := l.Connect(context.Background(), hi)
	if err != nil || c.PeerNodeID() != hi {
		t.Fatalf("Connect = %v, %v", c, err)
	}
	if dialed != "198.51.100.9:22000" {
		t.Fatalf("dialed %q, want cached addr", dialed)
	}
}

func TestLadderLookupAndCachesOnSuccess(t *testing.T) {
	cache := discovery.NewCache()
	l := NewLadder(LadderConfig{
		Self:  lo,
		Cache: cache,
		Dial: func(_ context.Context, addr, nodeID string) (netio.Conn, error) {
			if addr == "bad:1" {
				return nil, errors.New("unreachable")
			}
			return stubConn{id: nodeID}, nil
		},
		Lookup: func(context.Context, string) ([]string, error) {
			return []string{"bad:1", "198.51.100.9:22000"}, nil
		},
	})
	if _, err := l.Connect(context.Background(), hi); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if addr, ok := cache.Get(hi); !ok || addr != "198.51.100.9:22000" {
		t.Fatalf("working addr not cached: %q %v", addr, ok)
	}
}

func TestLadderHolepunchDialerDialsWithoutProbe(t *testing.T) {
	cache := discovery.NewCache()
	var probed, dialed atomic.Bool
	punchAt := time.Now().Add(40 * time.Millisecond).UnixMilli()
	l := NewLadder(LadderConfig{
		Self:       lo, // lo < hi, so we are the dialer
		Cache:      cache,
		Candidates: func() []disco.Address { return nil },
		Lookup:     func(context.Context, string) ([]string, error) { return nil, errors.New("not found") },
		Signal: func(_ context.Context, _ string, _ []disco.Address) (disco.PeerCandidates, error) {
			return disco.PeerCandidates{Candidates: []disco.Address{{IP: "203.0.113.7", Port: 22000, Type: disco.AddressPublic}}, PunchAtMillis: punchAt}, nil
		},
		Probe: func(context.Context, []string) error { probed.Store(true); return nil },
		Dial: func(_ context.Context, _, nodeID string) (netio.Conn, error) {
			dialed.Store(true)
			return stubConn{id: nodeID}, nil
		},
	})
	c, err := l.Connect(context.Background(), hi)
	if err != nil || c == nil {
		t.Fatalf("holepunch dialer Connect = %v, %v", c, err)
	}
	if !dialed.Load() {
		t.Fatal("dialer did not dial")
	}
	if probed.Load() {
		t.Fatal("dialer sent a raw probe; its QUIC dial should be the only punch packet")
	}
}

// TestLadderHolepunchWaitsForPunchTime proves the dialer holds its dial until the
// server-brokered punch time, so both sides' first packets cross mid-path. synctest
// gives a virtual clock, so the wait is asserted deterministically with no real
// sleep and no flakiness.
func TestLadderHolepunchWaitsForPunchTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const delay = 5 * time.Second
		start := time.Now()
		var dialedAfter time.Duration
		l := NewLadder(LadderConfig{
			Self:       lo, // lo < hi, so we are the dialer
			Cache:      discovery.NewCache(),
			Candidates: func() []disco.Address { return nil },
			Lookup:     func(context.Context, string) ([]string, error) { return nil, errors.New("not found") },
			Signal: func(context.Context, string, []disco.Address) (disco.PeerCandidates, error) {
				return disco.PeerCandidates{
					Candidates:    []disco.Address{{IP: "203.0.113.7", Port: 22000, Type: disco.AddressPublic}},
					PunchAtMillis: time.Now().Add(delay).UnixMilli(),
				}, nil
			},
			Probe: func(context.Context, []string) error { return nil },
			Dial: func(_ context.Context, _, nodeID string) (netio.Conn, error) {
				dialedAfter = time.Since(start)
				return stubConn{id: nodeID}, nil
			},
		})
		if _, err := l.Connect(context.Background(), hi); err != nil {
			t.Fatalf("Connect: %v", err)
		}
		if dialedAfter < delay {
			t.Fatalf("dialed after %v, want >= brokered punch delay %v", dialedAfter, delay)
		}
	})
}

// TestLadderHolepunchDialerRetriesBounded proves the dialer makes exactly
// maxPunchAttempts coordinated rounds, re-signalling once per round (so the acceptor
// re-probes in lockstep), then gives up with errPunchMissed rather than looping.
func TestLadderHolepunchDialerRetriesBounded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var signals, dials atomic.Int32
		l := NewLadder(LadderConfig{
			Self:       lo,
			Cache:      discovery.NewCache(),
			Candidates: func() []disco.Address { return nil },
			Lookup:     func(context.Context, string) ([]string, error) { return nil, errors.New("not found") },
			Signal: func(context.Context, string, []disco.Address) (disco.PeerCandidates, error) {
				signals.Add(1)
				return disco.PeerCandidates{
					Candidates:    []disco.Address{{IP: "203.0.113.7", Port: 22000, Type: disco.AddressPublic}},
					PunchAtMillis: time.Now().UnixMilli(),
				}, nil
			},
			Probe: func(context.Context, []string) error { return nil },
			Dial: func(context.Context, string, string) (netio.Conn, error) {
				dials.Add(1)
				return nil, errors.New("punch missed")
			},
		})
		_, err := l.Connect(context.Background(), hi)
		if !errors.Is(err, errPunchMissed) {
			t.Fatalf("Connect err = %v, want errPunchMissed", err)
		}
		if got := signals.Load(); got != maxPunchAttempts {
			t.Fatalf("signal rounds = %d, want %d (one re-signal per round)", got, maxPunchAttempts)
		}
		if got := dials.Load(); got != maxPunchAttempts {
			t.Fatalf("dial attempts = %d, want %d", got, maxPunchAttempts)
		}
	})
}

func TestLadderHolepunchAcceptorProbesAndAwaitsInbound(t *testing.T) {
	var probed atomic.Bool
	l := NewLadder(LadderConfig{
		Self:       hi, // hi > lo, so we accept rather than dial
		Cache:      discovery.NewCache(),
		Candidates: func() []disco.Address { return nil },
		Lookup:     func(context.Context, string) ([]string, error) { return nil, errors.New("not found") },
		Signal: func(_ context.Context, _ string, _ []disco.Address) (disco.PeerCandidates, error) {
			return disco.PeerCandidates{
				Candidates:    []disco.Address{{IP: "203.0.113.1", Port: 22000, Type: disco.AddressPublic}},
				PunchAtMillis: time.Now().UnixMilli(),
			}, nil
		},
		Probe: func(_ context.Context, addrs []string) error {
			if len(addrs) == 0 {
				t.Error("acceptor probe got no candidates — its NAT mapping will not open")
			}
			probed.Store(true)
			return nil
		},
		Dial: func(context.Context, string, string) (netio.Conn, error) {
			t.Fatal("acceptor must not dial")
			return nil, nil
		},
	})
	_, err := l.Connect(context.Background(), lo)
	if !errors.Is(err, errAwaitInbound) {
		t.Fatalf("acceptor Connect err = %v, want errAwaitInbound", err)
	}
	if !probed.Load() {
		t.Fatal("acceptor did not probe to open its NAT mapping")
	}
}
