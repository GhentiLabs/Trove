package signaling

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/GhentiLabs/Trove/pkg/discovery"
)

type fakeWS struct {
	in        chan []byte
	out       chan []byte
	closed    chan struct{}
	closeOnce sync.Once
}

func newFakeWS() *fakeWS {
	return &fakeWS{in: make(chan []byte, 8), out: make(chan []byte, 8), closed: make(chan struct{})}
}

func (f *fakeWS) Read(ctx context.Context) ([]byte, error) {
	select {
	case d := <-f.in:
		return d, nil
	case <-f.closed:
		return nil, io.EOF
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *fakeWS) Write(ctx context.Context, data []byte) error {
	cp := append([]byte(nil), data...)
	select {
	case f.out <- cp:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (f *fakeWS) Ping(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func (f *fakeWS) Close(string) error {
	f.closeOnce.Do(func() { close(f.closed) })
	return nil
}

func (f *fakeWS) push(t *testing.T, typ discovery.SignalType, payload any) {
	t.Helper()
	msg, err := discovery.NewSignalMessage(typ, payload)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	f.in <- data
}

func (f *fakeWS) expect(t *testing.T, want discovery.SignalType, into any) {
	t.Helper()
	select {
	case data := <-f.out:
		var msg discovery.SignalMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			t.Fatalf("decode framing: %v", err)
		}
		if msg.Type != want {
			t.Fatalf("got message type %q, want %q", msg.Type, want)
		}
		if into != nil {
			if err := msg.Decode(into); err != nil {
				t.Fatalf("decode payload: %v", err)
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %q", want)
	}
}

func testBroker(resolve func(string) ([]discovery.Address, bool)) *Broker {
	return New(Options{
		MaxConns:     10,
		SendBuffer:   8,
		PunchOffset:  500 * time.Millisecond,
		PingInterval: time.Hour,
		WriteTimeout: time.Second,
		RatePerSec:   1000,
		RateBurst:    1000,
		Clock:        func() time.Time { return time.Unix(1000, 0) },
		Resolve:      resolve,
	})
}

func waitActive(t *testing.T, b *Broker, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if b.Active() == n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("broker active = %d, want %d", b.Active(), n)
}

func TestRouteDeliversBothSides(t *testing.T) {
	bAddrs := []discovery.Address{{IP: "203.0.113.7", Port: 4000, Type: discovery.AddressPublic}}
	broker := testBroker(func(id string) ([]discovery.Address, bool) {
		if id == "node-b" {
			return bAddrs, true
		}
		return nil, false
	})

	wsA, wsB := newFakeWS(), newFakeWS()
	go broker.Serve(context.Background(), wsA, "node-a")
	go broker.Serve(context.Background(), wsB, "node-b")
	waitActive(t, broker, 2)

	aCandidates := []discovery.Address{{IP: "198.51.100.3", Port: 9000, Type: discovery.AddressPublic}}
	wsA.push(t, discovery.SignalConnectRequest, discovery.ConnectRequest{TargetNodeID: "node-b", MyCandidates: aCandidates})

	var incoming discovery.IncomingRequest
	wsB.expect(t, discovery.SignalIncomingRequest, &incoming)
	if incoming.FromNodeID != "node-a" || len(incoming.Candidates) != 1 || incoming.Candidates[0] != aCandidates[0] {
		t.Fatalf("target got wrong incoming_request: %+v", incoming)
	}

	var peer discovery.PeerCandidates
	wsA.expect(t, discovery.SignalPeerCandidates, &peer)
	if peer.FromNodeID != "node-b" || len(peer.Candidates) != 1 || peer.Candidates[0] != bAddrs[0] {
		t.Fatalf("requester got wrong peer_candidates: %+v", peer)
	}
	if incoming.PunchAtMillis != peer.PunchAtMillis || peer.PunchAtMillis == 0 {
		t.Fatalf("punch times not synchronized: %d vs %d", incoming.PunchAtMillis, peer.PunchAtMillis)
	}
}

func TestRouteTargetUnavailable(t *testing.T) {
	broker := testBroker(func(string) ([]discovery.Address, bool) { return nil, false })
	wsA := newFakeWS()
	go broker.Serve(context.Background(), wsA, "node-a")
	waitActive(t, broker, 1)

	wsA.push(t, discovery.SignalConnectRequest, discovery.ConnectRequest{TargetNodeID: "ghost"})
	var unavail discovery.TargetUnavailable
	wsA.expect(t, discovery.SignalTargetUnavailable, &unavail)
	if unavail.TargetNodeID != "ghost" {
		t.Fatalf("unexpected target: %q", unavail.TargetNodeID)
	}
}

func TestSelfConnectIsUnavailable(t *testing.T) {
	broker := testBroker(func(string) ([]discovery.Address, bool) { return nil, true })
	wsA := newFakeWS()
	go broker.Serve(context.Background(), wsA, "node-a")
	waitActive(t, broker, 1)

	wsA.push(t, discovery.SignalConnectRequest, discovery.ConnectRequest{TargetNodeID: "node-a"})
	wsA.expect(t, discovery.SignalTargetUnavailable, nil)
}

func TestCapacityRejected(t *testing.T) {
	broker := New(Options{
		MaxConns: 1, SendBuffer: 4, PingInterval: time.Hour, WriteTimeout: time.Second,
		RatePerSec: 100, RateBurst: 100, Resolve: func(string) ([]discovery.Address, bool) { return nil, false },
	})
	wsA := newFakeWS()
	go broker.Serve(context.Background(), wsA, "node-a")
	waitActive(t, broker, 1)

	if err := broker.Serve(context.Background(), newFakeWS(), "node-b"); !errors.Is(err, ErrAtCapacity) {
		t.Fatalf("err = %v, want ErrAtCapacity", err)
	}
}

func TestDisconnectUnregisters(t *testing.T) {
	broker := testBroker(func(string) ([]discovery.Address, bool) { return nil, false })
	wsA := newFakeWS()
	done := make(chan struct{})
	go func() { broker.Serve(context.Background(), wsA, "node-a"); close(done) }()
	waitActive(t, broker, 1)

	wsA.Close("")
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after disconnect")
	}
	waitActive(t, broker, 0)
}

func TestReplaceSameNode(t *testing.T) {
	broker := testBroker(func(string) ([]discovery.Address, bool) { return nil, false })
	wsA1 := newFakeWS()
	go broker.Serve(context.Background(), wsA1, "node-a")
	waitActive(t, broker, 1)

	wsA2 := newFakeWS()
	go broker.Serve(context.Background(), wsA2, "node-a")
	// The displaced connection must be closed; active count stays at 1.
	waitActive(t, broker, 1)
	select {
	case <-wsA1.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("old connection was not closed on replacement")
	}
}
