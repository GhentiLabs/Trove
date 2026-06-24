package netio

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
)

// streamBufferSize bounds how many Conns may await Accept, and streams await
// AcceptStream, before the producer blocks. A test that opens more than this many
// streams (or dials this many conns) before the accept loop runs will block.
const streamBufferSize = 16

var (
	errConnClosed      = errors.New("netio: connection closed")
	errTransportClosed = errors.New("netio: transport closed")
)

// MemNet is an in-memory Transport registry for deterministic tests. Transports
// register under an address and dial each other by that address.
//
// Each stream is backed by net.Pipe, which is synchronous and unbuffered: a Write
// blocks until the peer Reads. This models backpressure but differs from QUIC,
// which buffers writes. A test that writes on both ends of a stream before either
// reads will therefore deadlock under MemNet though it would succeed over QUIC;
// such tests must run the read side concurrently.
type MemNet struct {
	mu         sync.Mutex
	transports map[string]*memTransport
}

// NewMemNet returns an empty registry.
func NewMemNet() *MemNet {
	return &MemNet{transports: make(map[string]*memTransport)}
}

// Transport registers and returns a Transport listening at addr and presenting
// nodeID to peers it dials and accepts.
func (m *MemNet) Transport(addr, nodeID string) Transport {
	t := &memTransport{
		net:      m,
		addr:     addr,
		nodeID:   nodeID,
		incoming: make(chan *memConn, streamBufferSize),
		closed:   make(chan struct{}),
	}
	m.mu.Lock()
	m.transports[addr] = t
	m.mu.Unlock()
	return t
}

func (m *MemNet) lookup(addr string) (*memTransport, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.transports[addr]
	return t, ok
}

type memAddr string

func (a memAddr) Network() string { return "mem" }
func (a memAddr) String() string  { return string(a) }

type memTransport struct {
	net       *MemNet
	addr      string
	nodeID    string
	incoming  chan *memConn
	closeOnce sync.Once
	closed    chan struct{}
}

func (t *memTransport) LocalAddr() net.Addr { return memAddr(t.addr) }

func (t *memTransport) Probe(context.Context, []string) error { return nil }

func (t *memTransport) Dial(ctx context.Context, addr, nodeID string) (Conn, error) {
	peer, ok := t.net.lookup(addr)
	if !ok {
		return nil, fmt.Errorf("netio: no transport at %q", addr)
	}
	if peer.nodeID != nodeID {
		return nil, fmt.Errorf("netio: pin mismatch at %q: have %q, want %q", addr, peer.nodeID, nodeID)
	}
	dialer := newMemConn(peer.nodeID)
	acceptor := newMemConn(t.nodeID)
	dialer.peer = acceptor
	acceptor.peer = dialer
	select {
	case peer.incoming <- acceptor:
		return dialer, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (t *memTransport) Accept(ctx context.Context) (Conn, error) {
	select {
	case c := <-t.incoming:
		return c, nil
	case <-t.closed:
		return nil, errTransportClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (t *memTransport) Close() error {
	t.net.mu.Lock()
	delete(t.net.transports, t.addr)
	t.net.mu.Unlock()
	t.closeOnce.Do(func() { close(t.closed) })
	return nil
}

type memConn struct {
	peerNodeID string
	peer       *memConn
	incoming   chan net.Conn

	mu        sync.Mutex
	streams   []io.Closer
	closeOnce sync.Once
	closed    chan struct{}
}

func newMemConn(peerNodeID string) *memConn {
	return &memConn{
		peerNodeID: peerNodeID,
		incoming:   make(chan net.Conn, streamBufferSize),
		closed:     make(chan struct{}),
	}
}

func (c *memConn) PeerNodeID() string { return c.peerNodeID }

func (c *memConn) OpenStream(ctx context.Context) (Stream, error) {
	// Reject deterministically when already closed; otherwise the buffered send
	// below races the closed case in the select and may spuriously succeed.
	select {
	case <-c.closed:
		return nil, errConnClosed
	default:
	}
	local, remote := net.Pipe()
	select {
	case c.peer.incoming <- remote:
		c.track(local)
		return local, nil
	case <-c.closed:
		_ = local.Close()
		_ = remote.Close()
		return nil, errConnClosed
	case <-ctx.Done():
		_ = local.Close()
		_ = remote.Close()
		return nil, ctx.Err()
	}
}

func (c *memConn) AcceptStream(ctx context.Context) (Stream, error) {
	select {
	case s := <-c.incoming:
		c.track(s)
		return s, nil
	case <-c.closed:
		return nil, errConnClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *memConn) track(s io.Closer) {
	c.mu.Lock()
	select {
	case <-c.closed:
		// A concurrent shutdown already drained streams; close this late one here so
		// it is not leaked open.
		c.mu.Unlock()
		_ = s.Close()
		return
	default:
	}
	c.streams = append(c.streams, s)
	c.mu.Unlock()
}

// Close models QUIC connection teardown: it closes every stream this side handed
// out (which errors the peer's matching ends) and propagates to the peer so its
// blocked AcceptStream returns.
func (c *memConn) Close() error {
	c.shutdown()
	if c.peer != nil {
		c.peer.shutdown()
	}
	return nil
}

func (c *memConn) shutdown() {
	c.closeOnce.Do(func() {
		close(c.closed)
		c.mu.Lock()
		for _, s := range c.streams {
			_ = s.Close()
		}
		c.streams = nil
		c.mu.Unlock()
	})
}
