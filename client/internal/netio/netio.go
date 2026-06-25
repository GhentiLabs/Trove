// Package netio is the transport seam: a multiplexed, authenticated byte-stream
// connection behind interfaces so an in-memory transport can be substituted in
// tests. The QUIC implementation lives in the transport package; streams here are
// raw byte pipes.
package netio

import (
	"context"
	"errors"
	"io"
	"net"
	"time"
)

// ErrPeerClosed is returned by a Stream read when the peer closed the connection
// cleanly, as opposed to an abrupt drop, without depending on the concrete transport.
var ErrPeerClosed = errors.New("netio: peer closed the connection")

// Stream is a bidirectional byte stream within a Conn (one QUIC stream). Close
// half-closes the write side (sends FIN); the read side stays open until the peer
// closes its end. Tear the whole connection down via Conn.Close.
type Stream interface {
	io.Reader
	io.Writer
	io.Closer
	// SetReadDeadline bounds blocking reads so a stalled peer cannot hold a reader
	// past a per-attempt timeout. A zero time clears the deadline.
	SetReadDeadline(t time.Time) error
}

// Conn is a multiplexed, mutually-authenticated connection to one peer. Its
// PeerNodeID is the peer's SPKI fingerprint from its verified TLS certificate.
type Conn interface {
	OpenStream(ctx context.Context) (Stream, error)
	AcceptStream(ctx context.Context) (Stream, error)
	PeerNodeID() string
	Close() error
}

// Dialer opens a Conn to addr, pinning the peer's identity to nodeID during the TLS
// handshake. The caller resolves node_id->addr via discovery.
type Dialer interface {
	Dial(ctx context.Context, addr, nodeID string) (Conn, error)
}

// Listener accepts inbound Conns; the caller authorizes each PeerNodeID.
type Listener interface {
	Accept(ctx context.Context) (Conn, error)
	Close() error
}

// Transport both dials and accepts on one local endpoint (one UDP socket for QUIC,
// which the holepunch path requires).
type Transport interface {
	Dialer
	Listener
	LocalAddr() net.Addr
	// Probe sends empty datagrams to open this side's NAT mapping before a holepunch.
	Probe(ctx context.Context, addrs []string) error
}
