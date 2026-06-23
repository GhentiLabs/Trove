// Package netio is the transport seam: a multiplexed, authenticated byte-stream
// connection between two nodes, behind interfaces so a deterministic in-memory
// transport can be substituted in tests (synctest cannot fake real network I/O).
// The QUIC implementation lives in the transport package; the wire framing lives
// above this seam, so streams here are raw byte pipes.
package netio

import (
	"context"
	"io"
	"net"
)

// Stream is a bidirectional byte stream within a Conn (one QUIC stream).
type Stream interface {
	io.Reader
	io.Writer
	io.Closer
}

// Conn is a multiplexed, mutually-authenticated connection to one peer. Its
// PeerNodeID is the peer's 52-character base32 SPKI fingerprint, derived from its
// verified TLS certificate.
type Conn interface {
	OpenStream(ctx context.Context) (Stream, error)
	AcceptStream(ctx context.Context) (Stream, error)
	PeerNodeID() string
	Close() error
}

// Dialer opens a Conn to the peer reachable at a UDP address, pinning its identity
// to nodeID during the TLS handshake: a peer that presents a certificate with a
// different fingerprint is rejected before any application data. The caller
// resolves node_id→addr (via discovery) and passes both.
type Dialer interface {
	Dial(ctx context.Context, addr, nodeID string) (Conn, error)
}

// Listener accepts inbound Conns. Authorization (is this node_id in the registry)
// is the caller's responsibility, applied to each accepted Conn's PeerNodeID.
type Listener interface {
	Accept(ctx context.Context) (Conn, error)
	Close() error
}

// Transport both dials and accepts on one local endpoint (one UDP socket for QUIC,
// which the holepunch path requires).
type Transport interface {
	Dialer
	Listener
	// LocalAddr is the local UDP address the transport is bound to. Candidate
	// gathering advertises it, and it identifies the socket the server-observed
	// external address maps to.
	LocalAddr() net.Addr
	// Probe sends empty datagrams to addrs on the local socket to open this side's
	// NAT mapping before a simultaneous-open holepunch. A transport without a real
	// socket (the in-memory fake) treats it as a no-op.
	Probe(ctx context.Context, addrs []string) error
}
