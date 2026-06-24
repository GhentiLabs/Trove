// Package transport implements the netio seam over QUIC. A single quic.Transport
// bound to one UDP socket both dials and accepts (demultiplexed by connection ID),
// which is what the holepunch path requires: the punched NAT mapping and the QUIC
// listener share the same local port. Peer authentication is mTLS with fingerprint
// pinning via pkg/identity; there is no certificate authority.
package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/netio"
	"github.com/GhentiLabs/Trove/pkg/identity"
	"github.com/quic-go/quic-go"
)

// alpn is the Trove ALPN token negotiated on every session; quic-go requires a
// non-empty NextProtos. It is frozen cross-node contract.
const alpn = "trove/1"

const (
	keepAlivePeriod      = 15 * time.Second
	maxIdleTimeout       = 30 * time.Second
	handshakeIdleTimeout = 10 * time.Second
)

// maxProbeTargets caps how many candidates a single Probe fires at, bounding abuse
// when the candidate list comes from an untrusted discovery source.
const maxProbeTargets = 16

// probePayload is a single non-QUIC byte sent to open a NAT mapping before a
// holepunch. The peer's transport ignores it; only the outbound mapping matters.
// Never mutated.
var probePayload = []byte{0}

// Options configures New.
type Options struct {
	// Cert is this node's identity certificate (from pkg/identity).
	Cert tls.Certificate
	// UDPAddr is the local address to bind, e.g. "0.0.0.0:0" for an ephemeral port.
	UDPAddr string
}

// Transport is a QUIC endpoint that both dials and accepts on one UDP socket.
type Transport struct {
	conn      *net.UDPConn
	tr        *quic.Transport
	ln        *quic.Listener
	cert      tls.Certificate
	quicConf  *quic.Config
	closeOnce sync.Once
}

// New binds a UDP socket and starts accepting QUIC connections on it.
func New(opts Options) (*Transport, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", opts.UDPAddr)
	if err != nil {
		return nil, fmt.Errorf("transport: resolve %q: %w", opts.UDPAddr, err)
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, fmt.Errorf("transport: listen udp: %w", err)
	}
	quicConf := &quic.Config{
		KeepAlivePeriod:      keepAlivePeriod,
		MaxIdleTimeout:       maxIdleTimeout,
		HandshakeIdleTimeout: handshakeIdleTimeout,
	}
	tr := &quic.Transport{Conn: conn}
	ln, err := tr.Listen(serverTLS(opts.Cert), quicConf)
	if err != nil {
		// tr.Close does not close an externally-supplied conn (createdConn=false),
		// so close the socket explicitly.
		_ = tr.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("transport: listen quic: %w", err)
	}
	return &Transport{conn: conn, tr: tr, ln: ln, cert: opts.Cert, quicConf: quicConf}, nil
}

// Dial opens a Conn to addr, pinning the peer's certificate fingerprint to nodeID
// during the handshake.
func (t *Transport) Dial(ctx context.Context, addr, nodeID string) (netio.Conn, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("transport: resolve %q: %w", addr, err)
	}
	qc, err := t.tr.Dial(ctx, udpAddr, clientTLS(t.cert, nodeID), t.quicConf)
	if err != nil {
		return nil, fmt.Errorf("transport: dial %s: %w", addr, err)
	}
	return newConn(qc)
}

// Accept returns the next inbound Conn. Authorization of its PeerNodeID against the
// peer registry is the caller's responsibility.
func (t *Transport) Accept(ctx context.Context) (netio.Conn, error) {
	qc, err := t.ln.Accept(ctx)
	if err != nil {
		return nil, fmt.Errorf("transport: accept: %w", err)
	}
	return newConn(qc)
}

// LocalAddr is the bound UDP address.
func (t *Transport) LocalAddr() net.Addr { return t.conn.LocalAddr() }

// Probe sends a datagram to each addr on the shared socket to open this side's NAT
// mapping ahead of a simultaneous-open holepunch. It bounds the target count and
// skips any address that is not a public unicast endpoint (loopback, multicast,
// unspecified, link-local, or private) so an untrusted candidate list cannot aim
// the node's socket at hosts inside its own network. A failed send is skipped, not
// fatal: one bad candidate must not suppress the others.
func (t *Transport) Probe(ctx context.Context, addrs []string) error {
	if len(addrs) > maxProbeTargets {
		addrs = addrs[:maxProbeTargets]
	}
	for _, a := range addrs {
		ua, err := net.ResolveUDPAddr("udp", a)
		if err != nil || !routableUnicast(ua.IP) {
			continue
		}
		_, _ = t.tr.WriteTo(probePayload, ua)
	}
	return nil
}

func routableUnicast(ip net.IP) bool {
	return ip != nil && !ip.IsLoopback() && !ip.IsMulticast() && !ip.IsUnspecified() &&
		!ip.IsLinkLocalUnicast() && !ip.IsLinkLocalMulticast() && !ip.IsPrivate()
}

// Close tears down the listener, the QUIC transport, and the UDP socket. It is
// idempotent. quic-go does not close an externally-supplied conn, so the socket is
// closed explicitly; otherwise its file descriptor and port leak.
func (t *Transport) Close() error {
	var err error
	t.closeOnce.Do(func() {
		_ = t.ln.Close()
		err = t.tr.Close()
		_ = t.conn.Close()
	})
	return err
}

func clientTLS(cert tls.Certificate, pin string) *tls.Config {
	c := identity.PinnedClientConfig(cert, pin)
	c.NextProtos = []string{alpn}
	return c
}

func serverTLS(cert tls.Certificate) *tls.Config {
	c := identity.ServerTLSConfig(cert)
	c.NextProtos = []string{alpn}
	return c
}
