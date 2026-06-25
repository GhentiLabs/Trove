// Package transport implements the netio seam over QUIC on a single UDP socket.
package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/rand/v2"
	"net"
	"sync"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/netio"
	disco "github.com/GhentiLabs/Trove/pkg/discovery"
	"github.com/GhentiLabs/Trove/pkg/identity"
	"github.com/quic-go/quic-go"
)

const alpn = "trove/1"

const (
	keepAlivePeriod      = 15 * time.Second
	maxIdleTimeout       = 30 * time.Second
	handshakeIdleTimeout = 10 * time.Second
)

const (
	maxProbeTargets = 16
	probeRounds     = 12
	probePayload    = "\x00"
)

// Options configures New.
type Options struct {
	Cert    tls.Certificate
	UDPAddr string
}

// Transport is a QUIC endpoint that both dials and accepts on one UDP socket.
type Transport struct {
	conn      *net.UDPConn
	sc        *stunConn
	tr        *quic.Transport
	ln        *quic.Listener
	cert      tls.Certificate
	quicConf  *quic.Config
	closeOnce sync.Once
}

// New binds a UDP socket and starts accepting QUIC connections.
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
	sc := newSTUNConn(conn)
	tr := &quic.Transport{Conn: sc}
	ln, err := tr.Listen(serverTLS(opts.Cert), quicConf)
	if err != nil {
		_ = tr.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("transport: listen quic: %w", err)
	}
	return &Transport{conn: conn, sc: sc, tr: tr, ln: ln, cert: opts.Cert, quicConf: quicConf}, nil
}

// Dial opens a Conn to addr, pinning the peer's fingerprint to nodeID.
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

// Accept returns the next inbound Conn; the caller authorizes its PeerNodeID.
func (t *Transport) Accept(ctx context.Context) (netio.Conn, error) {
	qc, err := t.ln.Accept(ctx)
	if err != nil {
		return nil, fmt.Errorf("transport: accept: %w", err)
	}
	return newConn(qc)
}

// LocalAddr is the bound UDP address.
func (t *Transport) LocalAddr() net.Addr { return t.conn.LocalAddr() }

// Probe sprays datagrams at addrs to open this side's NAT mapping before a holepunch.
func (t *Transport) Probe(ctx context.Context, addrs []string) error {
	if len(addrs) > maxProbeTargets {
		addrs = addrs[:maxProbeTargets]
	}
	targets := make([]*net.UDPAddr, 0, len(addrs))
	for _, a := range addrs {
		ua, err := net.ResolveUDPAddr("udp", a)
		if err != nil || !disco.RoutableIP(ua.AddrPort().Addr().Unmap()) {
			continue
		}
		targets = append(targets, ua)
	}
	for i := range probeRounds {
		for _, ua := range targets {
			_, _ = t.tr.WriteTo([]byte(probePayload), ua)
		}
		if i == probeRounds-1 {
			break
		}
		jitter := min(10*(i+1)*(i+1), 200)
		select {
		case <-time.After(10*time.Millisecond + time.Duration(rand.IntN(jitter))*time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// Close tears down the listener, transport, and socket. It is idempotent.
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
