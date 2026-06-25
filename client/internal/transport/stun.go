package transport

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/GhentiLabs/Trove/pkg/stun"
)

const (
	stunRTO    = 500 * time.Millisecond
	stunMaxRTO = 2 * time.Second
)

type stunConn struct {
	// Held as net.PacketConn, not *net.UDPConn, to stop quic-go's ReadMsgUDP fast path.
	net.PacketConn

	mu      sync.Mutex
	waiters map[stun.TxID]chan netip.AddrPort
}

func newSTUNConn(c net.PacketConn) *stunConn {
	return &stunConn{PacketConn: c, waiters: make(map[stun.TxID]chan netip.AddrPort)}
}

func (c *stunConn) ReadFrom(p []byte) (int, net.Addr, error) {
	for {
		n, addr, err := c.PacketConn.ReadFrom(p)
		if err != nil {
			return n, addr, err
		}
		if stun.Looks(p[:n]) {
			if id, mapped, ok := stun.ParseResponse(p[:n]); ok {
				c.deliver(id, mapped)
			}
			continue
		}
		return n, addr, err
	}
}

func (c *stunConn) SetReadBuffer(n int) error {
	if u, ok := c.PacketConn.(interface{ SetReadBuffer(int) error }); ok {
		return u.SetReadBuffer(n)
	}
	return nil
}

func (c *stunConn) SetWriteBuffer(n int) error {
	if u, ok := c.PacketConn.(interface{ SetWriteBuffer(int) error }); ok {
		return u.SetWriteBuffer(n)
	}
	return nil
}

func (c *stunConn) deliver(id stun.TxID, mapped netip.AddrPort) {
	c.mu.Lock()
	ch, ok := c.waiters[id]
	c.mu.Unlock()
	if ok {
		select {
		case ch <- mapped:
		default:
		}
	}
}

func (c *stunConn) register(id stun.TxID) chan netip.AddrPort {
	ch := make(chan netip.AddrPort, 1)
	c.mu.Lock()
	c.waiters[id] = ch
	c.mu.Unlock()
	return ch
}

func (c *stunConn) unregister(id stun.TxID) {
	c.mu.Lock()
	delete(c.waiters, id)
	c.mu.Unlock()
}

// Reflexive discovers the external ip:port this transport's UDP socket maps to.
func (t *Transport) Reflexive(ctx context.Context, server string) (netip.AddrPort, error) {
	ua, err := net.ResolveUDPAddr("udp", server)
	if err != nil {
		return netip.AddrPort{}, fmt.Errorf("transport: resolve stun %q: %w", server, err)
	}
	id, err := stun.NewTxID()
	if err != nil {
		return netip.AddrPort{}, err
	}
	ch := t.sc.register(id)
	defer t.sc.unregister(id)

	req := stun.AppendBindingRequest(nil, id)
	rto := stunRTO
	for {
		if _, err := t.tr.WriteTo(req, ua); err != nil {
			return netip.AddrPort{}, fmt.Errorf("transport: stun write: %w", err)
		}
		timer := time.NewTimer(rto)
		select {
		case mapped := <-ch:
			timer.Stop()
			return mapped, nil
		case <-timer.C:
			rto = min(rto*2, stunMaxRTO)
		case <-ctx.Done():
			timer.Stop()
			return netip.AddrPort{}, fmt.Errorf("transport: stun: %w", ctx.Err())
		}
	}
}
