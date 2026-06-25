package discovery

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"time"

	disco "github.com/GhentiLabs/Trove/pkg/discovery"
	nat "github.com/fd/go-nat"
)

const mappingTTL = time.Hour

var cgnat = netip.MustParsePrefix("100.64.0.0/10")

// PortMap is an active UPnP/NAT-PMP external port mapping.
type PortMap struct {
	nat      nat.NAT
	internal int
	External disco.Address
}

// MapPort maps an external UDP port to internalPort via UPnP/NAT-PMP. Best-effort.
func MapPort(ctx context.Context, internalPort int) (*PortMap, error) {
	type result struct {
		pm  *PortMap
		err error
	}
	ch := make(chan result, 1)
	go func() {
		gw, err := nat.DiscoverGateway()
		if err != nil {
			ch <- result{nil, fmt.Errorf("discovery: no nat gateway: %w", err)}
			return
		}
		ext, err := gw.GetExternalAddress()
		if err != nil {
			ch <- result{nil, fmt.Errorf("discovery: external address: %w", err)}
			return
		}
		if !publiclyRoutable(ext) {
			ch <- result{nil, fmt.Errorf("discovery: external address %s is not publicly routable", ext)}
			return
		}
		extPort, err := gw.AddPortMapping("udp", internalPort, "trove", mappingTTL)
		if err != nil {
			ch <- result{nil, fmt.Errorf("discovery: add port mapping: %w", err)}
			return
		}
		ch <- result{&PortMap{
			nat:      gw,
			internal: internalPort,
			External: disco.Address{IP: ext.String(), Port: extPort, Type: disco.AddressPublic},
		}, nil}
	}()

	select {
	case r := <-ch:
		return r.pm, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Refresh renews the mapping's lease, bounded by ctx.
func (m *PortMap) Refresh(ctx context.Context) error {
	type result struct {
		port int
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		port, err := m.nat.AddPortMapping("udp", m.internal, "trove", mappingTTL)
		ch <- result{port, err}
	}()

	select {
	case r := <-ch:
		if r.err != nil {
			return fmt.Errorf("discovery: refresh port mapping: %w", r.err)
		}
		m.External.Port = r.port
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release removes the port mapping from the gateway.
func (m *PortMap) Release() error {
	if err := m.nat.DeletePortMapping("udp", m.internal); err != nil {
		return fmt.Errorf("discovery: delete port mapping: %w", err)
	}
	return nil
}

func publiclyRoutable(ip net.IP) bool {
	return !ip.IsPrivate() && !ip.IsLoopback() && !ip.IsUnspecified() && !isCGNAT(ip)
}

func isCGNAT(ip net.IP) bool {
	a, ok := netip.AddrFromSlice(ip)
	return ok && cgnat.Contains(a.Unmap())
}
