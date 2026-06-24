package discovery

import (
	"context"
	"fmt"
	"time"

	disco "github.com/GhentiLabs/Trove/pkg/discovery"
	nat "github.com/fd/go-nat"
)

// mappingTTL is how long a requested external port mapping should live before the
// gateway may reclaim it; the daemon refreshes well before this.
const mappingTTL = time.Hour

// PortMap is an active UPnP/NAT-PMP external port mapping. Release removes it.
type PortMap struct {
	nat      nat.NAT
	internal int
	// External is the mapped external candidate to advertise.
	External disco.Address
}

// MapPort discovers a UPnP/NAT-PMP gateway and maps an external UDP port to
// internalPort, returning an external candidate. It is best-effort: networks
// without a supporting gateway (or behind CGNAT) return an error the caller is
// expected to ignore and fall back to holepunching. Gateway discovery is bounded
// by ctx.
func MapPort(ctx context.Context, internalPort int) (*PortMap, error) {
	type result struct {
		gw  nat.NAT
		err error
	}
	ch := make(chan result, 1)
	go func() {
		gw, err := nat.DiscoverGateway()
		ch <- result{gw, err}
	}()

	var gw nat.NAT
	select {
	case r := <-ch:
		if r.err != nil {
			return nil, fmt.Errorf("discovery: no nat gateway: %w", r.err)
		}
		gw = r.gw
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	ext, err := gw.GetExternalAddress()
	if err != nil {
		return nil, fmt.Errorf("discovery: external address: %w", err)
	}
	extPort, err := gw.AddPortMapping("udp", internalPort, "trove", mappingTTL)
	if err != nil {
		return nil, fmt.Errorf("discovery: add port mapping: %w", err)
	}
	return &PortMap{
		nat:      gw,
		internal: internalPort,
		External: disco.Address{IP: ext.String(), Port: extPort, Type: disco.AddressPublic},
	}, nil
}

// Release removes the port mapping from the gateway.
func (m *PortMap) Release() error {
	if err := m.nat.DeletePortMapping("udp", m.internal); err != nil {
		return fmt.Errorf("discovery: delete port mapping: %w", err)
	}
	return nil
}
