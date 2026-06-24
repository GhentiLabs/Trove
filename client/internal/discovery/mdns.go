package discovery

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/GhentiLabs/Trove/pkg/identity"
	"github.com/libp2p/zeroconf/v2"
)

const (
	mdnsService = "_trove._udp"
	mdnsDomain  = "local."
	// mdnsShutdownTimeout bounds Close: zeroconf's Shutdown sends a goodbye over an
	// undeadlined multicast write that can hang on a constrained interface, so we
	// never let it stall daemon teardown. Peers expire the stale entry via its TTL.
	mdnsShutdownTimeout = 2 * time.Second
)

// LANPeer is a Trove peer discovered on the local network via mDNS.
type LANPeer struct {
	NodeID string
	Addr   string // ip:port
}

// MDNS advertises this node as a Trove service instance on the LAN so peers can
// find it without Trove. It is the cheapest reachability tier.
type MDNS struct {
	server *zeroconf.Server
}

// Advertise registers this node (instance = nodeID) as a Trove service at port.
func Advertise(nodeID string, port int) (*MDNS, error) {
	srv, err := zeroconf.Register(nodeID, mdnsService, mdnsDomain, port, []string{"id=" + nodeID}, nil)
	if err != nil {
		return nil, fmt.Errorf("discovery: mdns register: %w", err)
	}
	return &MDNS{server: srv}, nil
}

// Close withdraws the advertisement, bounded so a hung multicast write cannot stall
// daemon teardown.
func (m *MDNS) Close() {
	if m.server == nil {
		return
	}
	done := make(chan struct{})
	go func() {
		m.server.Shutdown()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(mdnsShutdownTimeout):
	}
}

// BrowseLAN delivers Trove peers seen on the local network until ctx is cancelled,
// excluding self. The returned channel closes when browsing stops. mDNS is
// best-effort: a multicast setup failure simply yields no peers.
func BrowseLAN(ctx context.Context, self string) <-chan LANPeer {
	entries := make(chan *zeroconf.ServiceEntry, incomingBuffer)
	out := make(chan LANPeer, incomingBuffer)
	go func() { _ = zeroconf.Browse(ctx, mdnsService, mdnsDomain, entries) }()
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case e, ok := <-entries:
				if !ok {
					return
				}
				if e.Instance == self || !identity.ValidNodeID(e.Instance) || len(e.AddrIPv4) == 0 {
					continue
				}
				peer := LANPeer{NodeID: e.Instance, Addr: net.JoinHostPort(e.AddrIPv4[0].String(), strconv.Itoa(e.Port))}
				select {
				case out <- peer:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}
