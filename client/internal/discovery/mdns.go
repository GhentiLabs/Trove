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
	mdnsService         = "_trove._udp"
	mdnsDomain          = "local."
	mdnsShutdownTimeout = 2 * time.Second
)

// LANPeer is a Trove peer discovered on the local network via mDNS.
type LANPeer struct {
	NodeID string
	Addr   string
}

// MDNS advertises this node as a Trove service instance on the LAN.
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

// Close withdraws the advertisement, bounded by mdnsShutdownTimeout.
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

// BrowseLAN delivers Trove peers seen on the local network until ctx is cancelled.
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
				if e.Instance == self || !identity.ValidNodeID(e.Instance) {
					continue
				}
				ip := mdnsHost(e)
				if ip == "" {
					continue
				}
				peer := LANPeer{NodeID: e.Instance, Addr: net.JoinHostPort(ip, strconv.Itoa(e.Port))}
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

func mdnsHost(e *zeroconf.ServiceEntry) string {
	if len(e.AddrIPv4) > 0 {
		return e.AddrIPv4[0].String()
	}
	if len(e.AddrIPv6) > 0 {
		return e.AddrIPv6[0].String()
	}
	return ""
}
