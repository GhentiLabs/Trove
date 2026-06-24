package discovery

import (
	"fmt"
	"net"

	disco "github.com/GhentiLabs/Trove/pkg/discovery"
)

// LocalCandidates returns this node's routable interface addresses at port, tagged
// LAN for private (RFC 1918 / ULA) addresses and Public for globally routable ones.
// Loopback, link-local, multicast, and unspecified addresses are excluded — they
// are never valid peer candidates. These are the cheapest reachability tier and the
// base set extended by the server-observed address (announce) and UPnP.
func LocalCandidates(port int) ([]disco.Address, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, fmt.Errorf("discovery: list interfaces: %w", err)
	}
	var out []disco.Address
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			ipnet, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			ip := ipnet.IP
			if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast() || ip.IsUnspecified() {
				continue
			}
			typ := disco.AddressPublic
			if ip.IsPrivate() {
				typ = disco.AddressLAN
			}
			out = append(out, disco.Address{IP: ip.String(), Port: port, Type: typ})
		}
	}
	return out, nil
}
