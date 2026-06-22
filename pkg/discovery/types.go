// Package discovery defines the JSON wire types for the Trove discovery
// protocol, shared between the discovery server and its clients. Requests carry
// no authentication fields: the caller's identity comes from the mutual-TLS
// client certificate, and the node ID is its SPKI fingerprint (see pkg/identity).
package discovery

import (
	"fmt"
	"net"
	"net/netip"
	"strconv"
)

// AddressType tags how a candidate address was discovered.
type AddressType string

const (
	AddressLAN    AddressType = "lan"
	AddressPublic AddressType = "public"
	AddressSTUN   AddressType = "stun"
)

func (t AddressType) valid() bool {
	switch t {
	case AddressLAN, AddressPublic, AddressSTUN:
		return true
	default:
		return false
	}
}

// Address is a single candidate transport endpoint a peer may be reachable at.
type Address struct {
	IP   string      `json:"ip"`
	Port int         `json:"port"`
	Type AddressType `json:"type"`
}

// Validate checks that the address is well formed. It is intentionally strict:
// malformed candidates are rejected at ingestion rather than stored.
func (a Address) Validate() error {
	ip, err := netip.ParseAddr(a.IP)
	if err != nil {
		return fmt.Errorf("invalid ip %q", a.IP)
	}
	if ip.IsLoopback() || ip.IsUnspecified() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return fmt.Errorf("non-routable ip %q", a.IP)
	}
	if a.Port < 1 || a.Port > 65535 {
		return fmt.Errorf("invalid port %d", a.Port)
	}
	if !a.Type.valid() {
		return fmt.Errorf("invalid address type %q", a.Type)
	}
	return nil
}

// String renders the address as host:port.
func (a Address) String() string {
	return net.JoinHostPort(a.IP, strconv.Itoa(a.Port))
}

// AnnounceRequest publishes a node's current candidate addresses.
type AnnounceRequest struct {
	Addresses        []Address `json:"addresses"`
	RequestedTTLSecs int       `json:"requested_ttl_secs"`
}

// AnnounceResponse confirms registration and reflects the observed source
// address as a convenience for NAT discovery.
type AnnounceResponse struct {
	NodeID          string `json:"node_id"`
	ObservedAddr    string `json:"observed_addr"`
	GrantedTTLSecs  int    `json:"granted_ttl_secs"`
	ExpiresAtMillis int64  `json:"expires_at_millis"`
}

// LookupRequest resolves a target node's current addresses.
type LookupRequest struct {
	TargetNodeID string `json:"target_node_id"`
}

// LookupResponse is the resolved registry entry for a node.
type LookupResponse struct {
	NodeID         string    `json:"node_id"`
	Addresses      []Address `json:"addresses"`
	LastSeenMillis int64     `json:"last_seen_millis"`
}

// AnalyticsRequest reports an open-ended set of telemetry fields. The reporting
// node is identified by its mTLS client certificate, not by the body.
type AnalyticsRequest struct {
	InstallID     string         `json:"install_id"`
	SchemaVersion int            `json:"schema_version"`
	EventMillis   int64          `json:"event_millis"`
	Fields        map[string]any `json:"fields"`
}

// AnalyticsResponse acknowledges storage.
type AnalyticsResponse struct {
	Stored bool `json:"stored"`
}

// HealthResponse is the public liveness payload. It stays minimal: nothing here
// should leak operational detail.
type HealthResponse struct {
	Status        string `json:"status"`
	UptimeSeconds int64  `json:"uptime_seconds"`
	RegistrySize  int    `json:"registry_size"`
}

// Error is the single error envelope returned by every endpoint. Messages are
// generic; detail is logged server-side only.
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e Error) Error() string { return e.Code + ": " + e.Message }
