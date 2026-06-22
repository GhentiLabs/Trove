package httpapi

import "context"

// RequestInfo is the per-request signal passed to the filter hook.
type RequestInfo struct {
	SourceIP string
	Endpoint string
}

// Filter is the extension point for spam/abuse detection, consulted on every
// request. The default implementation passes everything.
type Filter interface {
	// Allow reports whether the request may proceed and, when denied, a short
	// machine-readable reason for logging.
	Allow(ctx context.Context, info RequestInfo) (allowed bool, reason string)
}

// Denylist is the extension point for static blocking by source IP or node ID,
// consulted on every request.
type Denylist interface {
	BlockedIP(ip string) bool
	BlockedNode(nodeID string) bool
}

// AllowAllFilter is the default no-op Filter.
type AllowAllFilter struct{}

func (AllowAllFilter) Allow(context.Context, RequestInfo) (bool, string) { return true, "" }

// EmptyDenylist is the default Denylist that blocks nothing.
type EmptyDenylist struct{}

func (EmptyDenylist) BlockedIP(string) bool   { return false }
func (EmptyDenylist) BlockedNode(string) bool { return false }
