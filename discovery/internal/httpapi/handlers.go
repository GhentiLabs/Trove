package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/GhentiLabs/Trove/discovery/internal/analytics"
	"github.com/GhentiLabs/Trove/discovery/internal/registry"
	"github.com/GhentiLabs/Trove/pkg/discovery"
	"github.com/GhentiLabs/Trove/pkg/identity"
)

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, discovery.HealthResponse{
		Status:        "ok",
		UptimeSeconds: int64(s.clock().Sub(s.start).Seconds()),
		RegistrySize:  s.reg.Size(),
	})
}

func (s *Server) handleAnnounce(w http.ResponseWriter, r *http.Request) {
	nodeID, ok := s.authenticate(w, r, "announce", s.announceLim)
	if !ok {
		return
	}
	var req discovery.AnnounceRequest
	if !s.decode(w, "announce", r, &req) {
		return
	}
	if len(req.Addresses) == 0 {
		s.reject(w, "announce", http.StatusBadRequest, codeBadRequest, "at least one address required", nil)
		return
	}
	for _, a := range req.Addresses {
		if err := a.Validate(); err != nil {
			s.reject(w, "announce", http.StatusBadRequest, codeBadRequest, "invalid address", err)
			return
		}
	}

	ttl := s.clampTTL(req.RequestedTTLSecs)
	entry, err := s.reg.Announce(nodeID, req.Addresses, ttl)
	if err != nil {
		switch {
		case errors.Is(err, registry.ErrRegistryFull):
			s.reject(w, "announce", http.StatusServiceUnavailable, codeUnavailable, "registry full", err)
		case errors.Is(err, registry.ErrTooManyAddresses):
			s.reject(w, "announce", http.StatusBadRequest, codeBadRequest, "too many addresses", err)
		default:
			s.reject(w, "announce", http.StatusInternalServerError, codeInternal, "internal error", err)
		}
		return
	}

	s.metrics.request("announce", "ok")
	writeJSON(w, http.StatusOK, discovery.AnnounceResponse{
		NodeID: nodeID,
		// RemoteAddr is the real client only because TLS is terminated here; a
		// future proxy/LB in front would make this reflect the proxy instead.
		ObservedAddr:    r.RemoteAddr,
		GrantedTTLSecs:  int(ttl.Seconds()),
		ExpiresAtMillis: entry.ExpiresAt.UnixMilli(),
	})
}

func (s *Server) handleLookup(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.authenticate(w, r, "lookup", s.lookupLim); !ok {
		return
	}
	var req discovery.LookupRequest
	if !s.decode(w, "lookup", r, &req) {
		return
	}
	if !identity.ValidNodeID(req.TargetNodeID) {
		s.reject(w, "lookup", http.StatusBadRequest, codeBadRequest, "invalid target node id", nil)
		return
	}

	entry, found := s.reg.Lookup(req.TargetNodeID)
	if !found {
		s.metrics.request("lookup", "not_found")
		writeError(w, nil, http.StatusNotFound, codeNotFound, "node not found", nil)
		return
	}

	s.metrics.request("lookup", "found")
	writeJSON(w, http.StatusOK, discovery.LookupResponse{
		NodeID:         entry.NodeID,
		Addresses:      entry.Addresses,
		LastSeenMillis: entry.LastSeen.UnixMilli(),
	})
}

func (s *Server) handleAnalytics(w http.ResponseWriter, r *http.Request) {
	nodeID, ok := s.authenticate(w, r, "analytics", s.analyticsLim)
	if !ok {
		return
	}
	var req discovery.AnalyticsRequest
	if !s.decode(w, "analytics", r, &req) {
		return
	}

	err := s.analytics.Insert(r.Context(), analytics.Record{
		NodeID:        nodeID,
		InstallID:     req.InstallID,
		SchemaVersion: req.SchemaVersion,
		EventMillis:   req.EventMillis,
		SourceIP:      clientIP(r),
		Fields:        req.Fields,
	})
	switch {
	case errors.Is(err, analytics.ErrDiskFull):
		s.reject(w, "analytics", http.StatusInsufficientStorage, codeUnavailable, "analytics temporarily unavailable", err)
		return
	case err != nil:
		s.reject(w, "analytics", http.StatusInternalServerError, codeInternal, "internal error", err)
		return
	}

	s.metrics.request("analytics", "ok")
	writeJSON(w, http.StatusOK, discovery.AnalyticsResponse{Stored: true})
}

// decode reads and JSON-decodes the request body. It maps an oversized body to
// 413 and malformed JSON to 400, recording the rejection metric.
func (s *Server) decode(w http.ResponseWriter, endpoint string, r *http.Request, dst any) bool {
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			s.reject(w, endpoint, http.StatusRequestEntityTooLarge, codePayloadLarge, "request body too large", err)
		} else {
			s.reject(w, endpoint, http.StatusBadRequest, codeBadRequest, "malformed request", err)
		}
		return false
	}
	return true
}

// authenticate derives the caller's node ID from its mTLS client certificate,
// then enforces the node denylist and per-node rate limit. On failure it has
// already written the response and recorded the metric.
func (s *Server) authenticate(w http.ResponseWriter, r *http.Request, endpoint string, lim *limiterStore) (string, bool) {
	nodeID, err := identity.PeerFingerprint(r.TLS)
	if err != nil {
		s.reject(w, endpoint, http.StatusUnauthorized, codeUnauthorized, "unauthorized", err)
		return "", false
	}
	if s.denylist.BlockedNode(nodeID) {
		s.reject(w, endpoint, http.StatusForbidden, codeForbidden, "forbidden", nil)
		return "", false
	}
	if !lim.allow("node:" + nodeID) {
		s.reject(w, endpoint, http.StatusTooManyRequests, codeRateLimited, "rate limit exceeded", nil)
		return "", false
	}
	return nodeID, true
}

func (s *Server) reject(w http.ResponseWriter, endpoint string, status int, code, msg string, detail error) {
	s.metrics.request(endpoint, "rejected")
	writeError(w, s.log, status, code, msg, detail)
}

func (s *Server) clampTTL(requestedSecs int) time.Duration {
	if requestedSecs <= 0 {
		return s.cfg.TTLDefault
	}
	ttl := time.Duration(requestedSecs) * time.Second
	switch {
	case ttl < s.cfg.TTLMin:
		return s.cfg.TTLMin
	case ttl > s.cfg.TTLMax:
		return s.cfg.TTLMax
	default:
		return ttl
	}
}
