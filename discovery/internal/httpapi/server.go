// Package httpapi wires the REST and WebSocket surface: routing, middleware,
// mTLS-derived identity, and the abuse-handling extension points. It translates
// between the wire types in pkg/discovery and the internal services.
package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/coder/websocket"

	"github.com/GhentiLabs/Trove/discovery/internal/analytics"
	"github.com/GhentiLabs/Trove/discovery/internal/config"
	"github.com/GhentiLabs/Trove/discovery/internal/registry"
	"github.com/GhentiLabs/Trove/discovery/internal/signaling"
)

// Deps bundles the collaborators a Server needs. Filter, Denylist and Clock are
// optional and default to safe no-op / wall-clock implementations.
type Deps struct {
	Config    *config.Config
	Logger    *slog.Logger
	Registry  *registry.Registry
	Analytics *analytics.Store
	Broker    *signaling.Broker
	Metrics   *Metrics
	Filter    Filter
	Denylist  Denylist
	Clock     func() time.Time
}

// Server holds the resolved dependencies and request-scoped state.
type Server struct {
	cfg       *config.Config
	log       *slog.Logger
	reg       *registry.Registry
	analytics *analytics.Store
	broker    *signaling.Broker
	metrics   *Metrics
	filter    Filter
	denylist  Denylist
	clock     func() time.Time
	start     time.Time

	announceLim  *limiterStore
	lookupLim    *limiterStore
	analyticsLim *limiterStore
	signalLim    *limiterStore

	wsAccept *websocket.AcceptOptions
}

// New constructs a Server from its dependencies.
func New(d Deps) *Server {
	clock := d.Clock
	if clock == nil {
		clock = time.Now
	}
	filter := d.Filter
	if filter == nil {
		filter = AllowAllFilter{}
	}
	denylist := d.Denylist
	if denylist == nil {
		denylist = EmptyDenylist{}
	}
	return &Server{
		cfg:          d.Config,
		log:          d.Logger,
		reg:          d.Registry,
		analytics:    d.Analytics,
		broker:       d.Broker,
		metrics:      d.Metrics,
		filter:       filter,
		denylist:     denylist,
		clock:        clock,
		start:        clock(),
		announceLim:  newLimiterStore(d.Config.AnnounceRate, clock),
		lookupLim:    newLimiterStore(d.Config.LookupRate, clock),
		analyticsLim: newLimiterStore(d.Config.AnalyticsRate, clock),
		signalLim:    newLimiterStore(d.Config.SignalRate, clock),
		wsAccept:     &websocket.AcceptOptions{OriginPatterns: d.Config.AllowedWSOrigins},
	}
}

// Handler returns the fully wired public HTTP handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("POST /v1/announce",
		chain(http.HandlerFunc(s.handleAnnounce), s.bodyLimit(s.cfg.MaxRequestBodyBytes), s.gate("announce", s.announceLim)))
	mux.Handle("POST /v1/lookup",
		chain(http.HandlerFunc(s.handleLookup), s.bodyLimit(s.cfg.MaxRequestBodyBytes), s.gate("lookup", s.lookupLim)))
	mux.Handle("POST /v1/analytics",
		chain(http.HandlerFunc(s.handleAnalytics), s.bodyLimit(s.cfg.AnalyticsMaxBodyBytes), s.gate("analytics", s.analyticsLim)))
	mux.Handle("GET /v1/signal",
		chain(http.HandlerFunc(s.handleSignal), s.gate("signal", s.signalLim)))

	return chain(mux, withRequestID, logRequests(s.log), recoverPanic(s.log))
}

// HealthHandler serves the public liveness payload. It is registered on the
// localhost-only metrics listener, not the mTLS public listener.
func (s *Server) HealthHandler() http.HandlerFunc { return s.handleHealth }

// StartMaintenance periodically reaps idle rate limiters until ctx is done.
func (s *Server) StartMaintenance(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			const maxIdle = 10 * time.Minute
			for _, l := range []*limiterStore{s.announceLim, s.lookupLim, s.analyticsLim, s.signalLim} {
				l.sweep(maxIdle)
			}
		}
	}
}

// bodyLimit caps the request body so slow or oversized uploads cannot exhaust
// memory. Exceeding it surfaces as a decode error mapped to 413.
func (s *Server) bodyLimit(maxBytes int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
			next.ServeHTTP(w, r)
		})
	}
}

// gate is the single place IP-level denylist, the abuse filter, and per-IP rate
// limiting are enforced, before any handler body runs.
func (s *Server) gate(endpoint string, lim *limiterStore) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ip := clientIP(r)
			if s.denylist.BlockedIP(ip) {
				s.metrics.request(endpoint, "denied")
				writeError(w, s.log, http.StatusForbidden, codeForbidden, "forbidden", nil)
				return
			}
			if allowed, reason := s.filter.Allow(r.Context(), RequestInfo{SourceIP: ip, Endpoint: endpoint}); !allowed {
				s.metrics.request(endpoint, "filtered")
				s.log.Debug("request filtered", "endpoint", endpoint, "source_ip", ip, "reason", reason)
				writeError(w, s.log, http.StatusForbidden, codeForbidden, "forbidden", nil)
				return
			}
			if !lim.allow("ip:" + ip) {
				s.metrics.request(endpoint, "rate_limited")
				writeError(w, s.log, http.StatusTooManyRequests, codeRateLimited, "rate limit exceeded", nil)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
