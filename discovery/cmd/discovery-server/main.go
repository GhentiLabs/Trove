// Command server runs the Trove discovery and signaling service.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/GhentiLabs/Trove/discovery/internal/analytics"
	"github.com/GhentiLabs/Trove/discovery/internal/config"
	"github.com/GhentiLabs/Trove/discovery/internal/httpapi"
	"github.com/GhentiLabs/Trove/discovery/internal/registry"
	"github.com/GhentiLabs/Trove/discovery/internal/signaling"
	"github.com/GhentiLabs/Trove/discovery/internal/stun"
	"github.com/GhentiLabs/Trove/pkg/discovery"
	"github.com/GhentiLabs/Trove/pkg/identity"
)

func main() {
	level := new(slog.LevelVar)
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})).With("component", "discovery-server")
	if err := run(log, level); err != nil {
		log.Error("discovery server exited with error", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, level *slog.LevelVar) error {
	cfg, err := config.Load(os.Args[1:], os.Getenv)
	if err != nil {
		return err
	}
	level.Set(cfg.SlogLevel())
	if cfg.HealthCheck {
		return healthCheck(cfg.MetricsListenAddr)
	}

	key, err := identity.LoadOrCreateKey(cfg.ServerKeyPath)
	if err != nil {
		return err
	}
	cert, err := identity.LoadOrCreateCert(cfg.ServerCertPath, key)
	if err != nil {
		return err
	}
	fingerprint := identity.FingerprintCert(cert.Leaf)
	log.Info("starting discovery server",
		"listen_addr", cfg.PublicListenAddr,
		"metrics_addr", cfg.MetricsListenAddr,
		"fingerprint", fingerprint,
		"connect", connectionString(cfg, fingerprint))

	promReg := prometheus.NewRegistry()
	promReg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	metrics := httpapi.NewMetrics(promReg)

	reg := registry.New(registry.Options{
		MaxEntries:      cfg.RegistryMaxEntries,
		MaxAddrsPerNode: cfg.RegistryMaxAddrsPerNode,
		SweepInterval:   min(cfg.TTLMin, time.Minute),
		OnSizeChange:    metrics.SetRegistrySize,
	})
	defer reg.Close()

	store, err := analytics.Open(cfg.AnalyticsDBPath, cfg.AnalyticsDiskCapBytes, nil)
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	broker := signaling.New(signaling.Options{
		MaxConns:     cfg.MaxWSConns,
		SendBuffer:   cfg.WSSendBuffer,
		PunchOffset:  cfg.PunchOffset,
		PingInterval: cfg.WSPingInterval,
		WriteTimeout: cfg.WriteTimeout,
		RatePerSec:   cfg.SignalRate.RPS,
		RateBurst:    cfg.SignalRate.Burst,
		Logger:       log,
		Resolve: func(nodeID string) ([]discovery.Address, bool) {
			e, ok := reg.Lookup(nodeID)
			if !ok {
				return nil, false
			}
			return e.Addresses, true
		},
		Metrics: signaling.Metrics{
			OnMatch:       metrics.SignalMatch,
			OnActiveDelta: metrics.SignalActiveDelta,
		},
	})

	srv := httpapi.New(httpapi.Deps{
		Config:    cfg,
		Logger:    log,
		Registry:  reg,
		Analytics: store,
		Broker:    broker,
		Metrics:   metrics,
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go srv.StartMaintenance(ctx)

	// Cancelling baseCtx on shutdown tears down live signaling connections
	// instead of waiting on them.
	baseCtx, cancelConns := context.WithCancel(context.Background())
	defer cancelConns()

	public := &http.Server{
		Addr:              cfg.PublicListenAddr,
		Handler:           srv.Handler(),
		TLSConfig:         identity.ServerTLSConfig(cert),
		ReadTimeout:       cfg.ReadTimeout,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		BaseContext:       func(net.Listener) context.Context { return baseCtx },
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}
	metricsSrv := &http.Server{
		Addr:              cfg.MetricsListenAddr,
		Handler:           metricsHandler(promReg, srv.HealthHandler()),
		ReadTimeout:       cfg.ReadTimeout,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
	}

	stunAddr, err := net.ResolveUDPAddr("udp", cfg.STUNListenAddr)
	if err != nil {
		return fmt.Errorf("resolve stun addr: %w", err)
	}
	stunConn, err := net.ListenUDP("udp", stunAddr)
	if err != nil {
		return fmt.Errorf("listen stun: %w", err)
	}
	stunSrv := stun.New(stun.Options{Conn: stunConn, Logger: log, RatePerSec: cfg.STUNRate.RPS, Burst: cfg.STUNRate.Burst})
	defer func() { _ = stunSrv.Close() }()

	errCh := make(chan error, 3)
	go serve(log, "metrics", metricsSrv, errCh)
	go func() {
		log.Info("listening", "server", "public", "addr", public.Addr, "tls", "1.3")
		if err := public.ListenAndServeTLS("", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	go func() {
		log.Info("listening", "server", "stun", "addr", cfg.STUNListenAddr, "proto", "udp")
		if err := stunSrv.Serve(); err != nil {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		log.Info("shutdown signal received")
	case err := <-errCh:
		return err
	}

	cancelConns()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	err = public.Shutdown(shutdownCtx)
	_ = metricsSrv.Shutdown(shutdownCtx)
	if err != nil {
		return err
	}
	log.Info("shutdown complete")
	return nil
}

func metricsHandler(reg *prometheus.Registry, health http.HandlerFunc) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /healthz", health)
	return mux
}

func serve(log *slog.Logger, name string, srv *http.Server, errCh chan<- error) {
	log.Info("listening", "server", name, "addr", srv.Addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		errCh <- err
	}
}

func healthCheck(metricsAddr string) error {
	host, port, err := net.SplitHostPort(metricsAddr)
	if err != nil {
		return err
	}
	switch host {
	case "", "0.0.0.0":
		host = "127.0.0.1"
	case "::":
		host = "::1"
	}
	resp, err := http.Get("http://" + net.JoinHostPort(host, port) + "/healthz")
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("healthcheck: status %d", resp.StatusCode)
	}
	return nil
}

// connectionString renders trove://host:port?id=<fingerprint>. The server cannot
// know any external port mapping (e.g. 443 -> 8443), so the operator sets
// AdvertiseAddr to the public host[:port]; a bare host reuses the listen port.
func connectionString(cfg *config.Config, fingerprint string) string {
	authority := cfg.AdvertiseAddr
	if authority == "" {
		authority = cfg.PublicListenAddr
	} else if _, _, err := net.SplitHostPort(authority); err != nil {
		if _, port, e := net.SplitHostPort(cfg.PublicListenAddr); e == nil {
			authority = net.JoinHostPort(authority, port)
		}
	}
	return fmt.Sprintf("trove://%s?id=%s", authority, fingerprint)
}
