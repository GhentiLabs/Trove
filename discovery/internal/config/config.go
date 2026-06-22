// Package config loads server configuration from environment variables with
// command-line flag overrides. Loading never panics: invalid input is reported
// as an error and the caller decides how to fail.
package config

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	maxRegistryEntries = 1_000_000
	maxWSConns         = 100_000
)

// RateLimit is a token-bucket setting applied per source key (IP or node).
type RateLimit struct {
	RPS   float64
	Burst int
}

// Config is the fully resolved server configuration.
type Config struct {
	PublicListenAddr  string
	MetricsListenAddr string
	AdvertiseAddr     string

	ServerKeyPath  string
	ServerCertPath string
	HealthCheck    bool

	TTLMin     time.Duration
	TTLDefault time.Duration
	TTLMax     time.Duration

	MaxRequestBodyBytes   int64
	AnalyticsMaxBodyBytes int64
	MaxSignalMsgBytes     int64

	RegistryMaxEntries      int
	RegistryMaxAddrsPerNode int

	AnalyticsDBPath       string
	AnalyticsDiskCapBytes int64

	AllowedWSOrigins []string
	MaxWSConns       int
	WSPingInterval   time.Duration
	WSSendBuffer     int
	PunchOffset      time.Duration

	AnnounceRate  RateLimit
	LookupRate    RateLimit
	AnalyticsRate RateLimit
	SignalRate    RateLimit

	ReadTimeout       time.Duration
	ReadHeaderTimeout time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	ShutdownTimeout   time.Duration
}

func defaults() Config {
	return Config{
		PublicListenAddr:        "0.0.0.0:8443",
		MetricsListenAddr:       "127.0.0.1:9090",
		ServerKeyPath:           "server.key",
		ServerCertPath:          "server.crt",
		TTLMin:                  1 * time.Minute,
		TTLDefault:              10 * time.Minute,
		TTLMax:                  1 * time.Hour,
		MaxRequestBodyBytes:     16 << 10,
		AnalyticsMaxBodyBytes:   256 << 10,
		MaxSignalMsgBytes:       4 << 10,
		RegistryMaxEntries:      100_000,
		RegistryMaxAddrsPerNode: 16,
		AnalyticsDBPath:         "analytics.db",
		AnalyticsDiskCapBytes:   256 << 20,
		MaxWSConns:              5_000,
		WSPingInterval:          30 * time.Second,
		WSSendBuffer:            16,
		PunchOffset:             500 * time.Millisecond,
		AnnounceRate:            RateLimit{RPS: 1, Burst: 5},
		LookupRate:              RateLimit{RPS: 5, Burst: 20},
		AnalyticsRate:           RateLimit{RPS: 0.2, Burst: 5},
		SignalRate:              RateLimit{RPS: 5, Burst: 20},
		ReadTimeout:             10 * time.Second,
		ReadHeaderTimeout:       5 * time.Second,
		WriteTimeout:            10 * time.Second,
		IdleTimeout:             120 * time.Second,
		ShutdownTimeout:         15 * time.Second,
	}
}

// Load resolves configuration from env (via getenv) seeding defaults, then
// applies flag overrides parsed from args. Both getenv and args are injected so
// the loader is testable.
func Load(args []string, getenv func(string) string) (*Config, error) {
	e := &envReader{getenv: getenv}
	c := defaults()

	c.PublicListenAddr = e.str("TROVE_DISCOVERY_LISTEN_ADDR", c.PublicListenAddr)
	c.MetricsListenAddr = e.str("TROVE_DISCOVERY_METRICS_ADDR", c.MetricsListenAddr)
	c.AdvertiseAddr = e.str("TROVE_DISCOVERY_ADVERTISE_ADDR", c.AdvertiseAddr)
	c.ServerKeyPath = e.str("TROVE_DISCOVERY_SERVER_KEY", c.ServerKeyPath)
	c.ServerCertPath = e.str("TROVE_DISCOVERY_SERVER_CERT", c.ServerCertPath)
	c.TTLMin = e.dur("TROVE_DISCOVERY_TTL_MIN", c.TTLMin)
	c.TTLDefault = e.dur("TROVE_DISCOVERY_TTL_DEFAULT", c.TTLDefault)
	c.TTLMax = e.dur("TROVE_DISCOVERY_TTL_MAX", c.TTLMax)
	c.MaxRequestBodyBytes = e.i64("TROVE_DISCOVERY_MAX_BODY_BYTES", c.MaxRequestBodyBytes)
	c.AnalyticsMaxBodyBytes = e.i64("TROVE_DISCOVERY_ANALYTICS_MAX_BODY_BYTES", c.AnalyticsMaxBodyBytes)
	c.MaxSignalMsgBytes = e.i64("TROVE_DISCOVERY_MAX_SIGNAL_BYTES", c.MaxSignalMsgBytes)
	c.RegistryMaxEntries = e.intv("TROVE_DISCOVERY_REGISTRY_MAX_ENTRIES", c.RegistryMaxEntries)
	c.RegistryMaxAddrsPerNode = e.intv("TROVE_DISCOVERY_REGISTRY_MAX_ADDRS", c.RegistryMaxAddrsPerNode)
	c.AnalyticsDBPath = e.str("TROVE_DISCOVERY_ANALYTICS_DB", c.AnalyticsDBPath)
	c.AnalyticsDiskCapBytes = e.i64("TROVE_DISCOVERY_ANALYTICS_DISK_CAP_BYTES", c.AnalyticsDiskCapBytes)
	c.AllowedWSOrigins = e.csv("TROVE_DISCOVERY_WS_ALLOWED_ORIGINS", c.AllowedWSOrigins)
	c.MaxWSConns = e.intv("TROVE_DISCOVERY_MAX_WS_CONNS", c.MaxWSConns)
	c.WSPingInterval = e.dur("TROVE_DISCOVERY_WS_PING_INTERVAL", c.WSPingInterval)
	c.WSSendBuffer = e.intv("TROVE_DISCOVERY_WS_SEND_BUFFER", c.WSSendBuffer)
	c.PunchOffset = e.dur("TROVE_DISCOVERY_PUNCH_OFFSET", c.PunchOffset)
	c.AnnounceRate = e.rate("TROVE_DISCOVERY_RATE_ANNOUNCE", c.AnnounceRate)
	c.LookupRate = e.rate("TROVE_DISCOVERY_RATE_LOOKUP", c.LookupRate)
	c.AnalyticsRate = e.rate("TROVE_DISCOVERY_RATE_ANALYTICS", c.AnalyticsRate)
	c.SignalRate = e.rate("TROVE_DISCOVERY_RATE_SIGNAL", c.SignalRate)
	c.ReadTimeout = e.dur("TROVE_DISCOVERY_READ_TIMEOUT", c.ReadTimeout)
	c.ReadHeaderTimeout = e.dur("TROVE_DISCOVERY_READ_HEADER_TIMEOUT", c.ReadHeaderTimeout)
	c.WriteTimeout = e.dur("TROVE_DISCOVERY_WRITE_TIMEOUT", c.WriteTimeout)
	c.IdleTimeout = e.dur("TROVE_DISCOVERY_IDLE_TIMEOUT", c.IdleTimeout)
	c.ShutdownTimeout = e.dur("TROVE_DISCOVERY_SHUTDOWN_TIMEOUT", c.ShutdownTimeout)

	if err := e.err(); err != nil {
		return nil, err
	}

	if err := bindFlags(&c, args); err != nil {
		return nil, err
	}
	if err := c.validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func bindFlags(c *Config, args []string) error {
	fs := flag.NewFlagSet("trove-server", flag.ContinueOnError)
	fs.StringVar(&c.PublicListenAddr, "listen-addr", c.PublicListenAddr, "public TLS listen address")
	fs.StringVar(&c.MetricsListenAddr, "metrics-addr", c.MetricsListenAddr, "localhost-only metrics listen address")
	fs.StringVar(&c.AdvertiseAddr, "advertise-addr", c.AdvertiseAddr, "public host or host:port shown in the trove:// connection string")
	fs.StringVar(&c.ServerKeyPath, "server-key", c.ServerKeyPath, "path to the persistent Ed25519 server key")
	fs.StringVar(&c.ServerCertPath, "server-cert", c.ServerCertPath, "path to the self-signed server certificate")
	fs.DurationVar(&c.TTLDefault, "ttl-default", c.TTLDefault, "default announce TTL")
	fs.DurationVar(&c.TTLMin, "ttl-min", c.TTLMin, "minimum announce TTL")
	fs.DurationVar(&c.TTLMax, "ttl-max", c.TTLMax, "maximum announce TTL")
	fs.StringVar(&c.AnalyticsDBPath, "analytics-db", c.AnalyticsDBPath, "analytics SQLite path")
	fs.Int64Var(&c.AnalyticsDiskCapBytes, "analytics-disk-cap", c.AnalyticsDiskCapBytes, "analytics disk-usage cap in bytes")
	fs.IntVar(&c.MaxWSConns, "max-ws-conns", c.MaxWSConns, "maximum concurrent signaling connections")
	fs.BoolVar(&c.HealthCheck, "healthcheck", c.HealthCheck, "probe the local metrics /healthz endpoint and exit")
	return fs.Parse(args)
}

func (c *Config) validate() error {
	switch {
	case c.PublicListenAddr == "":
		return fmt.Errorf("config: empty listen address")
	case c.MetricsListenAddr == "":
		return fmt.Errorf("config: empty metrics address")
	case c.TTLMin <= 0 || c.TTLDefault <= 0 || c.TTLMax <= 0:
		return fmt.Errorf("config: TTL values must be positive")
	case c.TTLMin > c.TTLDefault || c.TTLDefault > c.TTLMax:
		return fmt.Errorf("config: require ttl-min <= ttl-default <= ttl-max")
	case c.MaxRequestBodyBytes <= 0 || c.AnalyticsMaxBodyBytes <= 0 || c.MaxSignalMsgBytes <= 0:
		return fmt.Errorf("config: body/message size caps must be positive")
	case c.RegistryMaxEntries <= 0 || c.RegistryMaxAddrsPerNode <= 0:
		return fmt.Errorf("config: registry caps must be positive")
	case c.RegistryMaxEntries > maxRegistryEntries:
		return fmt.Errorf("config: registry max entries %d exceeds safe limit %d for a 1GB host", c.RegistryMaxEntries, maxRegistryEntries)
	case c.AnalyticsDBPath == "":
		return fmt.Errorf("config: empty analytics db path")
	case c.AnalyticsDiskCapBytes <= 0:
		return fmt.Errorf("config: analytics disk cap must be positive")
	case c.MaxWSConns <= 0 || c.WSSendBuffer <= 0:
		return fmt.Errorf("config: websocket caps must be positive")
	case c.MaxWSConns > maxWSConns:
		return fmt.Errorf("config: max websocket connections %d exceeds safe limit %d for a 1GB host", c.MaxWSConns, maxWSConns)
	case c.WSPingInterval <= 0:
		return fmt.Errorf("config: ws ping interval must be positive")
	case c.PunchOffset < 0:
		return fmt.Errorf("config: punch offset must be non-negative")
	case c.ReadTimeout <= 0 || c.ReadHeaderTimeout <= 0 || c.WriteTimeout <= 0 || c.IdleTimeout <= 0 || c.ShutdownTimeout <= 0:
		return fmt.Errorf("config: server timeouts must be positive")
	case c.ServerKeyPath == "" || c.ServerCertPath == "":
		return fmt.Errorf("config: server key and cert paths must be set")
	}
	for _, r := range []RateLimit{c.AnnounceRate, c.LookupRate, c.AnalyticsRate, c.SignalRate} {
		if r.RPS <= 0 || r.Burst <= 0 {
			return fmt.Errorf("config: rate limits must be positive")
		}
	}
	return nil
}

type envReader struct {
	getenv func(string) string
	errs   []string
}

func (e *envReader) fail(key string, err error) {
	e.errs = append(e.errs, fmt.Sprintf("%s: %v", key, err))
}

func (e *envReader) err() error {
	if len(e.errs) == 0 {
		return nil
	}
	return fmt.Errorf("config: invalid environment: %s", strings.Join(e.errs, "; "))
}

func (e *envReader) str(key, def string) string {
	if v := e.getenv(key); v != "" {
		return v
	}
	return def
}

func (e *envReader) intv(key string, def int) int {
	v := e.getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		e.fail(key, err)
		return def
	}
	return n
}

func (e *envReader) i64(key string, def int64) int64 {
	v := e.getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		e.fail(key, err)
		return def
	}
	return n
}

func (e *envReader) dur(key string, def time.Duration) time.Duration {
	v := e.getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		e.fail(key, err)
		return def
	}
	return d
}

func (e *envReader) csv(key string, def []string) []string {
	v := e.getenv(key)
	if v == "" {
		return def
	}
	parts := strings.Split(v, ",")
	out := parts[:0]
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func (e *envReader) rate(prefix string, def RateLimit) RateLimit {
	out := def
	if v := e.getenv(prefix + "_RPS"); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			e.fail(prefix+"_RPS", err)
		} else {
			out.RPS = f
		}
	}
	if v := e.getenv(prefix + "_BURST"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			e.fail(prefix+"_BURST", err)
		} else {
			out.Burst = n
		}
	}
	return out
}
