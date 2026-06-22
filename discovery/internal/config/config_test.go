package config

import (
	"testing"
	"time"
)

func emptyEnv(string) string { return "" }

func mapEnv(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadDefaults(t *testing.T) {
	c, err := Load(nil, emptyEnv)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.PublicListenAddr != "0.0.0.0:8443" {
		t.Errorf("listen addr = %q", c.PublicListenAddr)
	}
	if c.MetricsListenAddr != "127.0.0.1:9090" {
		t.Errorf("metrics addr = %q", c.MetricsListenAddr)
	}
	if c.ServerKeyPath != "server.key" || c.ServerCertPath != "server.crt" {
		t.Errorf("server key/cert = %q / %q", c.ServerKeyPath, c.ServerCertPath)
	}
	if !c.RequireClientCert {
		t.Error("require client cert should default true")
	}
	if c.MaxSignalMsgBytes != 4096 {
		t.Errorf("max signal bytes = %d", c.MaxSignalMsgBytes)
	}
}

func TestLoadEnvOverride(t *testing.T) {
	c, err := Load(nil, mapEnv(map[string]string{
		"TROVE_DISCOVERY_LISTEN_ADDR":        "0.0.0.0:9000",
		"TROVE_DISCOVERY_TTL_DEFAULT":        "30m",
		"TROVE_DISCOVERY_RATE_LOOKUP_RPS":    "12.5",
		"TROVE_DISCOVERY_WS_ALLOWED_ORIGINS": "a.example.com, b.example.com ",
	}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.PublicListenAddr != "0.0.0.0:9000" {
		t.Errorf("listen addr = %q", c.PublicListenAddr)
	}
	if c.TTLDefault != 30*time.Minute {
		t.Errorf("ttl default = %v", c.TTLDefault)
	}
	if c.LookupRate.RPS != 12.5 {
		t.Errorf("lookup rps = %v", c.LookupRate.RPS)
	}
	if len(c.AllowedWSOrigins) != 2 || c.AllowedWSOrigins[1] != "b.example.com" {
		t.Errorf("origins = %v", c.AllowedWSOrigins)
	}
}

func TestFlagOverridesEnv(t *testing.T) {
	c, err := Load([]string{"-listen-addr", "127.0.0.1:7777", "-ttl-default", "1m"},
		mapEnv(map[string]string{"TROVE_DISCOVERY_LISTEN_ADDR": "0.0.0.0:9000"}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.PublicListenAddr != "127.0.0.1:7777" {
		t.Errorf("flag did not override env: %q", c.PublicListenAddr)
	}
}

func TestLoadRejectsBadEnv(t *testing.T) {
	if _, err := Load(nil, mapEnv(map[string]string{"TROVE_DISCOVERY_TTL_DEFAULT": "not-a-duration"})); err == nil {
		t.Fatal("expected error for invalid duration")
	}
}

func TestValidateRejectsInvertedTTL(t *testing.T) {
	if _, err := Load(nil, mapEnv(map[string]string{
		"TROVE_DISCOVERY_TTL_MIN": "2h",
		"TROVE_DISCOVERY_TTL_MAX": "1h",
	})); err == nil {
		t.Fatal("expected error for ttl-min > ttl-max")
	}
}

func TestValidateRejectsEmptyKeyPath(t *testing.T) {
	if _, err := Load([]string{"-server-key", ""}, emptyEnv); err == nil {
		t.Fatal("expected error for empty server key path")
	}
}

func TestValidateRejectsNonPositivePingInterval(t *testing.T) {
	if _, err := Load(nil, mapEnv(map[string]string{"TROVE_DISCOVERY_WS_PING_INTERVAL": "0s"})); err == nil {
		t.Fatal("expected error for zero ping interval")
	}
}

func TestValidateRejectsOversizedCaps(t *testing.T) {
	if _, err := Load(nil, mapEnv(map[string]string{"TROVE_DISCOVERY_REGISTRY_MAX_ENTRIES": "100000000"})); err == nil {
		t.Fatal("expected error for registry max entries above safe limit")
	}
}
