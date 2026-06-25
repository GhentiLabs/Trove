package httpapi

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/GhentiLabs/Trove/discovery/internal/analytics"
	"github.com/GhentiLabs/Trove/discovery/internal/config"
	"github.com/GhentiLabs/Trove/discovery/internal/registry"
	"github.com/GhentiLabs/Trove/discovery/internal/signaling"
	"github.com/GhentiLabs/Trove/pkg/discovery"
	"github.com/GhentiLabs/Trove/pkg/identity"
)

type harness struct {
	srv       *Server
	ts        *httptest.Server
	store     *analytics.Store
	broker    *signaling.Broker
	serverPin string
	wsURL     string
}

func newHarness(t *testing.T, tweaks ...func(*config.Config)) *harness {
	t.Helper()
	cfg, err := config.Load(nil, func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	cfg.AnalyticsDBPath = filepath.Join(t.TempDir(), "a.db")
	high := config.RateLimit{RPS: 1e6, Burst: 1e6}
	cfg.AnnounceRate, cfg.LookupRate, cfg.AnalyticsRate, cfg.SignalRate = high, high, high, high
	for _, tw := range tweaks {
		tw(cfg)
	}

	reg := registry.New(registry.Options{MaxEntries: cfg.RegistryMaxEntries, MaxAddrsPerNode: cfg.RegistryMaxAddrsPerNode})
	t.Cleanup(reg.Close)

	store, err := analytics.Open(cfg.AnalyticsDBPath, cfg.AnalyticsDiskCapBytes, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	metrics := NewMetrics(prometheus.NewRegistry())
	broker := signaling.New(signaling.Options{
		MaxConns: cfg.MaxWSConns, SendBuffer: cfg.WSSendBuffer, PunchOffset: cfg.PunchOffset,
		PingInterval: cfg.WSPingInterval, WriteTimeout: cfg.WriteTimeout,
		RatePerSec: cfg.SignalRate.RPS, RateBurst: cfg.SignalRate.Burst,
		Resolve: func(id string) ([]discovery.Address, bool) {
			e, ok := reg.Lookup(id)
			if !ok {
				return nil, false
			}
			return e.Addresses, true
		},
		Metrics: signaling.Metrics{OnMatch: metrics.SignalMatch, OnActiveDelta: metrics.SignalActiveDelta},
	})

	srv := New(Deps{
		Config: cfg, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Registry: reg, Analytics: store, Broker: broker, Metrics: metrics,
	})

	_, serverKey, err := identity.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	serverCert, err := identity.NewCertificate(serverKey)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewUnstartedServer(srv.Handler())
	ts.TLS = identity.ServerTLSConfig(serverCert)
	// Mirror production's http.Server deadlines so tests see the same socket-timeout
	// behavior the real listener imposes (main.go), not httptest's no-timeout default.
	ts.Config.ReadTimeout = cfg.ReadTimeout
	ts.Config.WriteTimeout = cfg.WriteTimeout
	ts.Config.IdleTimeout = cfg.IdleTimeout
	ts.StartTLS()
	t.Cleanup(ts.Close)

	return &harness{
		srv: srv, ts: ts, store: store, broker: broker,
		serverPin: identity.FingerprintCert(serverCert.Leaf),
		wsURL:     "wss" + strings.TrimPrefix(ts.URL, "https") + "/v1/signal",
	}
}

func genClient(t *testing.T) (tls.Certificate, string) {
	t.Helper()
	_, key, err := identity.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	cert, err := identity.NewCertificate(key)
	if err != nil {
		t.Fatal(err)
	}
	return cert, identity.FingerprintCert(cert.Leaf)
}

func (h *harness) client(cert tls.Certificate, pin string) *http.Client {
	return &http.Client{Transport: &http.Transport{TLSClientConfig: identity.PinnedClientConfig(cert, pin)}}
}

func (h *harness) post(t *testing.T, cert tls.Certificate, path string, body any) (int, []byte) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	return h.postRaw(t, cert, path, raw)
}

func (h *harness) postRaw(t *testing.T, cert tls.Certificate, path string, raw []byte) (int, []byte) {
	t.Helper()
	resp, err := h.client(cert, h.serverPin).Post(h.ts.URL+path, "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("post %s: %v", path, err)
	}
	data, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp.StatusCode, data
}

func (h *harness) announce(t *testing.T, cert tls.Certificate, addrs []discovery.Address) discovery.AnnounceResponse {
	t.Helper()
	status, data := h.post(t, cert, "/v1/announce", discovery.AnnounceRequest{Addresses: addrs, RequestedTTLSecs: 600})
	if status != http.StatusOK {
		t.Fatalf("announce status = %d, body = %s", status, data)
	}
	var out discovery.AnnounceResponse
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestAnnounceLookupFlow(t *testing.T) {
	h := newHarness(t)
	certA, idA := genClient(t)
	certB, idB := genClient(t)
	addrsB := []discovery.Address{{IP: "203.0.113.9", Port: 5000, Type: discovery.AddressPublic}}

	respA := h.announce(t, certA, []discovery.Address{{IP: "198.51.100.1", Port: 4000, Type: discovery.AddressPublic}})
	if respA.NodeID != idA {
		t.Fatalf("announce derived node id %q, want cert fingerprint %q", respA.NodeID, idA)
	}
	h.announce(t, certB, addrsB)

	status, data := h.post(t, certA, "/v1/lookup", discovery.LookupRequest{TargetNodeID: idB})
	if status != http.StatusOK {
		t.Fatalf("lookup status = %d, body = %s", status, data)
	}
	var lr discovery.LookupResponse
	if err := json.Unmarshal(data, &lr); err != nil {
		t.Fatal(err)
	}
	if len(lr.Addresses) != 1 || lr.Addresses[0] != addrsB[0] {
		t.Fatalf("lookup returned wrong addresses: %+v", lr.Addresses)
	}

	missing := strings.Repeat("a", identity.NodeIDLen)
	if status, _ := h.post(t, certA, "/v1/lookup", discovery.LookupRequest{TargetNodeID: missing}); status != http.StatusNotFound {
		t.Fatalf("missing lookup status = %d, want 404", status)
	}
}

func TestAnalyticsStoresDerivedIdentity(t *testing.T) {
	h := newHarness(t)
	cert, id := genClient(t)
	req := discovery.AnalyticsRequest{
		InstallID:     "install-7",
		SchemaVersion: 1,
		EventMillis:   time.Now().UnixMilli(),
		Fields:        map[string]any{"event": "startup", "peers": 3.0},
	}
	if status, data := h.post(t, cert, "/v1/analytics", req); status != http.StatusOK {
		t.Fatalf("analytics status = %d, body = %s", status, data)
	}

	records, err := h.store.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	if records[0].NodeID != id {
		t.Fatalf("stored node id = %q, want cert fingerprint %q", records[0].NodeID, id)
	}
	if records[0].Fields["event"] != "startup" {
		t.Fatalf("open fields not stored: %+v", records[0].Fields)
	}
}

func TestClampTTLUsesDefault(t *testing.T) {
	h := newHarness(t)
	if got := h.srv.clampTTL(0); got != 10*time.Minute {
		t.Fatalf("clampTTL(0) = %v, want default 10m", got)
	}
}

func TestWrongPinRejected(t *testing.T) {
	h := newHarness(t)
	cert, _ := genClient(t)
	wrongPin := strings.Repeat("a", identity.NodeIDLen)
	bad := h.client(cert, wrongPin)
	_, err := bad.Post(h.ts.URL+"/v1/announce", "application/json", strings.NewReader("{}"))
	if err == nil {
		t.Fatal("request with wrong server pin succeeded, want TLS rejection")
	}
	if !strings.Contains(err.Error(), "does not match pin") {
		t.Fatalf("wrong-pin rejected for the wrong reason: %v", err)
	}
}

func TestNoClientCertRejected(t *testing.T) {
	h := newHarness(t)
	noCert := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{
		InsecureSkipVerify: true, MinVersion: tls.VersionTLS13,
	}}}
	if _, err := noCert.Post(h.ts.URL+"/v1/announce", "application/json", strings.NewReader("{}")); err == nil {
		t.Fatal("request without a client cert succeeded, want mTLS rejection")
	}
}

func waitActive(t *testing.T, b *signaling.Broker, n int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if b.Active() == n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("broker active = %d, want %d", b.Active(), n)
}

func dialSignal(t *testing.T, h *harness, cert tls.Certificate) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, h.wsURL, &websocket.DialOptions{HTTPClient: h.client(cert, h.serverPin)})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	writeSignal(t, c, discovery.SignalHello, discovery.Hello{})
	return c
}

func writeSignal(t *testing.T, c *websocket.Conn, typ discovery.SignalType, payload any) {
	t.Helper()
	msg, err := discovery.NewSignalMessage(typ, payload)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Write(ctx, websocket.MessageText, data); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func readSignal(t *testing.T, c *websocket.Conn, want discovery.SignalType, into any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, data, err := c.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var msg discovery.SignalMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatal(err)
	}
	if msg.Type != want {
		t.Fatalf("got signal type %q, want %q (payload %s)", msg.Type, want, msg.Payload)
	}
	if into != nil {
		if err := msg.Decode(into); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSignalingExchange(t *testing.T) {
	h := newHarness(t)
	certA, idA := genClient(t)
	certB, idB := genClient(t)
	addrsB := []discovery.Address{{IP: "203.0.113.9", Port: 5000, Type: discovery.AddressPublic}}
	h.announce(t, certB, addrsB)

	connA := dialSignal(t, h, certA)
	defer connA.CloseNow()
	connB := dialSignal(t, h, certB)
	defer connB.CloseNow()
	waitActive(t, h.broker, 2)

	aCandidates := []discovery.Address{{IP: "198.51.100.1", Port: 4000, Type: discovery.AddressPublic}}
	writeSignal(t, connA, discovery.SignalConnectRequest, discovery.ConnectRequest{TargetNodeID: idB, MyCandidates: aCandidates})

	var incoming discovery.IncomingRequest
	readSignal(t, connB, discovery.SignalIncomingRequest, &incoming)
	if incoming.FromNodeID != idA || len(incoming.Candidates) != 1 || incoming.Candidates[0] != aCandidates[0] {
		t.Fatalf("B got wrong incoming_request: %+v", incoming)
	}

	var peer discovery.PeerCandidates
	readSignal(t, connA, discovery.SignalPeerCandidates, &peer)
	if peer.FromNodeID != idB || len(peer.Candidates) != 1 || peer.Candidates[0] != addrsB[0] {
		t.Fatalf("A got wrong peer_candidates: %+v", peer)
	}
	if incoming.PunchAtMillis != peer.PunchAtMillis || peer.PunchAtMillis == 0 {
		t.Fatalf("punch times not synchronized: %d vs %d", incoming.PunchAtMillis, peer.PunchAtMillis)
	}

	writeSignal(t, connA, discovery.SignalConnectRequest, discovery.ConnectRequest{TargetNodeID: strings.Repeat("a", identity.NodeIDLen)})
	readSignal(t, connA, discovery.SignalTargetUnavailable, nil)
}

// TestSignalingSurvivesWriteTimeout guards the long-lived signaling WebSocket
// against the http.Server's WriteTimeout: brokering must keep working long after
// the timeout would have elapsed for a normal request. (It holds because hijacking
// the connection clears the inherited socket deadlines; this test locks that in.)
func TestSignalingSurvivesWriteTimeout(t *testing.T) {
	h := newHarness(t, func(c *config.Config) { c.WriteTimeout = 300 * time.Millisecond })
	certA, _ := genClient(t)
	certB, idB := genClient(t)
	h.announce(t, certB, []discovery.Address{{IP: "203.0.113.9", Port: 5000, Type: discovery.AddressPublic}})

	connA := dialSignal(t, h, certA)
	defer connA.CloseNow()
	connB := dialSignal(t, h, certB)
	defer connB.CloseNow()
	waitActive(t, h.broker, 2)

	// Hold both connections idle well past the server's WriteTimeout.
	time.Sleep(700 * time.Millisecond)

	// Brokering must still deliver in both directions after the timeout elapsed.
	writeSignal(t, connA, discovery.SignalConnectRequest, discovery.ConnectRequest{
		TargetNodeID: idB,
		MyCandidates: []discovery.Address{{IP: "198.51.100.1", Port: 4000, Type: discovery.AddressPublic}},
	})
	readSignal(t, connB, discovery.SignalIncomingRequest, nil)
	readSignal(t, connA, discovery.SignalPeerCandidates, nil)
}

func TestHealthHandler(t *testing.T) {
	h := newHarness(t)
	hs := httptest.NewServer(h.srv.HealthHandler())
	defer hs.Close()

	resp, err := http.Get(hs.URL)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health status = %d", resp.StatusCode)
	}
	var hr discovery.HealthResponse
	if err := json.Unmarshal(data, &hr); err != nil {
		t.Fatal(err)
	}
	if hr.Status != "ok" {
		t.Fatalf("health status field = %q", hr.Status)
	}
}

func TestHostileInputs(t *testing.T) {
	h := newHarness(t)
	cert, _ := genClient(t)
	good := []discovery.Address{{IP: "198.51.100.1", Port: 4000, Type: discovery.AddressPublic}}

	t.Run("malformed json", func(t *testing.T) {
		if status, _ := h.postRaw(t, cert, "/v1/announce", []byte("{not json")); status != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", status)
		}
	})

	t.Run("oversized body", func(t *testing.T) {
		many := make([]discovery.Address, 500)
		for i := range many {
			many[i] = good[0]
		}
		if status, _ := h.post(t, cert, "/v1/announce", discovery.AnnounceRequest{Addresses: many}); status != http.StatusRequestEntityTooLarge {
			t.Fatalf("status = %d, want 413", status)
		}
	})

	t.Run("invalid address", func(t *testing.T) {
		req := discovery.AnnounceRequest{Addresses: []discovery.Address{{IP: "999.999.0.1", Port: 1, Type: discovery.AddressLAN}}, RequestedTTLSecs: 300}
		if status, _ := h.post(t, cert, "/v1/announce", req); status != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", status)
		}
	})

	if h.announce(t, cert, good).NodeID == "" {
		t.Fatal("server unhealthy after hostile inputs")
	}
}
