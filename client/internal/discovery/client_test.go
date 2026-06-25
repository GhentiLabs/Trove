package discovery

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	disco "github.com/GhentiLabs/Trove/pkg/discovery"
	"github.com/GhentiLabs/Trove/pkg/identity"
)

func newIdentity(t *testing.T) (tls.Certificate, string) {
	t.Helper()
	_, key, err := identity.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	cert, err := identity.NewCertificate(key)
	if err != nil {
		t.Fatalf("NewCertificate: %v", err)
	}
	return cert, identity.FingerprintCert(cert.Leaf)
}

// fakeTrove starts an mTLS test server with the given handler and returns a
// trove:// URL pinning its fingerprint.
func fakeTrove(t *testing.T, h http.Handler) string {
	t.Helper()
	srvCert, srvID := newIdentity(t)
	s := httptest.NewUnstartedServer(h)
	s.TLS = identity.ServerTLSConfig(srvCert)
	s.StartTLS()
	t.Cleanup(s.Close)
	u, err := url.Parse(s.URL)
	if err != nil {
		t.Fatalf("parse server url: %v", err)
	}
	return "trove://" + u.Host + "?id=" + srvID
}

func newClient(t *testing.T, troveURL string, cert tls.Certificate) *Client {
	t.Helper()
	c, err := New(Options{Server: troveURL, Cert: cert})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

func TestAnnounceDerivesNodeIDFromCert(t *testing.T) {
	cliCert, cliID := newIdentity(t)
	var gotReq disco.AnnounceRequest
	url := fakeTrove(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/announce" {
			http.Error(w, "bad path", http.StatusNotFound)
			return
		}
		id, err := identity.PeerFingerprint(r.TLS)
		if err != nil {
			http.Error(w, "no cert", http.StatusUnauthorized)
			return
		}
		_ = json.NewDecoder(r.Body).Decode(&gotReq)
		writeJSON(t, w, disco.AnnounceResponse{NodeID: id, ObservedAddr: "203.0.113.7:9000", GrantedTTLSecs: 600})
	}))

	c := newClient(t, url, cliCert)
	addrs := []disco.Address{{IP: "192.0.2.5", Port: 22000, Type: disco.AddressLAN}}
	resp, err := c.Announce(context.Background(), addrs, 600*time.Second)
	if err != nil {
		t.Fatalf("Announce: %v", err)
	}
	if resp.NodeID != cliID {
		t.Fatalf("server derived node id %q, want client %q", resp.NodeID, cliID)
	}
	if resp.ObservedAddr != "203.0.113.7:9000" {
		t.Fatalf("observed addr = %q", resp.ObservedAddr)
	}
	if len(gotReq.Addresses) != 1 || gotReq.Addresses[0].Port != 22000 {
		t.Fatalf("server received addresses %+v", gotReq.Addresses)
	}
}

func TestLookupFoundAndMiss(t *testing.T) {
	cliCert, _ := newIdentity(t)
	target := "dddddddddddddddddddddddddddddddddddddddddddddddddddd"
	url := fakeTrove(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req disco.LookupRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.TargetNodeID != target {
			writeError(w, http.StatusNotFound, "not_found")
			return
		}
		writeJSON(t, w, disco.LookupResponse{
			NodeID:    target,
			Addresses: []disco.Address{{IP: "198.51.100.9", Port: 22000, Type: disco.AddressPublic}},
		})
	}))

	c := newClient(t, url, cliCert)
	resp, err := c.Lookup(context.Background(), target)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if len(resp.Addresses) != 1 || resp.Addresses[0].IP != "198.51.100.9" {
		t.Fatalf("lookup addresses = %+v", resp.Addresses)
	}

	if _, err := c.Lookup(context.Background(), "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee0"); !errors.Is(err, ErrPeerNotFound) {
		t.Fatalf("Lookup of unknown node err = %v, want ErrPeerNotFound", err)
	}
}

func TestNewRejectsBadTroveURL(t *testing.T) {
	cert, _ := newIdentity(t)
	for _, bad := range []string{"http://x?id=y", "trove://host:1", "trove://?id=y", "not a url"} {
		if _, err := New(Options{Server: bad, Cert: cert}); err == nil {
			t.Fatalf("New(%q) should fail", bad)
		}
	}
}

func writeJSON(t *testing.T, w http.ResponseWriter, v any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		t.Fatalf("encode: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(disco.Error{Code: code, Message: code})
}
