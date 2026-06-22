package identity

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func mustKey(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	return pub, priv
}

func certFor(t *testing.T) tls.Certificate {
	t.Helper()
	_, priv := mustKey(t)
	cert, err := NewCertificate(priv)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}

func TestFingerprintDeterministicAndShaped(t *testing.T) {
	cert := certFor(t)
	fp := FingerprintCert(cert.Leaf)
	if fp != FingerprintCert(cert.Leaf) {
		t.Fatal("FingerprintCert not deterministic")
	}
	if len(fp) != NodeIDLen || !ValidNodeID(fp) {
		t.Fatalf("malformed fingerprint %q", fp)
	}
}

func TestFingerprintDistinctKeys(t *testing.T) {
	a := FingerprintCert(certFor(t).Leaf)
	b := FingerprintCert(certFor(t).Leaf)
	if a == b {
		t.Fatal("distinct keys produced the same fingerprint")
	}
}

func TestValidNodeIDRejectsGarbage(t *testing.T) {
	for _, c := range []string{"", "tooshort", "0189", string(make([]byte, NodeIDLen))} {
		if ValidNodeID(c) {
			t.Errorf("ValidNodeID(%q) = true, want false", c)
		}
	}
}

func TestLoadOrCreateKeyPersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "server.key")
	first, err := LoadOrCreateKey(path)
	if err != nil {
		t.Fatal(err)
	}
	second, err := LoadOrCreateKey(path)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Equal(second) {
		t.Fatal("reloaded key differs from the persisted key")
	}
}

func TestCertRegenSameKeySameFingerprint(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "server.key")
	certPath := filepath.Join(dir, "server.crt")
	key, err := LoadOrCreateKey(keyPath)
	if err != nil {
		t.Fatal(err)
	}
	c1, err := LoadOrCreateCert(certPath, key)
	if err != nil {
		t.Fatal(err)
	}
	regen, err := NewCertificate(key)
	if err != nil {
		t.Fatal(err)
	}
	if FingerprintCert(c1.Leaf) != FingerprintCert(regen.Leaf) {
		t.Fatal("regenerated cert with same key changed the fingerprint")
	}
}

func TestPinnedDialAcceptsAndRejects(t *testing.T) {
	_, serverKey := mustKey(t)
	serverCert, err := NewCertificate(serverKey)
	if err != nil {
		t.Fatal(err)
	}
	_, clientKey := mustKey(t)
	clientCert, err := NewCertificate(clientKey)
	if err != nil {
		t.Fatal(err)
	}
	serverPin := FingerprintCert(serverCert.Leaf)

	ln, err := tls.Listen("tcp", "127.0.0.1:0", ServerTLSConfig(serverCert, true))
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	peerCh := make(chan string, 1)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				tc := conn.(*tls.Conn)
				if err := tc.Handshake(); err != nil {
					return
				}
				state := tc.ConnectionState()
				fp, _ := PeerFingerprint(&state)
				select {
				case peerCh <- fp:
				default:
				}
			}()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := DialPinned(ctx, "tcp", ln.Addr().String(), clientCert, serverPin)
	if err != nil {
		t.Fatalf("pinned dial with correct pin failed: %v", err)
	}
	conn.Close()
	if got := <-peerCh; got != FingerprintCert(clientCert.Leaf) {
		t.Fatalf("server derived client fingerprint %q, want %q", got, FingerprintCert(clientCert.Leaf))
	}

	wrongPin := strings.Repeat("a", NodeIDLen)
	wrong, err := DialPinned(ctx, "tcp", ln.Addr().String(), clientCert, wrongPin)
	if err == nil {
		wrong.Close()
		t.Fatal("pinned dial with wrong pin succeeded, want rejection")
	}
	if !strings.Contains(err.Error(), "does not match pin") {
		t.Fatalf("wrong-pin rejected for the wrong reason: %v", err)
	}
}

func TestPeerFingerprintNoCert(t *testing.T) {
	if _, err := PeerFingerprint(&tls.ConnectionState{}); err == nil {
		t.Fatal("expected error for connection with no peer certificate")
	}
	if _, err := PeerFingerprint(nil); err == nil {
		t.Fatal("expected error for nil connection state")
	}
}
