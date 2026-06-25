package transport

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"testing"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/netio"
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

func newTransport(t *testing.T, cert tls.Certificate) *Transport {
	t.Helper()
	tr, err := New(Options{Cert: cert, UDPAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = tr.Close() })
	return tr
}

func TestDialAcceptPinnedRoundTrip(t *testing.T) {
	srvCert, srvID := newIdentity(t)
	cliCert, cliID := newIdentity(t)
	srv := newTransport(t, srvCert)
	cli := newTransport(t, cliCert)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	type res struct {
		c   netio.Conn
		err error
	}
	ch := make(chan res, 1)
	go func() {
		c, err := srv.Accept(ctx)
		ch <- res{c, err}
	}()

	cc, err := cli.Dial(ctx, srv.LocalAddr().String(), srvID)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	r := <-ch
	if r.err != nil {
		t.Fatalf("Accept: %v", r.err)
	}
	sc := r.c

	if cc.PeerNodeID() != srvID {
		t.Fatalf("client sees peer %q, want server %q", cc.PeerNodeID(), srvID)
	}
	if sc.PeerNodeID() != cliID {
		t.Fatalf("server sees peer %q, want client %q", sc.PeerNodeID(), cliID)
	}

	go func() {
		st, err := cc.OpenStream(ctx)
		if err != nil {
			return
		}
		_, _ = st.Write([]byte("ping"))
		_ = st.Close()
	}()

	ss, err := sc.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}
	got, err := io.ReadAll(ss)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "ping" {
		t.Fatalf("got %q, want ping", got)
	}
}

func TestDialPinMismatchRejected(t *testing.T) {
	srvCert, _ := newIdentity(t)
	cliCert, _ := newIdentity(t)
	_, wrongID := newIdentity(t)
	srv := newTransport(t, srvCert)
	cli := newTransport(t, cliCert)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() { _, _ = srv.Accept(ctx) }()

	if _, err := cli.Dial(ctx, srv.LocalAddr().String(), wrongID); err == nil {
		t.Fatal("Dial pinned to the wrong node id must fail at the handshake")
	}
}

func TestProbeAndLocalAddr(t *testing.T) {
	cert, _ := newIdentity(t)
	tr := newTransport(t, cert)
	if tr.LocalAddr() == nil {
		t.Fatal("LocalAddr is nil")
	}
	bad := []string{tr.LocalAddr().String(), "239.0.0.1:1", "not-an-addr", "0.0.0.0:1"}
	if err := tr.Probe(context.Background(), bad); err != nil {
		t.Fatalf("Probe should skip non-routable candidates without error: %v", err)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	cert, _ := newIdentity(t)
	tr, err := New(Options{Cert: cert, UDPAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestCloseReleasesSocket(t *testing.T) {
	cert, _ := newIdentity(t)
	tr, err := New(Options{Cert: cert, UDPAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	addr := tr.LocalAddr().(*net.UDPAddr)
	if err := tr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	c, err := net.ListenUDP("udp", addr)
	if err != nil {
		t.Fatalf("port not released after Close: %v", err)
	}
	_ = c.Close()
}
