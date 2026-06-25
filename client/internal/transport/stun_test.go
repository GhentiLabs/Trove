package transport

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/GhentiLabs/Trove/pkg/stun"
)

// stunEcho runs a minimal STUN responder that echoes each request's source as an
// XOR-MAPPED-ADDRESS, like the discovery server's responder.
func stunEcho(t *testing.T) *net.UDPConn {
	t.Helper()
	srv, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	go func() {
		buf := make([]byte, 1500)
		for {
			n, from, err := srv.ReadFromUDP(buf)
			if err != nil {
				return
			}
			if id, ok := stun.ParseRequest(buf[:n]); ok {
				_, _ = srv.WriteToUDP(stun.AppendBindingResponse(nil, id, from.AddrPort()), from)
			}
		}
	}()
	return srv
}

func TestReflexiveOverSharedSocket(t *testing.T) {
	cert, _ := newIdentity(t)
	tr := newTransport(t, cert)
	srv := stunEcho(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := tr.Reflexive(ctx, srv.LocalAddr().String())
	if err != nil {
		t.Fatalf("Reflexive: %v", err)
	}
	// Must match this socket, proving the probe left from the shared QUIC port.
	want := tr.LocalAddr().(*net.UDPAddr).AddrPort()
	if got != want {
		t.Fatalf("reflexive = %v, want this socket %v", got, want)
	}
}

func TestReflexiveTimesOut(t *testing.T) {
	cert, _ := newIdentity(t)
	tr := newTransport(t, cert)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, err := tr.Reflexive(ctx, "127.0.0.1:1"); err == nil {
		t.Fatal("Reflexive to a dead port returned nil error")
	}
}

// TestReflexiveDoesNotBreakQUIC confirms STUN demultiplexing on the shared socket
// leaves the QUIC dial/accept path intact: a Binding exchange runs, then a normal
// pinned session is established on the same transports.
func TestReflexiveDoesNotBreakQUIC(t *testing.T) {
	srvCert, srvID := newIdentity(t)
	cliCert, _ := newIdentity(t)
	srv := newTransport(t, srvCert)
	cli := newTransport(t, cliCert)
	echo := stunEcho(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := cli.Reflexive(ctx, echo.LocalAddr().String()); err != nil {
		t.Fatalf("Reflexive: %v", err)
	}

	go func() { _, _ = srv.Accept(ctx) }()
	if _, err := cli.Dial(ctx, srv.LocalAddr().String(), srvID); err != nil {
		t.Fatalf("Dial after STUN: %v", err)
	}
}
