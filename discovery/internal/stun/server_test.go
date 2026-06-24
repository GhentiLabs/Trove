package stun

import (
	"net"
	"testing"
	"time"

	pkgstun "github.com/GhentiLabs/Trove/pkg/stun"
)

func newServer(t *testing.T, rps float64, burst int) *Server {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	srv := New(Options{Conn: conn, RatePerSec: rps, Burst: burst})
	go func() { _ = srv.Serve() }()
	t.Cleanup(func() { _ = srv.Close() })
	return srv
}

func TestServerEchoesSource(t *testing.T) {
	srv := newServer(t, 100, 100)

	cli, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("client ListenUDP: %v", err)
	}
	defer func() { _ = cli.Close() }()

	id, _ := pkgstun.NewTxID()
	if _, err := cli.WriteToUDP(pkgstun.AppendBindingRequest(nil, id), srv.conn.LocalAddr().(*net.UDPAddr)); err != nil {
		t.Fatalf("write request: %v", err)
	}
	_ = cli.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1500)
	n, _, err := cli.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	gotID, mapped, ok := pkgstun.ParseResponse(buf[:n])
	if !ok {
		t.Fatal("ParseResponse ok=false")
	}
	if gotID != id {
		t.Fatalf("txid: got %x want %x", gotID, id)
	}
	if want := cli.LocalAddr().(*net.UDPAddr).AddrPort(); mapped != want {
		t.Fatalf("mapped = %v, want client source %v", mapped, want)
	}
}

func TestServerIgnoresNonRequest(t *testing.T) {
	srv := newServer(t, 100, 100)
	cli, _ := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	defer func() { _ = cli.Close() }()

	// A non-STUN datagram must draw no response.
	_, _ = cli.WriteToUDP([]byte("not stun"), srv.conn.LocalAddr().(*net.UDPAddr))
	_ = cli.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	buf := make([]byte, 1500)
	if _, _, err := cli.ReadFromUDP(buf); err == nil {
		t.Fatal("got a response to a non-STUN datagram")
	}
}

func TestServerRateLimitDrops(t *testing.T) {
	srv := newServer(t, 0.0001, 0) // burst 0 -> every request dropped
	cli, _ := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	defer func() { _ = cli.Close() }()

	id, _ := pkgstun.NewTxID()
	_, _ = cli.WriteToUDP(pkgstun.AppendBindingRequest(nil, id), srv.conn.LocalAddr().(*net.UDPAddr))
	_ = cli.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	buf := make([]byte, 1500)
	if _, _, err := cli.ReadFromUDP(buf); err == nil {
		t.Fatal("rate limiter did not drop the request")
	}
}
