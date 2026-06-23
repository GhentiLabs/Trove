package netio

import (
	"context"
	"io"
	"testing"
)

func TestMemTransportConnAndStreams(t *testing.T) {
	ctx := context.Background()
	mn := NewMemNet()
	server := mn.Transport("srv", "server-node")
	client := mn.Transport("cli", "client-node")

	type accepted struct {
		c   Conn
		err error
	}
	ch := make(chan accepted, 1)
	go func() {
		c, err := server.Accept(ctx)
		ch <- accepted{c, err}
	}()

	cc, err := client.Dial(ctx, "srv", "server-node")
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	a := <-ch
	if a.err != nil {
		t.Fatalf("Accept: %v", a.err)
	}
	sc := a.c

	if cc.PeerNodeID() != "server-node" {
		t.Fatalf("client sees peer %q, want server-node", cc.PeerNodeID())
	}
	if sc.PeerNodeID() != "client-node" {
		t.Fatalf("server sees peer %q, want client-node", sc.PeerNodeID())
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

func TestMemTransportDialUnknownAddr(t *testing.T) {
	mn := NewMemNet()
	client := mn.Transport("cli", "client-node")
	if _, err := client.Dial(context.Background(), "nowhere", "x"); err == nil {
		t.Fatal("Dial to unregistered addr should fail")
	}
}

func TestMemTransportDialPinMismatch(t *testing.T) {
	mn := NewMemNet()
	mn.Transport("srv", "server-node")
	client := mn.Transport("cli", "client-node")
	if _, err := client.Dial(context.Background(), "srv", "imposter-node"); err == nil {
		t.Fatal("Dial with wrong expected node id should fail (pin mismatch)")
	}
}

func TestMemConnCloseUnblocksPeerAccept(t *testing.T) {
	ctx := context.Background()
	mn := NewMemNet()
	server := mn.Transport("srv", "server-node")
	client := mn.Transport("cli", "client-node")

	ch := make(chan Conn, 1)
	go func() { c, _ := server.Accept(ctx); ch <- c }()
	cc, err := client.Dial(ctx, "srv", "server-node")
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	sc := <-ch

	done := make(chan error, 1)
	go func() { _, err := sc.AcceptStream(ctx); done <- err }()

	if err := cc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := <-done; err == nil {
		t.Fatal("peer AcceptStream should error after the connection closes")
	}
}

func TestMemTransportCloseUnblocksAccept(t *testing.T) {
	ctx := context.Background()
	mn := NewMemNet()
	server := mn.Transport("srv", "server-node")

	done := make(chan error, 1)
	go func() { _, err := server.Accept(ctx); done <- err }()
	if err := server.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := <-done; err == nil {
		t.Fatal("Accept should error after the transport closes")
	}
}

func TestMemTransportLocalAddrAndProbe(t *testing.T) {
	mn := NewMemNet()
	tr := mn.Transport("srv", "server-node")
	if tr.LocalAddr() == nil || tr.LocalAddr().String() != "srv" {
		t.Fatalf("LocalAddr = %v, want srv", tr.LocalAddr())
	}
	if err := tr.Probe(context.Background(), []string{"1.2.3.4:5"}); err != nil {
		t.Fatalf("Probe (no-op) err = %v", err)
	}
}

func TestMemConnCloseRejectsOpenStream(t *testing.T) {
	ctx := context.Background()
	mn := NewMemNet()
	server := mn.Transport("srv", "server-node")
	client := mn.Transport("cli", "client-node")

	go func() { _, _ = server.Accept(ctx) }()
	cc, err := client.Dial(ctx, "srv", "server-node")
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if err := cc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := cc.OpenStream(ctx); err == nil {
		t.Fatal("OpenStream after Close should fail")
	}
}
