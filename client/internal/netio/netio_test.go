package netio

import (
	"context"
	"net"
	"testing"
)

// pipeStream proves Stream is implementable with the standard library.
type pipeStream struct{ c net.Conn }

func (s pipeStream) Send(_ context.Context, p []byte) error {
	_, err := s.c.Write(p)
	return err
}

func (s pipeStream) Receive(_ context.Context) ([]byte, error) {
	buf := make([]byte, 64)
	n, err := s.c.Read(buf)
	return buf[:n], err
}

func (s pipeStream) Close() error { return s.c.Close() }

var _ Stream = pipeStream{}

func TestStreamImplementable(t *testing.T) {
	a, b := net.Pipe()
	var send Stream = pipeStream{c: a}
	var recv Stream = pipeStream{c: b}
	t.Cleanup(func() { _ = send.Close(); _ = recv.Close() })

	go func() { _ = send.Send(context.Background(), []byte("ping")) }()

	got, err := recv.Receive(context.Background())
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if string(got) != "ping" {
		t.Fatalf("got %q, want %q", got, "ping")
	}
}
