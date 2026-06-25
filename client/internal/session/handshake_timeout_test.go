package session

import (
	"context"
	"testing"
	"time"
)

// TestHandshakeTimesOutOnStalledPeer covers the slowloris guard.
func TestHandshakeTimesOutOnStalledPeer(t *testing.T) {
	ac, bc := connPair(t, idA, idB)
	ctx := context.Background()

	go func() { _, _ = ac.OpenStream(ctx) }() // opens the stream, then sends nothing

	start := time.Now()
	_, err := Handshake(ctx, Config{
		Conn: bc, Initiator: false, Authorize: allow,
		Local:            Local{NodeID: idB},
		HandshakeTimeout: 100 * time.Millisecond,
	})
	if err == nil {
		t.Fatal("expected handshake to fail on a stalled peer, got nil")
	}
	if d := time.Since(start); d < 50*time.Millisecond || d > 1*time.Second {
		t.Fatalf("handshake returned after %v, want ~100ms (the configured HandshakeTimeout)", d)
	}
}
