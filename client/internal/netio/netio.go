// Package netio abstracts message-oriented stream transport so a simulated network
// can be substituted in tests. The interfaces are defined here but unused until
// the transport layer is built.
package netio

import "context"

// Stream is a bidirectional, message-framed connection to a single peer.
type Stream interface {
	Send(ctx context.Context, p []byte) error
	Receive(ctx context.Context) ([]byte, error)
	Close() error
}

// Dialer opens a Stream to the peer identified by nodeID.
type Dialer interface {
	Dial(ctx context.Context, nodeID string) (Stream, error)
}
