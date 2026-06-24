package transport

import (
	"context"
	"fmt"

	"github.com/GhentiLabs/Trove/client/internal/netio"
	"github.com/GhentiLabs/Trove/pkg/identity"
	"github.com/quic-go/quic-go"
)

// closeErrorCode is the QUIC application error code for a normal close; 0 carries
// no application semantics (RFC 9000).
const closeErrorCode = 0

type conn struct {
	qc         *quic.Conn
	peerNodeID string
}

func newConn(qc *quic.Conn) (netio.Conn, error) {
	state := qc.ConnectionState()
	nodeID, err := identity.PeerFingerprint(&state.TLS)
	if err != nil {
		_ = qc.CloseWithError(closeErrorCode, "")
		return nil, fmt.Errorf("transport: peer fingerprint: %w", err)
	}
	return &conn{qc: qc, peerNodeID: nodeID}, nil
}

func (c *conn) PeerNodeID() string { return c.peerNodeID }

func (c *conn) OpenStream(ctx context.Context) (netio.Stream, error) {
	s, err := c.qc.OpenStreamSync(ctx)
	if err != nil {
		return nil, fmt.Errorf("transport: open stream: %w", err)
	}
	return s, nil
}

func (c *conn) AcceptStream(ctx context.Context) (netio.Stream, error) {
	s, err := c.qc.AcceptStream(ctx)
	if err != nil {
		return nil, fmt.Errorf("transport: accept stream: %w", err)
	}
	return s, nil
}

func (c *conn) Close() error {
	return c.qc.CloseWithError(closeErrorCode, "")
}
