package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/coder/websocket"

	"github.com/GhentiLabs/Trove/discovery/internal/signaling"
	"github.com/GhentiLabs/Trove/pkg/discovery"
	"github.com/GhentiLabs/Trove/pkg/identity"
)

var errBadHello = errors.New("signal: first message must be a hello")

const helloTimeout = 5 * time.Second

func (s *Server) handleSignal(w http.ResponseWriter, r *http.Request) {
	nodeID, err := identity.PeerFingerprint(r.TLS)
	if err != nil {
		writeError(w, s.log, http.StatusUnauthorized, codeUnauthorized, "unauthorized", err)
		return
	}
	if s.denylist.BlockedNode(nodeID) {
		writeError(w, s.log, http.StatusForbidden, codeForbidden, "forbidden", nil)
		return
	}
	if !s.signalLim.allow("node:" + nodeID) {
		writeError(w, s.log, http.StatusTooManyRequests, codeRateLimited, "rate limit exceeded", nil)
		return
	}

	c, err := websocket.Accept(w, r, s.wsAccept)
	if err != nil {
		s.log.Warn("signal accept failed", "request_id", requestID(r.Context()), "detail", err.Error())
		return
	}
	c.SetReadLimit(s.cfg.MaxSignalMsgBytes)

	if err := readHello(r.Context(), c, helloTimeout); err != nil {
		_ = c.Close(websocket.StatusPolicyViolation, "hello required")
		return
	}

	if err := s.broker.Serve(r.Context(), &wsAdapter{c: c}, nodeID); err != nil {
		if errors.Is(err, signaling.ErrAtCapacity) {
			s.log.Warn("signal rejected at capacity", "node_id", nodeID)
		}
	}
}

func readHello(ctx context.Context, c *websocket.Conn, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	_, data, err := c.Read(ctx)
	if err != nil {
		return err
	}
	var msg discovery.SignalMessage
	if err := json.Unmarshal(data, &msg); err != nil || msg.Type != discovery.SignalHello {
		return errBadHello
	}
	return nil
}

// wsAdapter adapts a coder/websocket connection to the signaling.wsConn the
// broker consumes, hiding the message-type detail (all frames are text JSON).
type wsAdapter struct{ c *websocket.Conn }

func (a *wsAdapter) Read(ctx context.Context) ([]byte, error) {
	_, data, err := a.c.Read(ctx)
	return data, err
}

func (a *wsAdapter) Write(ctx context.Context, data []byte) error {
	return a.c.Write(ctx, websocket.MessageText, data)
}

func (a *wsAdapter) Ping(ctx context.Context) error { return a.c.Ping(ctx) }

func (a *wsAdapter) Close(reason string) error {
	return a.c.Close(websocket.StatusNormalClosure, reason)
}
