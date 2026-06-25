// Package signaling brokers NAT hole-punch coordination over WebSocket. It
// matches a requester to a target's live connection and forwards small control
// messages between them, stamped with a near-future "punch" time so both sides
// open simultaneously. It never carries peer payload data and holds nothing
// durable.
package signaling

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/GhentiLabs/Trove/pkg/discovery"
)

// ErrAtCapacity is returned by Serve when the concurrent-connection cap is
// reached. The caller should reject the upgrade.
var ErrAtCapacity = errors.New("signaling: at connection capacity")

// wsConn is the subset of a WebSocket connection the broker uses. It keeps the
// broker testable without a real socket and the transport swappable.
type wsConn interface {
	Read(ctx context.Context) (data []byte, err error)
	Write(ctx context.Context, data []byte) error
	Ping(ctx context.Context) error
	Close(reason string) error
}

// Metrics receives signaling events. Both hooks are optional.
type Metrics struct {
	OnMatch       func(success bool)
	OnActiveDelta func(delta int)
}

func (m Metrics) match(ok bool) {
	if m.OnMatch != nil {
		m.OnMatch(ok)
	}
}

func (m Metrics) active(delta int) {
	if m.OnActiveDelta != nil {
		m.OnActiveDelta(delta)
	}
}

// Options configure a Broker.
type Options struct {
	MaxConns     int
	SendBuffer   int
	PunchOffset  time.Duration
	PingInterval time.Duration
	WriteTimeout time.Duration
	RatePerSec   float64
	RateBurst    int
	Clock        func() time.Time
	// Resolve returns a node's last-announced addresses, used to tell a
	// requester where to reach the target. Required.
	Resolve func(nodeID string) ([]discovery.Address, bool)
	Metrics Metrics
	// Logger receives signaling events; nil discards them.
	Logger *slog.Logger
}

// Broker tracks live signaling connections and routes control messages.
type Broker struct {
	conns   *connSet
	opts    Options
	clock   func() time.Time
	metrics Metrics
	log     *slog.Logger
}

// New constructs a Broker.
func New(opts Options) *Broker {
	if opts.Clock == nil {
		opts.Clock = time.Now
	}
	if opts.SendBuffer <= 0 {
		opts.SendBuffer = 16
	}
	log := opts.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Broker{
		conns:   newConnSet(),
		opts:    opts,
		clock:   opts.Clock,
		metrics: opts.Metrics,
		log:     log,
	}
}

// Active reports the number of live connections.
func (b *Broker) Active() int { return b.conns.len() }

// Serve registers an authenticated connection and runs its read/write pumps
// until it closes. nodeID must already be derived from the peer's mTLS
// certificate. It returns ErrAtCapacity if the connection cannot be admitted.
func (b *Broker) Serve(ctx context.Context, ws wsConn, nodeID string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	c := newConn(nodeID, ws, cancel, b.opts.SendBuffer)

	old, admitted := b.conns.addIfRoom(c, b.opts.MaxConns)
	if !admitted {
		_ = ws.Close("server at capacity")
		return ErrAtCapacity
	}
	defer func() { _ = ws.Close("connection closed") }()
	if old != nil {
		old.close()
		b.metrics.active(-1)
		b.log.Debug("signaling: replaced stale connection", "node", nodeID)
	}
	b.metrics.active(1)
	b.log.Info("signaling: connected", "node", nodeID, "active", b.conns.len())
	defer func() {
		if b.conns.remove(c) {
			b.metrics.active(-1)
		}
		b.log.Debug("signaling: disconnected", "node", nodeID)
	}()

	go c.writePump(ctx, b.opts.PingInterval, b.opts.WriteTimeout)
	return c.readPump(ctx, b)
}

func (b *Broker) handle(c *conn, msg discovery.SignalMessage) {
	switch msg.Type {
	case discovery.SignalConnectRequest:
		var req discovery.ConnectRequest
		if err := msg.Decode(&req); err != nil {
			c.sendError("bad_request", "malformed connect_request")
			return
		}
		b.route(c, req)
	default:
		c.sendError("unexpected_type", "unsupported message type")
	}
}

// route matches a connect_request to the target's live connection.
func (b *Broker) route(from *conn, req discovery.ConnectRequest) {
	for _, a := range req.MyCandidates {
		if a.Validate() != nil {
			from.sendError("bad_request", "invalid candidate address")
			return
		}
	}

	target, ok := b.conns.get(req.TargetNodeID)
	if !ok || target == from {
		b.log.Debug("signaling: target unavailable", "from", from.nodeID, "target", req.TargetNodeID)
		b.notifyUnavailable(from, req.TargetNodeID)
		return
	}

	punchAt := b.clock().Add(b.opts.PunchOffset).UnixMilli()

	deliveredToTarget := target.send(discovery.SignalIncomingRequest, discovery.IncomingRequest{
		FromNodeID:    from.nodeID,
		Candidates:    req.MyCandidates,
		PunchAtMillis: punchAt,
	})
	if !deliveredToTarget {
		target.close()
		b.notifyUnavailable(from, req.TargetNodeID)
		return
	}

	var targetAddrs []discovery.Address
	if b.opts.Resolve != nil {
		targetAddrs, _ = b.opts.Resolve(req.TargetNodeID)
	}
	if !from.send(discovery.SignalPeerCandidates, discovery.PeerCandidates{
		FromNodeID:    target.nodeID,
		Candidates:    targetAddrs,
		PunchAtMillis: punchAt,
	}) {
		from.close()
		return
	}
	b.metrics.match(true)
	b.log.Info("signaling: holepunch brokered", "from", from.nodeID, "target", req.TargetNodeID,
		"candidates", len(req.MyCandidates), "punch_at_ms", punchAt)
}

func (b *Broker) notifyUnavailable(from *conn, targetNodeID string) {
	b.metrics.match(false)
	if !from.send(discovery.SignalTargetUnavailable, discovery.TargetUnavailable{TargetNodeID: targetNodeID}) {
		from.close()
	}
}
