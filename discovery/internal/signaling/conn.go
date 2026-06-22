package signaling

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/GhentiLabs/Trove/pkg/discovery"
)

// conn is one live signaling connection with a bounded outbound queue.
type conn struct {
	nodeID string
	ws     wsConn
	cancel context.CancelFunc

	out       chan discovery.SignalMessage
	closeOnce sync.Once
}

func newConn(nodeID string, ws wsConn, cancel context.CancelFunc, buffer int) *conn {
	return &conn{
		nodeID: nodeID,
		ws:     ws,
		cancel: cancel,
		out:    make(chan discovery.SignalMessage, buffer),
	}
}

// send enqueues a typed message. It is non-blocking: a full queue means a slow
// consumer, reported back so the caller can reap the connection rather than
// grow memory unbounded.
func (c *conn) send(typ discovery.SignalType, payload any) bool {
	msg, err := discovery.NewSignalMessage(typ, payload)
	if err != nil {
		return false
	}
	select {
	case c.out <- msg:
		return true
	default:
		return false
	}
}

func (c *conn) sendError(code, message string) {
	c.send(discovery.SignalError, discovery.SignalErrorPayload{Code: code, Message: message})
}

// close stops the connection; pumps unblock via the cancelled context.
func (c *conn) close() {
	c.closeOnce.Do(func() { c.cancel() })
}

var errRateExceeded = errors.New("signaling: message rate exceeded")

func (c *conn) readPump(ctx context.Context, b *Broker) error {
	limiter := rate.NewLimiter(rate.Limit(b.opts.RatePerSec), b.opts.RateBurst)
	for {
		data, err := c.ws.Read(ctx)
		if err != nil {
			return err
		}
		if !limiter.Allow() {
			return errRateExceeded
		}
		var msg discovery.SignalMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			c.sendError("bad_request", "malformed message")
			continue
		}
		b.handle(c, msg)
	}
}

func (c *conn) writePump(ctx context.Context, pingInterval, writeTimeout time.Duration) {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()
	defer c.close()

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-c.out:
			data, err := json.Marshal(msg)
			if err != nil {
				continue
			}
			wctx, cancel := context.WithTimeout(ctx, writeTimeout)
			err = c.ws.Write(wctx, data)
			cancel()
			if err != nil {
				return
			}
		case <-ticker.C:
			pctx, cancel := context.WithTimeout(ctx, writeTimeout)
			err := c.ws.Ping(pctx)
			cancel()
			if err != nil {
				return
			}
		}
	}
}

// connSet is a concurrency-safe node_id -> conn map.
type connSet struct {
	mu sync.RWMutex
	m  map[string]*conn
}

func newConnSet() *connSet { return &connSet{m: make(map[string]*conn)} }

func (s *connSet) addIfRoom(c *conn, max int) (*conn, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, exists := s.m[c.nodeID]
	if !exists && len(s.m) >= max {
		return nil, false
	}
	s.m[c.nodeID] = c
	return old, true
}

// remove deletes c only if it is still the registered connection for its node,
// avoiding a race where a replacement was already installed. It reports whether
// a deletion occurred.
func (s *connSet) remove(c *conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m[c.nodeID] == c {
		delete(s.m, c.nodeID)
		return true
	}
	return false
}

func (s *connSet) get(nodeID string) (*conn, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.m[nodeID]
	return c, ok
}

func (s *connSet) len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.m)
}
