package discovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"

	disco "github.com/GhentiLabs/Trove/pkg/discovery"
	"github.com/GhentiLabs/Trove/pkg/identity"
	"github.com/coder/websocket"
)

var (
	// ErrTargetUnavailable is returned by Connect when the target has no live
	// signaling connection (it is offline); the ladder cannot holepunch it now.
	ErrTargetUnavailable = errors.New("discovery: signal target unavailable")
	// ErrSignalerClosed is returned when the signaling connection has closed.
	ErrSignalerClosed = errors.New("discovery: signaler closed")
)

// incomingBuffer bounds undelivered inbound holepunch requests before they drop.
const incomingBuffer = 16

// Signaler is a live signaling WebSocket to the Trove server. It brokers holepunch
// coordination: Connect initiates toward a target and returns its candidates plus a
// synchronized punch time; Incoming delivers requests from peers punching toward us.
type Signaler struct {
	ws  *websocket.Conn
	ctx context.Context
	log *slog.Logger

	wmu sync.Mutex

	mu       sync.Mutex
	pending  map[string]chan signalResult
	incoming chan disco.IncomingRequest

	cancel    context.CancelFunc
	closeOnce sync.Once
	closed    chan struct{}
}

type signalResult struct {
	cands disco.PeerCandidates
	err   error
}

// Signal opens the signaling WebSocket and sends the opening Hello. The returned
// Signaler runs a read loop until Close. A separate no-timeout HTTP client is used
// because the shared client's request timeout would kill the long-lived socket.
func (c *Client) Signal(ctx context.Context) (*Signaler, error) {
	hc := &http.Client{
		Transport: &http.Transport{TLSClientConfig: identity.PinnedClientConfig(c.cert, c.pin)},
	}
	//nolint:bodyclose // coder/websocket nils and closes resp.Body before returning.
	ws, _, err := websocket.Dial(ctx, "wss://"+c.addr+"/v1/signal", &websocket.DialOptions{HTTPClient: hc})
	if err != nil {
		return nil, fmt.Errorf("discovery: signal dial: %w", err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	s := &Signaler{
		ws:       ws,
		ctx:      runCtx,
		log:      c.log,
		pending:  make(map[string]chan signalResult),
		incoming: make(chan disco.IncomingRequest, incomingBuffer),
		cancel:   cancel,
		closed:   make(chan struct{}),
	}
	if err := s.send(ctx, disco.SignalHello, disco.Hello{}); err != nil {
		cancel()
		_ = ws.Close(websocket.StatusInternalError, "hello failed")
		return nil, fmt.Errorf("discovery: signal hello: %w", err)
	}
	go s.readLoop()
	return s, nil
}

// Connect asks the server to broker a holepunch with target, advertising cands. It
// returns the target's candidates and the shared punch time, ErrTargetUnavailable
// if the target is offline, or ctx/closed errors.
func (s *Signaler) Connect(ctx context.Context, target string, cands []disco.Address) (disco.PeerCandidates, error) {
	ch := make(chan signalResult, 1)
	s.mu.Lock()
	s.pending[target] = ch
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.pending, target)
		s.mu.Unlock()
	}()

	if err := s.send(ctx, disco.SignalConnectRequest, disco.ConnectRequest{TargetNodeID: target, MyCandidates: cands}); err != nil {
		return disco.PeerCandidates{}, fmt.Errorf("discovery: connect request: %w", err)
	}
	select {
	case r := <-ch:
		return r.cands, r.err
	case <-ctx.Done():
		return disco.PeerCandidates{}, ctx.Err()
	case <-s.closed:
		return disco.PeerCandidates{}, ErrSignalerClosed
	}
}

// Incoming delivers holepunch requests from peers reaching toward this node.
func (s *Signaler) Incoming() <-chan disco.IncomingRequest { return s.incoming }

func (s *Signaler) readLoop() {
	defer func() { _ = s.Close() }()
	for {
		_, data, err := s.ws.Read(s.ctx)
		if err != nil {
			return
		}
		var msg disco.SignalMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			s.log.Warn("discovery: bad signal message", "err", err)
			continue
		}
		s.dispatch(msg)
	}
}

func (s *Signaler) dispatch(msg disco.SignalMessage) {
	switch msg.Type {
	case disco.SignalPeerCandidates:
		var pc disco.PeerCandidates
		if msg.Decode(&pc) == nil {
			s.deliver(pc.FromNodeID, signalResult{cands: pc})
		}
	case disco.SignalTargetUnavailable:
		var tu disco.TargetUnavailable
		if msg.Decode(&tu) == nil {
			s.deliver(tu.TargetNodeID, signalResult{err: fmt.Errorf("%w: %s", ErrTargetUnavailable, tu.TargetNodeID)})
		}
	case disco.SignalIncomingRequest:
		var ir disco.IncomingRequest
		if msg.Decode(&ir) == nil {
			select {
			case s.incoming <- ir:
			default:
				s.log.Warn("discovery: dropping incoming holepunch request, buffer full")
			}
		}
	case disco.SignalError:
		var e disco.SignalErrorPayload
		_ = msg.Decode(&e)
		s.log.Warn("discovery: signal error", "code", e.Code, "message", e.Message)
	}
}

func (s *Signaler) deliver(target string, r signalResult) {
	s.mu.Lock()
	ch, ok := s.pending[target]
	s.mu.Unlock()
	if ok {
		select {
		case ch <- r:
		default:
		}
	}
}

func (s *Signaler) send(ctx context.Context, typ disco.SignalType, payload any) error {
	msg, err := disco.NewSignalMessage(typ, payload)
	if err != nil {
		return err
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	s.wmu.Lock()
	defer s.wmu.Unlock()
	return s.ws.Write(ctx, websocket.MessageText, data)
}

// Close shuts the signaling connection down. It is idempotent.
func (s *Signaler) Close() error {
	s.closeOnce.Do(func() {
		close(s.closed)
		s.cancel()
		_ = s.ws.Close(websocket.StatusNormalClosure, "")
	})
	return nil
}
