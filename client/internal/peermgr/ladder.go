package peermgr

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/discovery"
	"github.com/GhentiLabs/Trove/client/internal/netio"
	disco "github.com/GhentiLabs/Trove/pkg/discovery"
)

// ErrUnreachable is returned when every reachability tier fails. There is no relay
// fallback; the peer simply cannot be reached right now.
var ErrUnreachable = errors.New("peermgr: peer unreachable")

// errAwaitInbound signals that this node is the holepunch acceptor: it has fired its
// NAT-opening probe and the connection will arrive via the accept loop, so the dial
// loop should back off rather than dial.
var errAwaitInbound = errors.New("peermgr: awaiting inbound holepunch")

// maxPunchDelay bounds how far in the future a server-supplied punch time may be, so
// an untrusted signaler cannot park the dial in a long sleep.
const maxPunchDelay = 30 * time.Second

// LadderConfig wires the reachability ladder from discovery and transport. The
// network-touching operations are injected so the tier logic — including the
// holepunch role decision — is testable; only real NAT traversal is left to the
// live integration gate.
type LadderConfig struct {
	// Self is this node's id, deciding the holepunch dial/accept role.
	Self string
	// Cache holds each peer's last working address.
	Cache *discovery.Cache
	// Dial opens an authenticated connection to addr, pinned to nodeID.
	Dial func(ctx context.Context, addr, nodeID string) (netio.Conn, error)
	// Probe fires NAT-opening datagrams at addrs on the shared socket.
	Probe func(ctx context.Context, addrs []string) error
	// Lookup resolves a peer's candidate addresses via Trove.
	Lookup func(ctx context.Context, nodeID string) ([]string, error)
	// Signal brokers a holepunch, returning the peer's candidates and punch time.
	Signal func(ctx context.Context, nodeID string, cands []disco.Address) (disco.PeerCandidates, error)
	// Candidates returns this node's current advertised candidates.
	Candidates func() []disco.Address
	// Logger receives ladder events; nil discards them.
	Logger *slog.Logger
}

// Ladder resolves a node id to a live connection, cheapest path first: a cached or
// mDNS-populated address, then a Trove lookup, then a holepunch.
type Ladder struct {
	cfg LadderConfig
	log *slog.Logger
}

// NewLadder builds a Ladder. Its Connect method is the Manager's Connect.
func NewLadder(cfg LadderConfig) *Ladder {
	log := cfg.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Ladder{cfg: cfg, log: log}
}

// Connect runs the reachability ladder for nodeID.
func (l *Ladder) Connect(ctx context.Context, nodeID string) (netio.Conn, error) {
	if addr, ok := l.cfg.Cache.Get(nodeID); ok {
		if c, err := l.cfg.Dial(ctx, addr, nodeID); err == nil {
			return c, nil
		}
		l.cfg.Cache.Remove(nodeID)
	}

	if l.cfg.Lookup != nil {
		if addrs, err := l.cfg.Lookup(ctx, nodeID); err == nil {
			if c := l.dialAny(ctx, nodeID, addrs); c != nil {
				return c, nil
			}
		} else {
			l.log.Debug("peermgr: lookup failed", "peer", nodeID, "err", err)
		}
	}

	return l.holepunch(ctx, nodeID)
}

func (l *Ladder) dialAny(ctx context.Context, nodeID string, addrs []string) netio.Conn {
	for _, a := range addrs {
		if c, err := l.cfg.Dial(ctx, a, nodeID); err == nil {
			l.cfg.Cache.Put(nodeID, a)
			return c
		}
	}
	return nil
}

func (l *Ladder) holepunch(ctx context.Context, nodeID string) (netio.Conn, error) {
	if l.cfg.Signal == nil {
		return nil, ErrUnreachable
	}
	pc, err := l.cfg.Signal(ctx, nodeID, l.cfg.Candidates())
	if err != nil {
		return nil, fmt.Errorf("peermgr: holepunch signal: %w", err)
	}

	d := time.Until(time.UnixMilli(pc.PunchAtMillis))
	if d > maxPunchDelay {
		return nil, fmt.Errorf("peermgr: implausible punch time %dms ahead", d.Milliseconds())
	}
	if d > 0 && !sleep(ctx, d) {
		return nil, ctx.Err()
	}

	addrs := addrStrings(pc.Candidates)
	if err := l.cfg.Probe(ctx, addrs); err != nil {
		l.log.Debug("peermgr: probe failed", "peer", nodeID, "err", err)
	}

	// Deterministic role: the lexicographically smaller node id dials; the other
	// has opened its NAT mapping with the probe and accepts the inbound connection.
	if l.cfg.Self < nodeID {
		if c := l.dialAny(ctx, nodeID, addrs); c != nil {
			return c, nil
		}
		return nil, ErrUnreachable
	}
	return nil, errAwaitInbound
}

func addrStrings(addrs []disco.Address) []string {
	out := make([]string, 0, len(addrs))
	for _, a := range addrs {
		out = append(out, a.String())
	}
	return out
}
