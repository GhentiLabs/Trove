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

// ErrUnreachable is returned when every reachability tier fails.
var ErrUnreachable = errors.New("peermgr: peer unreachable")

var errAwaitInbound = errors.New("peermgr: awaiting inbound holepunch")

var errPunchMissed = errors.New("peermgr: holepunch missed")

// MaxPunchDelay bounds a server-supplied punch time.
const MaxPunchDelay = 30 * time.Second

const (
	signalTimeout    = 15 * time.Second
	dialTimeout      = 5 * time.Second
	maxPunchAttempts = 4
)

// LadderConfig wires the reachability ladder from discovery and transport.
type LadderConfig struct {
	Self       string
	Cache      *discovery.Cache
	Dial       func(ctx context.Context, addr, nodeID string) (netio.Conn, error)
	Probe      func(ctx context.Context, addrs []string) error
	Lookup     func(ctx context.Context, nodeID string) ([]string, error)
	Signal     func(ctx context.Context, nodeID string, cands []disco.Address) (disco.PeerCandidates, error)
	Candidates func() []disco.Address
	Logger     *slog.Logger
}

// Ladder resolves a node id to a live connection, cheapest path first.
type Ladder struct {
	cfg LadderConfig
	log *slog.Logger
}

// NewLadder builds a Ladder.
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
			l.log.Info("peermgr: connected", "peer", nodeID, "tier", "cache", "addr", addr)
			return c, nil
		}
		l.cfg.Cache.Remove(nodeID)
	}

	if l.cfg.Lookup != nil {
		if addrs, err := l.cfg.Lookup(ctx, nodeID); err == nil {
			l.log.Debug("peermgr: lookup ok", "peer", nodeID, "candidates", len(addrs))
			if c := l.dialAny(ctx, nodeID, addrs, "lookup"); c != nil {
				return c, nil
			}
		} else {
			l.log.Debug("peermgr: lookup failed", "peer", nodeID, "err", err)
		}
	}

	return l.holepunch(ctx, nodeID)
}

func (l *Ladder) dialAny(ctx context.Context, nodeID string, addrs []string, tier string) netio.Conn {
	for _, a := range addrs {
		dctx, cancel := context.WithTimeout(ctx, dialTimeout)
		c, err := l.cfg.Dial(dctx, a, nodeID)
		cancel()
		if err != nil {
			l.log.Debug("peermgr: dial failed", "peer", nodeID, "tier", tier, "addr", a, "err", err)
			continue
		}
		l.cfg.Cache.Put(nodeID, a)
		l.log.Info("peermgr: connected", "peer", nodeID, "tier", tier, "addr", a)
		return c
	}
	return nil
}

func (l *Ladder) holepunch(ctx context.Context, nodeID string) (netio.Conn, error) {
	if l.cfg.Signal == nil {
		return nil, ErrUnreachable
	}
	if l.cfg.Self >= nodeID {
		l.log.Info("peermgr: holepunch", "peer", nodeID, "role", "accept")
		_, _ = l.punchRound(ctx, nodeID, false)
		return nil, errAwaitInbound
	}

	l.log.Info("peermgr: holepunch", "peer", nodeID, "role", "dial")
	for attempt := range maxPunchAttempts {
		if c, err := l.punchRound(ctx, nodeID, true); c != nil {
			return c, nil
		} else if err != nil {
			l.log.Debug("peermgr: punch round failed", "peer", nodeID, "attempt", attempt, "err", err)
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if attempt < maxPunchAttempts-1 && !sleep(ctx, holepunchRetry) {
			return nil, ctx.Err()
		}
	}
	return nil, errPunchMissed
}

func (l *Ladder) punchRound(ctx context.Context, nodeID string, dial bool) (netio.Conn, error) {
	sctx, cancel := context.WithTimeout(ctx, signalTimeout)
	pc, err := l.cfg.Signal(sctx, nodeID, l.cfg.Candidates())
	cancel()
	if err != nil {
		return nil, fmt.Errorf("peermgr: holepunch signal: %w", err)
	}

	d := time.Until(time.UnixMilli(pc.PunchAtMillis))
	if d > MaxPunchDelay {
		return nil, fmt.Errorf("peermgr: implausible punch time %dms ahead", d.Milliseconds())
	}
	if d > 0 && !sleep(ctx, d) {
		return nil, ctx.Err()
	}

	addrs := routableAddrs(pc.Candidates)
	if dial {
		return l.dialAny(ctx, nodeID, addrs, "holepunch"), nil
	}
	if err := l.cfg.Probe(ctx, addrs); err != nil {
		l.log.Debug("peermgr: probe failed", "peer", nodeID, "err", err)
	}
	return nil, nil
}

func routableAddrs(addrs []disco.Address) []string {
	out := make([]string, 0, len(addrs))
	for _, a := range addrs {
		if a.Routable() {
			out = append(out, a.String())
		}
	}
	return out
}
