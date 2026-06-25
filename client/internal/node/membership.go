package node

import (
	"context"
	"log/slog"
	"sync"

	"github.com/GhentiLabs/Trove/client/internal/membership"
	"github.com/GhentiLabs/Trove/client/internal/session"
	"github.com/GhentiLabs/Trove/client/internal/wire/wirepb"
)

// gossiper propagates network rosters across sessions: it pushes the local roster to
// each new peer, merges inbound gossip (verifying every entry), and re-broadcasts
// newly admitted entries to the other peers (introducer propagation).
type gossiper struct {
	store *membership.Store
	log   *slog.Logger

	mu    sync.Mutex
	peers map[string]*session.Session
}

func newGossiper(store *membership.Store, log *slog.Logger) *gossiper {
	return &gossiper{store: store, log: log, peers: make(map[string]*session.Session)}
}

// addPeer registers a session and pushes the local rosters to it.
func (g *gossiper) addPeer(ctx context.Context, s *session.Session) {
	g.mu.Lock()
	g.peers[s.PeerNodeID()] = s
	g.mu.Unlock()
	g.sendRosters(ctx, s)
}

func (g *gossiper) removePeer(peerID string, s *session.Session) {
	g.mu.Lock()
	if g.peers[peerID] == s {
		delete(g.peers, peerID)
	}
	g.mu.Unlock()
}

// handle merges an inbound gossip message from fromPeer and re-broadcasts what it
// newly admitted to the other peers.
func (g *gossiper) handle(ctx context.Context, fromPeer string, gm *wirepb.MembershipGossip) error {
	added, err := g.store.Merge(ctx, gm.GetNetworkId(), fromWireEntries(gm.GetEntries()))
	if err != nil {
		return err
	}
	if len(added) > 0 {
		g.broadcast(gm.GetNetworkId(), added, fromPeer)
	}
	return nil
}

func (g *gossiper) sendRosters(ctx context.Context, to *session.Session) {
	nets, err := g.store.Networks(ctx)
	if err != nil {
		g.log.Warn("node: list networks", "err", err)
		return
	}
	for _, net := range nets {
		roster, err := g.store.Roster(ctx, net)
		if err != nil {
			g.log.Warn("node: load roster", "network", net, "err", err)
			continue
		}
		if len(roster) == 0 {
			continue
		}
		if err := to.Send(toWireGossip(net, roster)); err != nil {
			g.log.Debug("node: send roster", "peer", to.PeerNodeID(), "err", err)
		}
	}
}

func (g *gossiper) broadcast(networkID string, entries []membership.Entry, exceptPeer string) {
	msg := toWireGossip(networkID, entries)
	g.mu.Lock()
	targets := make([]*session.Session, 0, len(g.peers))
	for id, s := range g.peers {
		if id != exceptPeer {
			targets = append(targets, s)
		}
	}
	g.mu.Unlock()
	for _, s := range targets {
		if err := s.Send(msg); err != nil {
			g.log.Debug("node: broadcast roster", "peer", s.PeerNodeID(), "err", err)
		}
	}
}

func toWireGossip(networkID string, entries []membership.Entry) *wirepb.MembershipGossip {
	out := make([]*wirepb.MembershipEntry, 0, len(entries))
	for _, e := range entries {
		out = append(out, &wirepb.MembershipEntry{
			NetworkId: e.NetworkID, NodeId: e.NodeID, PublicKey: e.PublicKey,
			Role: uint32(e.Role), AddedBy: e.AddedBy, AddedAtMs: e.AddedAtMs, Sig: e.Sig,
		})
	}
	return &wirepb.MembershipGossip{NetworkId: networkID, Entries: out}
}

func fromWireEntries(in []*wirepb.MembershipEntry) []membership.Entry {
	out := make([]membership.Entry, 0, len(in))
	for _, e := range in {
		out = append(out, membership.Entry{
			NetworkID: e.GetNetworkId(), NodeID: e.GetNodeId(), PublicKey: e.GetPublicKey(),
			Role: membership.Role(e.GetRole()), AddedBy: e.GetAddedBy(), AddedAtMs: e.GetAddedAtMs(), Sig: e.GetSig(),
		})
	}
	return out
}
