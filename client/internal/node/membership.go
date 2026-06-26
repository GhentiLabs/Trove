package node

import (
	"context"
	"log/slog"
	"math"
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

// maxGossipEntries bounds the roster a single message may carry, capping the
// signature-verification work a peer can force under the store's write transaction.
const maxGossipEntries = 4096

// handle merges an inbound gossip message from fromPeer and re-broadcasts what it
// newly admitted to the other peers.
func (g *gossiper) handle(ctx context.Context, fromPeer string, gm *wirepb.MembershipGossip) error {
	entries := gm.GetEntries()
	if len(entries) > maxGossipEntries {
		g.log.Warn("node: gossip roster over cap, dropping", "peer", fromPeer, "group", gm.GetNetworkId(), "count", len(entries), "cap", maxGossipEntries)
		return nil
	}
	added, err := g.store.Merge(ctx, gm.GetNetworkId(), fromWireEntries(entries))
	if err != nil {
		// A local roster-store failure is not a protocol violation; log it and let the
		// anti-entropy tick retry rather than tear the session down.
		g.log.Warn("node: gossip merge failed", "peer", fromPeer, "group", gm.GetNetworkId(), "err", err)
		return nil
	}
	if len(added) > 0 {
		g.broadcast(gm.GetNetworkId(), added, fromPeer)
	}
	return nil
}

// resync re-pushes the local rosters to every connected peer; the node calls it on a
// timer for anti-entropy so a roster that missed a peer eventually reaches it.
func (g *gossiper) resync(ctx context.Context) {
	g.mu.Lock()
	peers := make([]*session.Session, 0, len(g.peers))
	for _, s := range g.peers {
		peers = append(peers, s)
	}
	g.mu.Unlock()
	for _, s := range peers {
		g.sendRosters(ctx, s)
	}
}

func (g *gossiper) sendRosters(ctx context.Context, to *session.Session) {
	groups, err := g.store.Groups(ctx)
	if err != nil {
		g.log.Warn("node: list groups", "err", err)
		return
	}
	for _, group := range groups {
		roster, err := g.store.Roster(ctx, group)
		if err != nil {
			g.log.Warn("node: load roster", "group", group, "err", err)
			continue
		}
		if len(roster) == 0 {
			continue
		}
		if err := to.Send(toWireGossip(group, roster)); err != nil {
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
		if e.GetRole() > math.MaxUint8 {
			continue // out of Role's range; Merge would reject it anyway
		}
		out = append(out, membership.Entry{
			NetworkID: e.GetNetworkId(), NodeID: e.GetNodeId(), PublicKey: e.GetPublicKey(),
			Role: membership.Role(e.GetRole()), AddedBy: e.GetAddedBy(), AddedAtMs: e.GetAddedAtMs(), Sig: e.GetSig(),
		})
	}
	return out
}
