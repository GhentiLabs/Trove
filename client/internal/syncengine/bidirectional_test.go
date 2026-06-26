package syncengine

import (
	"context"
	"strings"
	"testing"
	"time"
)

// waitSameRoot blocks until every peer reports the same current root, the convergence
// signal for a multi-writer folder.
func waitSameRoot(t *testing.T, peers ...peer) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		want := peers[0].currentRoot(t)
		same := true
		for _, p := range peers[1:] {
			if p.currentRoot(t) != want {
				same = false
				break
			}
		}
		if same {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	roots := make([]string, len(peers))
	for i, p := range peers {
		roots[i] = p.currentRoot(t)
	}
	t.Fatalf("peers did not converge to one root: %v", roots)
}

// TestBidirectionalNonOverlappingEdits proves two writers that each originate a distinct
// path converge to the union, each pulling and applying the other's edit.
func TestBidirectionalNonOverlappingEdits(t *testing.T) {
	a := newPeer(t, ownerID)
	b := newPeer(t, replicaID)
	writeFile(t, a.root, "from-a.txt", []byte("authored by a"))
	writeFile(t, b.root, "from-b.txt", []byte("authored by b"))
	a.scan(t)
	b.scan(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sa, sb := memSessionPair(t, ctx, a, b)
	engineOn(t, ctx, sa, a, RoleWriter, nil)
	engineOn(t, ctx, sb, b, RoleWriter, nil)

	waitSameRoot(t, a, b)
	assertTreesEqual(t, a.root, b.root)
	assertLeafSetsEqual(t, a, b)
}

// TestTransitiveRelayConverges proves a writer's edit reaches a peer it never connects to
// directly: A links only to B, C links only to B, and B relays A's manifest to C.
func TestTransitiveRelayConverges(t *testing.T) {
	a := newPeer(t, ownerID)
	b := newPeer(t, strings.Repeat("b", 52))
	c := newPeer(t, replicaID)
	writeFile(t, a.root, "shared.txt", []byte("originated on a, must reach c via b"))
	a.scan(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ca := NewCoordinator(folderID, a.fc, a.chunks, 0, nil)
	cb := NewCoordinator(folderID, b.fc, b.chunks, 0, nil)
	cc := NewCoordinator(folderID, c.fc, c.chunks, 0, nil)

	abA, abB := memSessionPair(t, ctx, a, b)
	engineOn(t, ctx, abA, a, RoleWriter, ca)
	engineOn(t, ctx, abB, b, RoleReader, cb)

	bcB, bcC := memSessionPair(t, ctx, b, c)
	engineOn(t, ctx, bcB, b, RoleReader, cb)
	engineOn(t, ctx, bcC, c, RoleReader, cc)

	waitSameRoot(t, a, b, c)
	assertTreesEqual(t, a.root, c.root)
	assertLeafSetsEqual(t, a, c)
}
