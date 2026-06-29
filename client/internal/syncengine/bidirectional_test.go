package syncengine

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// waitSameRoot blocks until every peer reports the same current root.
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

// fileContents returns the set of regular-file contents under root, ignoring directories.
func fileContents(t *testing.T, root string) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	for _, e := range walk(t, root) {
		if !e.dir && e.link == "" {
			out[string(e.data)] = true
		}
	}
	return out
}

// TestConcurrentSamePathKeepsBoth proves two writers that edit the same path while apart
// converge to byte-identical trees holding the deterministic winner at the path and the
// loser preserved as a conflict copy — neither edit is lost.
func TestConcurrentSamePathKeepsBoth(t *testing.T) {
	a := newPeer(t, ownerID)
	b := newPeer(t, replicaID)
	writeFile(t, a.root, "doc.txt", []byte("version from A"))
	writeFile(t, b.root, "doc.txt", []byte("version from B"))
	a.scan(t)
	b.scan(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sa, sb := memSessionPair(t, ctx, a, b)
	engineOn(t, ctx, sa, a, RoleWriter, nil)
	engineOn(t, ctx, sb, b, RoleWriter, nil)

	waitSameRoot(t, a, b)
	assertTreesEqual(t, a.root, b.root)

	got := fileContents(t, a.root)
	want := map[string]bool{"version from A": true, "version from B": true}
	if len(got) != 2 || !got["version from A"] || !got["version from B"] {
		t.Fatalf("both versions must survive keep-both: got %v, want %v", got, want)
	}
}

// TestConcurrentSamePathReaderConverges adds a read-only peer to the concurrent-edit case:
// the reader originates nothing yet must converge to the identical resolved tree.
func TestConcurrentSamePathReaderConverges(t *testing.T) {
	a := newPeer(t, ownerID)
	b := newPeer(t, strings.Repeat("b", 52))
	c := newPeer(t, replicaID)
	writeFile(t, a.root, "doc.txt", []byte("version from A"))
	writeFile(t, b.root, "doc.txt", []byte("version from B"))
	a.scan(t)
	b.scan(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ca := NewCoordinator(folderID, a.fc, a.chunks, 0, nil)
	cb := NewCoordinator(folderID, b.fc, b.chunks, 0, nil)
	cc := NewCoordinator(folderID, c.fc, c.chunks, 0, nil)

	abA, abB := memSessionPair(t, ctx, a, b)
	engineOn(t, ctx, abA, a, RoleWriter, ca)
	engineOn(t, ctx, abB, b, RoleWriter, cb)

	acA, acC := memSessionPair(t, ctx, a, c)
	engineOn(t, ctx, acA, a, RoleWriter, ca)
	engineOn(t, ctx, acC, c, RoleReader, cc)

	bcB, bcC := memSessionPair(t, ctx, b, c)
	engineOn(t, ctx, bcB, b, RoleWriter, cb)
	engineOn(t, ctx, bcC, c, RoleReader, cc)

	waitSameRoot(t, a, b, c)
	assertTreesEqual(t, a.root, c.root)
}

// TestPushOnChangePropagatesBeforeTicker proves a mid-session edit reaches the peer via
// the change hook, not the 5s anti-entropy ticker: it must arrive well within one second.
func TestPushOnChangePropagatesBeforeTicker(t *testing.T) {
	a := newPeer(t, ownerID)
	b := newPeer(t, replicaID)
	writeFile(t, a.root, "seed.txt", []byte("initial"))
	a.scan(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sa, sb := memSessionPair(t, ctx, a, b)
	engineOn(t, ctx, sa, a, RoleWriter, nil)
	engineOn(t, ctx, sb, b, RoleWriter, nil)
	waitSameRoot(t, a, b)

	// Edit after both engines are running and settled; only push (not the ticker) can
	// deliver this within the deadline.
	writeFile(t, a.root, "live.txt", []byte("appeared mid-session"))
	a.scan(t)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(filepath.Join(b.root, "live.txt")); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("mid-session edit did not propagate within 1s; push-on-change may have regressed")
}

// TestConcurrentDeleteVsEditKeepsEdit proves that when one writer deletes a path while
// another edits it during the same offline window, both converge to the surviving edit —
// data is never lost to a concurrent delete.
func TestConcurrentDeleteVsEditKeepsEdit(t *testing.T) {
	a := newPeer(t, ownerID)
	b := newPeer(t, replicaID)
	writeFile(t, a.root, "shared.txt", []byte("base"))
	a.scan(t)

	ctx1, cancel1 := context.WithCancel(context.Background())
	sa, sb := memSessionPair(t, ctx1, a, b)
	engineOn(t, ctx1, sa, a, RoleWriter, nil)
	engineOn(t, ctx1, sb, b, RoleWriter, nil)
	waitSameRoot(t, a, b)
	cancel1()

	// Offline: A deletes the file, B edits it.
	if err := os.Remove(filepath.Join(a.root, "shared.txt")); err != nil {
		t.Fatalf("remove: %v", err)
	}
	a.scan(t)
	writeFile(t, b.root, "shared.txt", []byte("edited by B while A deleted"))
	b.scan(t)

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	sa2, sb2 := memSessionPair(t, ctx2, a, b)
	engineOn(t, ctx2, sa2, a, RoleWriter, nil)
	engineOn(t, ctx2, sb2, b, RoleWriter, nil)

	waitSameRoot(t, a, b)
	assertTreesEqual(t, a.root, b.root)
	got := fileContents(t, a.root)
	if len(got) != 1 || !got["edited by B while A deleted"] {
		t.Fatalf("edit must survive a concurrent delete: %v", got)
	}
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
