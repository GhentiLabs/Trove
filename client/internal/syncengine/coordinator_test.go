package syncengine

import (
	"context"
	"errors"
	"testing"

	"github.com/GhentiLabs/Trove/client/internal/chunkstore"
	"github.com/GhentiLabs/Trove/client/internal/hasher"
	"github.com/GhentiLabs/Trove/client/internal/manifest"
	"github.com/GhentiLabs/Trove/client/internal/netio"
)

type fakeConn struct{ id string }

func (f *fakeConn) OpenStream(context.Context) (netio.Stream, error)   { return nil, errors.New("no") }
func (f *fakeConn) AcceptStream(context.Context) (netio.Stream, error) { return nil, errors.New("no") }
func (f *fakeConn) PeerNodeID() string                                 { return f.id }
func (f *fakeConn) Close() error                                       { return nil }

// A torn-down session must not evict a newer session that already replaced it for the
// same peer.
func TestCoordinatorRemoveSourceIdentityGuard(t *testing.T) {
	c := NewCoordinator(folderID, chunkstore.FolderContext{}, nil, 0, nil)
	conn1, conn2 := &fakeConn{"a"}, &fakeConn{"a"}
	c.addSource("a", conn1)
	c.addSource("a", conn2) // reconnect replaces conn1
	c.removeSource("a", conn1)
	if c.sourceCount() != 1 {
		t.Fatalf("sourceCount = %d, want 1 (the newer conn must survive)", c.sourceCount())
	}
}

// order() puts the owner last (the guaranteed fallback) and rotates the peer sources so
// load spreads instead of always hitting the same one first.
func TestCoordinatorOrderOwnerLastAndRotates(t *testing.T) {
	c := NewCoordinator(folderID, chunkstore.FolderContext{}, nil, 0, nil)
	for _, id := range []string{"owner", "p1", "p2", "p3"} {
		c.addSource(id, &fakeConn{id})
	}
	firsts := map[string]bool{}
	for i := 0; i < 6; i++ {
		ord := c.order("owner")
		if len(ord) != 4 {
			t.Fatalf("order returned %d sources, want 4", len(ord))
		}
		if ord[len(ord)-1].peerID != "owner" {
			t.Fatalf("owner is not the last source: last = %q", ord[len(ord)-1].peerID)
		}
		firsts[ord[0].peerID] = true
	}
	if len(firsts) < 2 {
		t.Fatalf("rotation never varied the first source: %v", firsts)
	}
}

// missing() deduplicates repeated chunk ids and excludes ones already stored.
func TestCoordinatorMissingDedups(t *testing.T) {
	p := newPeer(t, ownerID)
	ctx := context.Background()
	have, err := p.chunks.Put(ctx, p.fc, []byte("stored chunk data"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	absent := hasher.Sum([]byte("absent chunk data"))
	c := NewCoordinator(folderID, p.fc, p.chunks, 0, nil)

	want, err := c.missing(ctx, []manifest.ChunkRef{{ID: have}, {ID: have}, {ID: absent}, {ID: absent}})
	if err != nil {
		t.Fatalf("missing: %v", err)
	}
	if len(want) != 1 || want[0] != absent {
		t.Fatalf("missing = %v, want exactly the one absent chunk", want)
	}
}

// pull() errors rather than hanging when a needed chunk has no source.
func TestCoordinatorPullNoSources(t *testing.T) {
	p := newPeer(t, ownerID)
	ctx := context.Background()
	c := NewCoordinator(folderID, p.fc, p.chunks, 0, nil)

	absent := hasher.Sum([]byte("never stored"))
	if err := c.pull(ctx, []manifest.ChunkRef{{ID: absent}}, "owner"); err == nil {
		t.Fatal("pull with a missing chunk and no sources should error")
	}
}
