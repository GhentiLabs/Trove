package node

import (
	"context"
	"crypto/ed25519"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/membership"
	"github.com/GhentiLabs/Trove/client/internal/netio"
	"github.com/GhentiLabs/Trove/client/internal/session"
	"github.com/GhentiLabs/Trove/client/internal/storage"
	"github.com/GhentiLabs/Trove/client/internal/wire"
	"github.com/GhentiLabs/Trove/client/internal/wire/wirepb"
	"github.com/GhentiLabs/Trove/pkg/identity"
	"google.golang.org/protobuf/proto"
)

type memberNode struct {
	store *membership.Store
	pub   ed25519.PublicKey
	id    string
}

func newMember(t *testing.T) memberNode {
	t.Helper()
	pub, key, err := identity.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	id, err := identity.FingerprintKey(pub)
	if err != nil {
		t.Fatalf("FingerprintKey: %v", err)
	}
	db, err := storage.Open(storage.Options{Path: filepath.Join(t.TempDir(), "m.db"), MaxOpenConns: 4})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	s, err := membership.Open(membership.Options{DB: db, NodeID: id, Key: key})
	if err != nil {
		t.Fatalf("membership.Open: %v", err)
	}
	return memberNode{store: s, pub: pub, id: id}
}

// gossipPair establishes a MemNet session between aID (accepts) and bID (dials),
// routing each side's MembershipGossip to its gossiper, and returns both sessions.
func gossipPair(t *testing.T, ctx context.Context, aID, bID string, ga, gb *gossiper) (*session.Session, *session.Session) {
	t.Helper()
	mn := netio.NewMemNet()
	at := mn.Transport("a", aID)
	bt := mn.Transport("b", bID)
	allow := func(string) ([]string, bool, error) { return nil, true, nil }

	type res struct {
		s   *session.Session
		err error
	}
	ch := make(chan res, 1)
	go func() {
		conn, err := at.Accept(ctx)
		if err != nil {
			ch <- res{nil, err}
			return
		}
		s, err := session.Handshake(ctx, session.Config{Conn: conn, Initiator: false, Authorize: allow, Local: session.Local{NodeID: aID}})
		ch <- res{s, err}
	}()
	bc, err := bt.Dial(ctx, "a", aID)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	bs, err := session.Handshake(ctx, session.Config{Conn: bc, Initiator: true, Authorize: allow, Local: session.Local{NodeID: bID}})
	if err != nil {
		t.Fatalf("handshake b: %v", err)
	}
	ar := <-ch
	if ar.err != nil {
		t.Fatalf("handshake a: %v", ar.err)
	}
	bindGossip(ar.s, ga)
	bindGossip(bs, gb)
	go func() { _ = ar.s.Run(ctx) }()
	go func() { _ = bs.Run(ctx) }()
	return ar.s, bs
}

func bindGossip(s *session.Session, g *gossiper) {
	peer := s.PeerNodeID()
	s.SetControlHandler(func(ctx context.Context, typ wire.MessageType, msg proto.Message) error {
		if typ == wire.TypeMembershipGossip {
			return g.handle(ctx, peer, msg.(*wirepb.MembershipGossip))
		}
		return nil
	})
}

// TestGossipPropagatesAcrossHop proves a node learns a roster transitively: the founder
// pushes to B, B re-broadcasts to C, so C learns the full roster without ever talking
// to the founder.
func TestGossipPropagatesAcrossHop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f := newMember(t)
	b := newMember(t)
	c := newMember(t)
	member := newMember(t) // admitted by the founder; needs no session

	net, err := f.store.Found(ctx)
	if err != nil {
		t.Fatalf("Found: %v", err)
	}
	if _, err := f.store.Add(ctx, net, member.id, member.pub, membership.RoleReader); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := b.store.Join(ctx, net); err != nil {
		t.Fatalf("b join: %v", err)
	}
	if err := c.store.Join(ctx, net); err != nil {
		t.Fatalf("c join: %v", err)
	}

	log := slog.New(slog.DiscardHandler)
	gf := newGossiper(f.store, log)
	gb := newGossiper(b.store, log)
	gc := newGossiper(c.store, log)

	// B<->C: register B's session to C so B can relay onward.
	_, bToC := gossipPair(t, ctx, c.id, b.id, gc, gb)
	gb.addPeer(ctx, bToC)

	// F<->B: registering F's session pushes the founder's roster to B, which relays to C.
	fToB, _ := gossipPair(t, ctx, f.id, b.id, gf, gb)
	gf.addPeer(ctx, fToB)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		fok, _ := c.store.IsMember(ctx, net, f.id)
		mok, _ := c.store.IsMember(ctx, net, member.id)
		if fok && mok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("C did not learn the roster transitively through B")
}
