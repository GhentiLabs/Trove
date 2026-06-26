package membership

import (
	"context"
	"crypto/ed25519"
	"errors"
	"path/filepath"
	"testing"

	"github.com/GhentiLabs/Trove/pkg/identity"

	"github.com/GhentiLabs/Trove/client/internal/storage"
)

type node struct {
	store *Store
	key   ed25519.PrivateKey
	pub   ed25519.PublicKey
	id    string
}

func newNode(t *testing.T) node {
	t.Helper()
	pub, key, err := identity.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	id, err := identity.FingerprintKey(pub)
	if err != nil {
		t.Fatalf("FingerprintKey: %v", err)
	}
	db, err := storage.Open(storage.Options{Path: filepath.Join(t.TempDir(), "cfg.db"), MaxOpenConns: 4})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	s, err := Open(Options{DB: db, NodeID: id, Key: key})
	if err != nil {
		t.Fatalf("membership.Open: %v", err)
	}
	return node{store: s, key: key, pub: pub, id: id}
}

func isMember(t *testing.T, s *Store, networkID, nodeID string) bool {
	t.Helper()
	ok, err := s.IsMember(context.Background(), networkID, nodeID)
	if err != nil {
		t.Fatalf("IsMember: %v", err)
	}
	return ok
}

func TestFoundSelfWriter(t *testing.T) {
	f := newNode(t)
	ctx := context.Background()
	net, err := f.store.Found(ctx)
	if err != nil {
		t.Fatalf("Found: %v", err)
	}
	if founder, ok := Founder(net); !ok || founder != f.id {
		t.Fatalf("group id %q does not commit to founder %q (ok=%v)", net, f.id, ok)
	}
	if !isMember(t, f.store, net, f.id) {
		t.Fatal("founder is not a member of its own group")
	}
}

// Distinct folders founded by one node are independent groups with their own rosters.
func TestFoundDistinctGroups(t *testing.T) {
	f := newNode(t)
	a := newNode(t)
	ctx := context.Background()
	g1, _ := f.store.Found(ctx)
	g2, _ := f.store.Found(ctx)
	if g1 == g2 {
		t.Fatalf("two folders got the same group id %q", g1)
	}
	if _, err := f.store.Add(ctx, g1, a.id, a.pub, RoleReader); err != nil {
		t.Fatalf("Add to g1: %v", err)
	}
	if !isMember(t, f.store, g1, a.id) {
		t.Fatal("a not in g1")
	}
	if isMember(t, f.store, g2, a.id) {
		t.Fatal("a leaked into g2; group rosters are not isolated")
	}
}

// Re-merging entries already in the roster admits nothing, so gossip does not amplify.
func TestMergeIdempotentOnKnownEntries(t *testing.T) {
	f := newNode(t)
	a := newNode(t)
	ctx := context.Background()
	net, _ := f.store.Found(ctx)
	if _, err := f.store.Add(ctx, net, a.id, a.pub, RoleReader); err != nil {
		t.Fatalf("Add: %v", err)
	}
	full := mustRoster(t, f.store, net)

	b := newNode(t)
	if err := b.store.Join(ctx, net); err != nil {
		t.Fatal(err)
	}
	if added, err := b.store.Merge(ctx, net, full); err != nil || len(added) != len(full) {
		t.Fatalf("first merge: added %d err %v, want %d", len(added), err, len(full))
	}
	added, err := b.store.Merge(ctx, net, full)
	if err != nil {
		t.Fatalf("re-merge: %v", err)
	}
	if len(added) != 0 {
		t.Fatalf("re-merge admitted %d known entries, want 0", len(added))
	}
}

// The reserved holder role (2) cannot be signed until encrypted-folder sync exists.
func TestSignRejectsReservedRole(t *testing.T) {
	n := newNode(t)
	e := Entry{NetworkID: "net", NodeID: n.id, PublicKey: n.pub, Role: 2, AddedBy: n.id, AddedAtMs: 1}
	if _, err := Sign(n.key, e); !errors.Is(err, ErrInvalidEntry) {
		t.Fatalf("Sign with reserved role 2: err = %v, want ErrInvalidEntry", err)
	}
}

func TestAddRequiresWriter(t *testing.T) {
	f := newNode(t)
	a := newNode(t)
	victim := newNode(t)
	ctx := context.Background()
	net, _ := f.store.Found(ctx)

	if _, err := a.store.Add(ctx, net, victim.id, victim.pub, RoleReader); !errors.Is(err, ErrNotWriter) {
		t.Fatalf("Add by non-writer: err = %v, want ErrNotWriter", err)
	}
}

func TestAddRejectsKeyNodeMismatch(t *testing.T) {
	f := newNode(t)
	a := newNode(t)
	ctx := context.Background()
	net, _ := f.store.Found(ctx)

	if _, err := f.store.Add(ctx, net, a.id, f.pub, RoleReader); !errors.Is(err, ErrKeyMismatch) {
		t.Fatalf("Add with mismatched key: err = %v, want ErrKeyMismatch", err)
	}
	if isMember(t, f.store, net, a.id) {
		t.Fatal("a phantom member was persisted")
	}
}

// A fresh node learns the full roster from a peer that holds it, anchored only by the
// network_id — the headline membership property.
func TestLearnFullRosterFromPeer(t *testing.T) {
	f := newNode(t)
	a := newNode(t)
	b := newNode(t)
	ctx := context.Background()

	net, _ := f.store.Found(ctx)
	if _, err := f.store.Add(ctx, net, a.id, a.pub, RoleReader); err != nil {
		t.Fatalf("Add a: %v", err)
	}
	full, err := f.store.Roster(ctx, net)
	if err != nil {
		t.Fatalf("Roster: %v", err)
	}

	// b joins with only the network_id, then merges the roster it received from a peer.
	if err := b.store.Join(ctx, net); err != nil {
		t.Fatalf("Join: %v", err)
	}
	added, err := b.store.Merge(ctx, net, full)
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if len(added) != len(full) {
		t.Fatalf("merged %d entries, want %d", len(added), len(full))
	}
	if !isMember(t, b.store, net, f.id) || !isMember(t, b.store, net, a.id) {
		t.Fatal("b did not learn the full roster")
	}
}

func TestMergeOutOfOrderConverges(t *testing.T) {
	f := newNode(t)
	admin := newNode(t)
	member := newNode(t)
	ctx := context.Background()
	net, _ := f.store.Found(ctx)

	if _, err := f.store.Add(ctx, net, admin.id, admin.pub, RoleWriter); err != nil {
		t.Fatalf("add admin: %v", err)
	}
	// admin must also be in admin's own store to sign; instead craft the member entry
	// signed by admin's key directly.
	adminEntry, err := loadByID(t, f.store, net, admin.id)
	if err != nil {
		t.Fatal(err)
	}
	memberEntry, err := Sign(admin.key, Entry{
		NetworkID: net, NodeID: member.id, PublicKey: member.pub, Role: RoleReader, AddedBy: admin.id, AddedAtMs: 1,
	})
	if err != nil {
		t.Fatalf("sign member: %v", err)
	}
	genesis, _ := loadByID(t, f.store, net, f.id)

	b := newNode(t)
	if err := b.store.Join(ctx, net); err != nil {
		t.Fatal(err)
	}
	// Deliver the member entry BEFORE the admin entry that authorizes it.
	added, err := b.store.Merge(ctx, net, []Entry{memberEntry, adminEntry, genesis})
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if len(added) != 3 {
		t.Fatalf("merged %d, want 3 (genesis, admin, member)", len(added))
	}
	if !isMember(t, b.store, net, member.id) {
		t.Fatal("member not admitted via the admin it depends on")
	}
}

func TestMergeRejectsForgedAndMemberSigned(t *testing.T) {
	f := newNode(t)
	member := newNode(t)
	outsider := newNode(t)
	victim := newNode(t)
	ctx := context.Background()
	net, _ := f.store.Found(ctx)
	if _, err := f.store.Add(ctx, net, member.id, member.pub, RoleReader); err != nil {
		t.Fatalf("add member: %v", err)
	}

	b := newNode(t)
	_ = b.store.Join(ctx, net)
	genesis, _ := loadByID(t, f.store, net, f.id)
	memberEntry, _ := loadByID(t, f.store, net, member.id)
	if _, err := b.store.Merge(ctx, net, []Entry{genesis, memberEntry}); err != nil {
		t.Fatalf("seed merge: %v", err)
	}

	// 1) An entry self-signed by an outsider (not the network root) is rejected.
	forged, _ := Sign(outsider.key, Entry{
		NetworkID: net, NodeID: outsider.id, PublicKey: outsider.pub, Role: RoleWriter, AddedBy: outsider.id, AddedAtMs: 1,
	})
	// 2) An entry signed by a non-admin member is rejected.
	memberSigned, _ := Sign(member.key, Entry{
		NetworkID: net, NodeID: victim.id, PublicKey: victim.pub, Role: RoleReader, AddedBy: member.id, AddedAtMs: 1,
	})
	added, err := b.store.Merge(ctx, net, []Entry{forged, memberSigned})
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if len(added) != 0 {
		t.Fatalf("admitted %d invalid entries, want 0", len(added))
	}
	if isMember(t, b.store, net, outsider.id) || isMember(t, b.store, net, victim.id) {
		t.Fatal("an invalid entry was admitted")
	}
}

func TestMergeRejectsWrongNetwork(t *testing.T) {
	f := newNode(t)
	other := newNode(t)
	ctx := context.Background()
	net, _ := f.store.Found(ctx)
	b := newNode(t)
	_ = b.store.Join(ctx, net)

	wrong, _ := Sign(other.key, Entry{
		NetworkID: "different-network", NodeID: other.id, PublicKey: other.pub, Role: RoleWriter, AddedBy: other.id, AddedAtMs: 1,
	})
	added, err := b.store.Merge(ctx, net, []Entry{wrong})
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if len(added) != 0 {
		t.Fatal("admitted an entry for a different network")
	}
}

func loadByID(t *testing.T, s *Store, networkID, nodeID string) (Entry, error) {
	t.Helper()
	for _, e := range mustRoster(t, s, networkID) {
		if e.NodeID == nodeID {
			return e, nil
		}
	}
	return Entry{}, errors.New("entry not found in roster")
}

func mustRoster(t *testing.T, s *Store, networkID string) []Entry {
	t.Helper()
	r, err := s.Roster(context.Background(), networkID)
	if err != nil {
		t.Fatalf("Roster: %v", err)
	}
	return r
}
