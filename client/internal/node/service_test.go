// The Run/gather/loop wiring is exercised end-to-end by the live two-machine gate
// (cmd/trove-peer) and the e2e matrix (client/test/e2e); the tests here cover the
// pure translation seams between config and the session/peermgr layers.
package node

import (
	"context"
	"net"
	"path/filepath"
	"slices"
	"testing"

	"github.com/GhentiLabs/Trove/client/internal/config"
	"github.com/GhentiLabs/Trove/client/internal/membership"
	"github.com/GhentiLabs/Trove/client/internal/storage"
	disco "github.com/GhentiLabs/Trove/pkg/discovery"
	"github.com/GhentiLabs/Trove/pkg/identity"
)

// serviceFixture is a Service whose roster has one peer (a reader) admitted to a folder's
// group, plus an unpaired local folder.
type serviceFixture struct {
	svc    *Service
	group  string
	peerID string
}

func newService(t *testing.T) serviceFixture {
	t.Helper()
	ctx := context.Background()

	pub, key, err := identity.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	selfID, err := identity.FingerprintKey(pub)
	if err != nil {
		t.Fatalf("FingerprintKey: %v", err)
	}

	cdb, err := storage.Open(storage.Options{Path: filepath.Join(t.TempDir(), "c.db"), MaxOpenConns: 1})
	if err != nil {
		t.Fatalf("storage.Open config: %v", err)
	}
	t.Cleanup(func() { _ = cdb.Close() })
	store, err := config.Open(config.Options{DB: cdb, NodeID: selfID})
	if err != nil {
		t.Fatalf("config.Open: %v", err)
	}

	mdb, err := storage.Open(storage.Options{Path: filepath.Join(t.TempDir(), "m.db"), MaxOpenConns: 4})
	if err != nil {
		t.Fatalf("storage.Open membership: %v", err)
	}
	t.Cleanup(func() { _ = mdb.Close() })
	members, err := membership.Open(membership.Options{DB: mdb, NodeID: selfID, Key: key})
	if err != nil {
		t.Fatalf("membership.Open: %v", err)
	}
	group, err := members.Found(ctx)
	if err != nil {
		t.Fatalf("Found: %v", err)
	}

	ppub, _, err := identity.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey peer: %v", err)
	}
	peerID, err := identity.FingerprintKey(ppub)
	if err != nil {
		t.Fatalf("FingerprintKey peer: %v", err)
	}
	if _, err := members.Add(ctx, group, peerID, ppub, membership.RoleReader); err != nil {
		t.Fatalf("Add peer: %v", err)
	}

	if err := store.AddFolder(ctx, config.Folder{ID: "docs", Root: "/docs", ShareID: group}); err != nil {
		t.Fatalf("AddFolder shared: %v", err)
	}
	if err := store.AddFolder(ctx, config.Folder{ID: "scratch", Root: "/scratch"}); err != nil {
		t.Fatalf("AddFolder unpaired: %v", err)
	}
	return serviceFixture{
		svc:    &Service{opts: Options{Config: store, NodeID: selfID}, members: members},
		group:  group,
		peerID: peerID,
	}
}

func mustCIDR(t *testing.T, s string) *net.IPNet {
	t.Helper()
	_, n, err := net.ParseCIDR(s)
	if err != nil {
		t.Fatalf("ParseCIDR(%q): %v", s, err)
	}
	return n
}

func TestDialable(t *testing.T) {
	local := []*net.IPNet{mustCIDR(t, "192.168.1.0/24")}
	tests := []struct {
		name string
		ip   string
		want bool
	}{
		{"public always dialable", "203.0.113.7", true},
		{"private inside our subnet", "192.168.1.50", true},
		{"private outside our subnets", "10.0.0.5", false},
		{"private same family other subnet", "192.168.2.50", false},
		{"invalid ip", "nope", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := dialable(disco.Address{IP: tt.ip, Port: 22000, Type: disco.AddressPublic}, local)
			if got != tt.want {
				t.Fatalf("dialable(%q) = %v, want %v", tt.ip, got, tt.want)
			}
		})
	}
}

func TestDialableNoLocalSubnets(t *testing.T) {
	// With no local subnets, the SSRF guard fails closed on private addresses.
	if dialable(disco.Address{IP: "192.168.1.50", Port: 1}, nil) {
		t.Fatal("private address dialable with no local subnets")
	}
	if !dialable(disco.Address{IP: "203.0.113.7", Port: 1}, nil) {
		t.Fatal("public address not dialable")
	}
}

func TestAuthorizeGrantsRosterMembers(t *testing.T) {
	f := newService(t)

	granted, ok, err := f.svc.authorize(context.Background(), f.peerID)
	if err != nil || !ok {
		t.Fatalf("authorize(member) = ok %v, err %v, want true, nil", ok, err)
	}
	if !slices.Equal(granted, []string{f.group}) {
		t.Fatalf("granted = %v, want [%s]", granted, f.group)
	}

	unknown := "cccccccccccccccccccccccccccccccccccccccccccccccccccc"
	if _, ok, err := f.svc.authorize(context.Background(), unknown); ok || err != nil {
		t.Fatalf("authorize(unknown) = ok %v, err %v, want false, nil", ok, err)
	}
}

func TestLocalConfigOffersOnlyPairedFolders(t *testing.T) {
	f := newService(t)

	local, err := f.svc.localConfig(context.Background())
	if err != nil {
		t.Fatalf("localConfig: %v", err)
	}
	if local.NodeID != f.svc.opts.NodeID {
		t.Fatalf("NodeID = %q, want %q", local.NodeID, f.svc.opts.NodeID)
	}
	if len(local.Folders) != 1 || local.Folders[0].ShareID != f.group {
		t.Fatalf("offered folders = %+v, want only the group folder (scratch is unpaired)", local.Folders)
	}
}

// A node that has only joined a group (empty roster) still authorizes the group's
// founder, derived from the group id — the bootstrap that lets it pull the roster.
func TestAuthorizeFounderBootstrap(t *testing.T) {
	ctx := context.Background()

	pub, key, err := identity.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	selfID, err := identity.FingerprintKey(pub)
	if err != nil {
		t.Fatalf("FingerprintKey: %v", err)
	}
	mdb, err := storage.Open(storage.Options{Path: filepath.Join(t.TempDir(), "m.db"), MaxOpenConns: 4})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = mdb.Close() })
	members, err := membership.Open(membership.Options{DB: mdb, NodeID: selfID, Key: key})
	if err != nil {
		t.Fatalf("membership.Open: %v", err)
	}

	fpub, _, err := identity.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey founder: %v", err)
	}
	founderID, err := identity.FingerprintKey(fpub)
	if err != nil {
		t.Fatalf("FingerprintKey founder: %v", err)
	}
	group := founderID + ".0123456789abcdef"
	if err := members.Join(ctx, group); err != nil {
		t.Fatalf("Join: %v", err)
	}

	s := &Service{opts: Options{NodeID: selfID}, members: members}
	granted, ok, err := s.authorize(context.Background(), founderID)
	if err != nil || !ok || !slices.Equal(granted, []string{group}) {
		t.Fatalf("authorize(founder) = %v, ok %v, err %v; want [%s], true, nil", granted, ok, err, group)
	}
	stranger := "dddddddddddddddddddddddddddddddddddddddddddddddddddd"
	if _, ok, _ := s.authorize(context.Background(), stranger); ok {
		t.Fatal("a non-founder non-member was authorized")
	}
}

func TestPeerIDsListsRosterMembers(t *testing.T) {
	f := newService(t)

	ids, err := f.svc.peerIDs(context.Background())
	if err != nil {
		t.Fatalf("peerIDs: %v", err)
	}
	if !slices.Equal(ids, []string{f.peerID}) {
		t.Fatalf("peerIDs = %v, want [%s]", ids, f.peerID)
	}
}
