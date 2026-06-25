// The Run/gather/loop wiring is exercised end-to-end by the live two-machine gate
// (cmd/trove-peer) and the NAT matrix (client/test/nat); the tests here cover the
// pure translation seams between config and the session/peermgr layers.
package node

import (
	"context"
	"net"
	"path/filepath"
	"slices"
	"testing"

	"github.com/GhentiLabs/Trove/client/internal/config"
	"github.com/GhentiLabs/Trove/client/internal/storage"
	disco "github.com/GhentiLabs/Trove/pkg/discovery"
)

const (
	selfID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	peerID = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func newService(t *testing.T) *Service {
	t.Helper()
	db, err := storage.Open(storage.Options{Path: filepath.Join(t.TempDir(), "c.db"), MaxOpenConns: 1})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store, err := config.Open(config.Options{DB: db, NodeID: selfID})
	if err != nil {
		t.Fatalf("config.Open: %v", err)
	}
	ctx := context.Background()
	if err := store.AddFolder(ctx, config.Folder{ID: "docs", Root: "/docs", ShareID: "docs-share"}); err != nil {
		t.Fatalf("AddFolder shared: %v", err)
	}
	if err := store.AddFolder(ctx, config.Folder{ID: "scratch", Root: "/scratch"}); err != nil {
		t.Fatalf("AddFolder unpaired: %v", err)
	}
	if err := store.AddPeer(ctx, config.Peer{NodeID: peerID, Name: "laptop", Folders: []string{"docs-share"}}); err != nil {
		t.Fatalf("AddPeer: %v", err)
	}
	return &Service{opts: Options{Config: store, NodeID: selfID}}
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

func TestAuthorizeGrantsConfiguredFolders(t *testing.T) {
	s := newService(t)

	granted, ok, err := s.authorize(peerID)
	if err != nil || !ok {
		t.Fatalf("authorize(known) = ok %v, err %v, want true, nil", ok, err)
	}
	if !slices.Equal(granted, []string{"docs-share"}) {
		t.Fatalf("granted = %v, want [docs-share]", granted)
	}

	unknown := "cccccccccccccccccccccccccccccccccccccccccccccccccccc"
	if _, ok, err := s.authorize(unknown); ok || err != nil {
		t.Fatalf("authorize(unknown) = ok %v, err %v, want false, nil", ok, err)
	}
}

func TestLocalConfigOffersOnlyPairedFolders(t *testing.T) {
	s := newService(t)

	local, err := s.localConfig(context.Background())
	if err != nil {
		t.Fatalf("localConfig: %v", err)
	}
	if local.NodeID != selfID {
		t.Fatalf("NodeID = %q, want %q", local.NodeID, selfID)
	}
	if len(local.Folders) != 1 || local.Folders[0].ShareID != "docs-share" {
		t.Fatalf("offered folders = %+v, want only docs-share (scratch is unpaired)", local.Folders)
	}
}

func TestPeerIDsListsAuthorizedPeers(t *testing.T) {
	s := newService(t)

	ids, err := s.peerIDs(context.Background())
	if err != nil {
		t.Fatalf("peerIDs: %v", err)
	}
	if !slices.Equal(ids, []string{peerID}) {
		t.Fatalf("peerIDs = %v, want [%s]", ids, peerID)
	}
}
