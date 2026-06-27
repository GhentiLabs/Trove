package node

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/GhentiLabs/Trove/client/internal/config"
	"github.com/GhentiLabs/Trove/client/internal/membership"
	"github.com/GhentiLabs/Trove/client/internal/storage"
	"github.com/GhentiLabs/Trove/client/internal/wire/wirepb"
	"github.com/GhentiLabs/Trove/pkg/identity"
)

func openConfig(t *testing.T, nodeID string) *config.Store {
	t.Helper()
	db, err := storage.Open(storage.Options{Path: filepath.Join(t.TempDir(), "c.db"), MaxOpenConns: 1})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	s, err := config.Open(config.Options{DB: db, NodeID: nodeID})
	if err != nil {
		t.Fatalf("config.Open: %v", err)
	}
	return s
}

func openMembers(t *testing.T) (*membership.Store, string, []byte) {
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
	m, err := membership.Open(membership.Options{DB: db, NodeID: id, Key: key})
	if err != nil {
		t.Fatalf("membership.Open: %v", err)
	}
	return m, id, pub
}

// TestReceiveFolderKeyFromWriter checks that a key delivered by the group founder for an
// unkeyed encrypted folder is persisted once, triggers a reattach, and that a replay is
// ignored.
func TestReceiveFolderKeyFromWriter(t *testing.T) {
	ctx := context.Background()
	members, founderID, _ := openMembers(t)
	group, err := members.Found(ctx)
	if err != nil {
		t.Fatalf("Found: %v", err)
	}

	cfg := openConfig(t, "self-node")
	if err := cfg.AddFolder(ctx, config.Folder{ID: group, Root: "/r", ShareID: group, Encrypted: true}); err != nil {
		t.Fatalf("AddFolder: %v", err)
	}
	rt := &syncRuntime{self: "self-node", members: members, cfg: cfg, byShare: map[string]config.Folder{
		group: {ID: group, Root: "/r", ShareID: group, Encrypted: true},
	}}
	log := slog.New(slog.DiscardHandler)

	var key [config.MasterKeyLen]byte
	key[0] = 0x11
	fk := &wirepb.FolderKey{FolderId: group, Key: key[:], KeyGeneration: 1}

	if err := rt.receiveFolderKey(ctx, log, founderID, fk); !errors.Is(err, errReattachAfterKey) {
		t.Fatalf("first delivery err = %v, want errReattachAfterKey", err)
	}
	got, gen, err := cfg.GetFolderKey(ctx, group)
	if err != nil || got != key || gen != 1 {
		t.Fatalf("stored key=%x gen=%d err=%v, want key=%x gen=1", got, gen, err, key)
	}
	if err := rt.receiveFolderKey(ctx, log, founderID, fk); err != nil {
		t.Fatalf("replay delivery err = %v, want nil", err)
	}
}

// TestReceiveFolderKeyRejectsNonWriter checks a key offered by a node that is not a roster
// writer is ignored and never stored.
func TestReceiveFolderKeyRejectsNonWriter(t *testing.T) {
	ctx := context.Background()
	members, _, _ := openMembers(t)
	group, err := members.Found(ctx)
	if err != nil {
		t.Fatalf("Found: %v", err)
	}
	cfg := openConfig(t, "self-node")
	if err := cfg.AddFolder(ctx, config.Folder{ID: group, Root: "/r", ShareID: group, Encrypted: true}); err != nil {
		t.Fatalf("AddFolder: %v", err)
	}
	rt := &syncRuntime{self: "self-node", members: members, cfg: cfg, byShare: map[string]config.Folder{
		group: {ID: group, Root: "/r", ShareID: group, Encrypted: true},
	}}
	log := slog.New(slog.DiscardHandler)

	var key [config.MasterKeyLen]byte
	key[0] = 0x22
	stranger := "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"
	if err := rt.receiveFolderKey(ctx, log, stranger, &wirepb.FolderKey{FolderId: group, Key: key[:], KeyGeneration: 1}); err != nil {
		t.Fatalf("non-writer delivery err = %v, want nil", err)
	}
	if _, _, err := cfg.GetFolderKey(ctx, group); !errors.Is(err, config.ErrNoKey) {
		t.Fatalf("key state after non-writer delivery err = %v, want ErrNoKey (not stored)", err)
	}
	members2, founderID, _ := openMembers(t)
	group2, err := members2.Found(ctx)
	if err != nil {
		t.Fatalf("Found: %v", err)
	}
	cfg2 := openConfig(t, "self-node-2")
	if err := cfg2.AddFolder(ctx, config.Folder{ID: group2, Root: "/r", ShareID: group2, Encrypted: true}); err != nil {
		t.Fatalf("AddFolder: %v", err)
	}
	rt2 := &syncRuntime{self: "self-node-2", members: members2, cfg: cfg2, byShare: map[string]config.Folder{
		group2: {ID: group2, Root: "/r", ShareID: group2, Encrypted: true},
	}}
	short := []byte{1, 2, 3}
	if err := rt2.receiveFolderKey(ctx, log, founderID, &wirepb.FolderKey{FolderId: group2, Key: short, KeyGeneration: 1}); err != nil {
		t.Fatalf("short-key delivery err = %v, want nil", err)
	}
	if _, _, err := cfg2.GetFolderKey(ctx, group2); !errors.Is(err, config.ErrNoKey) {
		t.Fatalf("short key was stored; want ErrNoKey")
	}
}

// TestHolderPutAllowedOnlyForWriters checks a holder accepts blob stores only from a
// roster writer (the founder here), not from a reader or a non-member.
func TestHolderPutAllowedOnlyForWriters(t *testing.T) {
	ctx := context.Background()
	members, founderID, _ := openMembers(t)
	group, err := members.Found(ctx)
	if err != nil {
		t.Fatalf("Found: %v", err)
	}
	_, readerID, readerPub := openMembers(t)
	if _, err := members.Add(ctx, group, readerID, readerPub, membership.RoleReader); err != nil {
		t.Fatalf("Add reader: %v", err)
	}
	rt := &syncRuntime{self: "holder-self", members: members}

	cases := []struct {
		name   string
		peerID string
		want   bool
	}{
		{"writer/founder allowed", founderID, true},
		{"reader denied", readerID, false},
		{"non-member denied", "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := rt.holderPutAllowed(group, tc.peerID)
			if err != nil {
				t.Fatalf("holderPutAllowed: %v", err)
			}
			if got != tc.want {
				t.Fatalf("holderPutAllowed(%s) = %v, want %v", tc.peerID, got, tc.want)
			}
		})
	}
}

// TestDeliverableOnlyToTrustedMembers checks a writer offers the key to a reader member
// but never to a non-member.
func TestDeliverableOnlyToTrustedMembers(t *testing.T) {
	ctx := context.Background()
	members, founderID, _ := openMembers(t)
	group, err := members.Found(ctx)
	if err != nil {
		t.Fatalf("Found: %v", err)
	}
	_, readerID, readerPub := openMembers(t)
	if _, err := members.Add(ctx, group, readerID, readerPub, membership.RoleReader); err != nil {
		t.Fatalf("Add reader: %v", err)
	}

	cfg := openConfig(t, founderID)
	if err := cfg.AddFolder(ctx, config.Folder{ID: group, Root: "/r", ShareID: group, Encrypted: true}); err != nil {
		t.Fatalf("AddFolder: %v", err)
	}
	rt := &syncRuntime{self: founderID, members: members, cfg: cfg}
	cf := config.Folder{ID: group, Root: "/r", ShareID: group, Encrypted: true}
	var key [config.MasterKeyLen]byte

	if d := rt.deliverable(ctx, slog.New(slog.DiscardHandler), cf, readerID, key, 1); d == nil || d.GetFolderId() != group {
		t.Fatalf("deliverable to reader = %v, want a message for %s", d, group)
	}
	stranger := "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"
	if d := rt.deliverable(ctx, slog.New(slog.DiscardHandler), cf, stranger, key, 1); d != nil {
		t.Fatal("delivered a key to a non-member")
	}
}
