package node

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/GhentiLabs/Trove/client/internal/chunkstore"
	"github.com/GhentiLabs/Trove/client/internal/config"
	"github.com/GhentiLabs/Trove/client/internal/crypto"
	"github.com/GhentiLabs/Trove/client/internal/membership"
	"github.com/GhentiLabs/Trove/client/internal/storage"
	"github.com/GhentiLabs/Trove/client/internal/syncengine"
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
	verifier := crypto.FolderVerifier(key, group)

	if err := rt.receiveFolderKey(ctx, log, founderID, fk, verifier); !errors.Is(err, errReattachAfterKey) {
		t.Fatalf("first delivery err = %v, want errReattachAfterKey", err)
	}
	got, gen, err := cfg.GetFolderKey(ctx, group)
	if err != nil || got != key || gen != 1 {
		t.Fatalf("stored key=%x gen=%d err=%v, want key=%x gen=1", got, gen, err, key)
	}
	if err := rt.receiveFolderKey(ctx, log, founderID, fk, verifier); err != nil {
		t.Fatalf("replay delivery err = %v, want nil", err)
	}
}

// TestReceiveFolderKeyRejectsVerifierMismatch checks a key whose verifier disagrees with
// the one the sender announced is refused and not stored.
func TestReceiveFolderKeyRejectsVerifierMismatch(t *testing.T) {
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
	var key [config.MasterKeyLen]byte
	key[0] = 0x55
	fk := &wirepb.FolderKey{FolderId: group, Key: key[:], KeyGeneration: 1}
	if err := rt.receiveFolderKey(ctx, slog.New(slog.DiscardHandler), founderID, fk, []byte("wrong-verifier")); err != nil {
		t.Fatalf("mismatch delivery err = %v, want nil", err)
	}
	if _, _, err := cfg.GetFolderKey(ctx, group); !errors.Is(err, config.ErrNoKey) {
		t.Fatalf("key stored despite verifier mismatch; want ErrNoKey")
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
	if err := rt.receiveFolderKey(ctx, log, stranger, &wirepb.FolderKey{FolderId: group, Key: key[:], KeyGeneration: 1}, nil); err != nil {
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
	if err := rt2.receiveFolderKey(ctx, log, founderID, &wirepb.FolderKey{FolderId: group2, Key: short, KeyGeneration: 1}, nil); err != nil {
		t.Fatalf("short-key delivery err = %v, want nil", err)
	}
	if _, _, err := cfg2.GetFolderKey(ctx, group2); !errors.Is(err, config.ErrNoKey) {
		t.Fatalf("short key was stored; want ErrNoKey")
	}
}

// TestHolderPushCoalesces checks the live-mirror pusher collapses a burst of change triggers
// during one in-flight push into a single queued re-run, and that stop prevents new runs.
func TestHolderPushCoalesces(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	set := &holderPushSet{ctx: ctx}

	var runs atomic.Int64
	started := make(chan struct{})
	release := make(chan struct{})
	p := &holderPusher{folder: "f", log: slog.New(slog.DiscardHandler), do: func(context.Context) error {
		runs.Add(1)
		started <- struct{}{}
		<-release
		return nil
	}}

	set.trigger(p)
	<-started // run 1 is in do
	set.trigger(p)
	set.trigger(p)
	set.trigger(p) // coalesced: dirty, no new run
	release <- struct{}{}
	<-started // run 2 (the single queued re-run) is in do
	release <- struct{}{}
	set.stop()

	if got := runs.Load(); got != 2 {
		t.Fatalf("runs = %d, want 2 (initial + one coalesced re-run)", got)
	}
	set.trigger(p)
	if runs.Load() != 2 {
		t.Fatal("trigger after stop started a new run")
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
			got, err := rt.holderPutAllowed(ctx, group, tc.peerID)
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

	if d := rt.folderKeyForPeer(ctx, slog.New(slog.DiscardHandler), cf, readerID, key, 1); d == nil || d.GetFolderId() != group {
		t.Fatalf("folderKeyForPeer to reader = %v, want a message for %s", d, group)
	}
	stranger := "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"
	if d := rt.folderKeyForPeer(ctx, slog.New(slog.DiscardHandler), cf, stranger, key, 1); d != nil {
		t.Fatal("delivered a key to a non-member")
	}

	_, holderID, holderPub := openMembers(t)
	if _, err := members.Add(ctx, group, holderID, holderPub, membership.RoleHolder); err != nil {
		t.Fatalf("Add holder: %v", err)
	}
	if d := rt.folderKeyForPeer(ctx, slog.New(slog.DiscardHandler), cf, holderID, key, 1); d != nil {
		t.Fatal("delivered a key to a holder")
	}
}

// TestReceiveFolderKeyIgnoredForHolderFolder checks a holder node never stores a delivered
// key, even from the founder.
func TestReceiveFolderKeyIgnoredForHolderFolder(t *testing.T) {
	ctx := context.Background()
	members, founderID, _ := openMembers(t)
	group, err := members.Found(ctx)
	if err != nil {
		t.Fatalf("Found: %v", err)
	}
	cfg := openConfig(t, "holder-self")
	if err := cfg.AddFolder(ctx, config.Folder{ID: group, Root: "", ShareID: group, Encrypted: true, Holder: true}); err != nil {
		t.Fatalf("AddFolder: %v", err)
	}
	rt := &syncRuntime{self: "holder-self", members: members, cfg: cfg, byShare: map[string]config.Folder{
		group: {ID: group, ShareID: group, Encrypted: true, Holder: true},
	}}
	var key [config.MasterKeyLen]byte
	key[0] = 0x7
	fk := &wirepb.FolderKey{FolderId: group, Key: key[:], KeyGeneration: 1}
	if err := rt.receiveFolderKey(ctx, slog.New(slog.DiscardHandler), founderID, fk, nil); err != nil {
		t.Fatalf("holder delivery err = %v, want nil", err)
	}
	if _, _, err := cfg.GetFolderKey(ctx, group); !errors.Is(err, config.ErrNoKey) {
		t.Fatal("holder stored a delivered key; want ErrNoKey")
	}
}

// TestAttachFoldersReconnectAfterKeyDelivery checks an encrypted folder is skipped while
// unkeyed and attaches with the live key once delivered (the reconnect-to-attach flow).
func TestAttachFoldersReconnectAfterKeyDelivery(t *testing.T) {
	ctx := context.Background()
	members, founderID, _ := openMembers(t)
	group, err := members.Found(ctx)
	if err != nil {
		t.Fatalf("Found: %v", err)
	}
	cfg := openConfig(t, founderID)
	if err := cfg.AddFolder(ctx, config.Folder{ID: group, Root: "/r", ShareID: group, Encrypted: true}); err != nil {
		t.Fatalf("AddFolder: %v", err)
	}
	fc := syncengine.FolderConfig{
		FolderID: group, Role: syncengine.RoleWriter,
		Coord: syncengine.NewCoordinator(group, chunkstore.FolderContext{Encrypted: true}, nil, 0, nil),
	}
	rt := &syncRuntime{
		self: founderID, members: members, cfg: cfg,
		folders: []syncengine.FolderConfig{fc},
		byShare: map[string]config.Folder{group: {ID: group, ShareID: group, Encrypted: true}},
	}
	log := slog.New(slog.DiscardHandler)
	shared := map[string]bool{group: true}

	if fcs, _ := rt.attachFolders(ctx, log, "peer", shared); len(fcs) != 0 {
		t.Fatalf("unkeyed encrypted folder attached: %d configs", len(fcs))
	}

	key, err := cfg.GenerateFolderKey(ctx, group)
	if err != nil {
		t.Fatalf("GenerateFolderKey: %v", err)
	}
	fcs, _ := rt.attachFolders(ctx, log, "peer", shared)
	if len(fcs) != 1 {
		t.Fatalf("keyed folder not attached: %d configs", len(fcs))
	}
	if !fcs[0].FolderCtx.Encrypted || fcs[0].FolderCtx.MasterKey != key {
		t.Fatal("attached folder context is not keyed")
	}
}
