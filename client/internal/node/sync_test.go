package node

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/chunkstore"
	"github.com/GhentiLabs/Trove/client/internal/config"
	"github.com/GhentiLabs/Trove/client/internal/crypto"
	"github.com/GhentiLabs/Trove/client/internal/gc"
	"github.com/GhentiLabs/Trove/client/internal/hasher"
	"github.com/GhentiLabs/Trove/client/internal/manifest"
	"github.com/GhentiLabs/Trove/client/internal/membership"
	"github.com/GhentiLabs/Trove/client/internal/model"
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
	set := &holderPusherSet{ctx: ctx}

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

// TestRecoverySecretForPeer checks a writer delivers an unencrypted folder's recovery secret to
// a trusted member (so it can later prove the recovery verifier) but never to a non-member.
func TestRecoverySecretForPeer(t *testing.T) {
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
	if err := cfg.AddFolder(ctx, config.Folder{ID: group, Root: "/r", ShareID: group}); err != nil {
		t.Fatalf("AddFolder: %v", err)
	}
	secret, err := cfg.GenerateRecoverySecret(ctx, group)
	if err != nil {
		t.Fatalf("GenerateRecoverySecret: %v", err)
	}
	rt := &syncRuntime{self: founderID, members: members, cfg: cfg, byShare: map[string]config.Folder{
		group: {ID: group, ShareID: group},
	}}
	log := slog.New(slog.DiscardHandler)
	cf := config.Folder{ID: group, ShareID: group}

	if d := rt.recoverySecretForPeer(ctx, log, cf, readerID); d == nil || !bytes.Equal(d.GetKey(), secret[:]) {
		t.Fatalf("no recovery secret delivered to a trusted reader")
	}
	stranger := "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"
	if d := rt.recoverySecretForPeer(ctx, log, cf, stranger); d != nil {
		t.Fatal("recovery secret delivered to a non-member")
	}
}

// TestReceiveRecoverySecret checks a writer's delivery of an unencrypted folder's recovery secret
// is stored (no reattach), a replay is benign, and a non-writer's delivery is ignored.
func TestReceiveRecoverySecret(t *testing.T) {
	ctx := context.Background()
	members, founderID, _ := openMembers(t)
	group, err := members.Found(ctx)
	if err != nil {
		t.Fatalf("Found: %v", err)
	}
	cfg := openConfig(t, "self-node")
	if err := cfg.AddFolder(ctx, config.Folder{ID: group, Root: "/r", ShareID: group}); err != nil {
		t.Fatalf("AddFolder: %v", err)
	}
	rt := &syncRuntime{self: "self-node", members: members, cfg: cfg, byShare: map[string]config.Folder{
		group: {ID: group, Root: "/r", ShareID: group},
	}}
	log := slog.New(slog.DiscardHandler)

	var secret [config.MasterKeyLen]byte
	secret[0] = 0x11
	fk := &wirepb.FolderKey{FolderId: group, Key: secret[:]}

	if err := rt.receiveFolderKey(ctx, log, founderID, fk, nil); err != nil {
		t.Fatalf("recovery secret delivery err = %v, want nil (no reattach)", err)
	}
	if got, err := cfg.FolderSecret(ctx, group); err != nil || got != secret {
		t.Fatalf("stored secret = %x err=%v, want %x", got, err, secret)
	}
	if err := rt.receiveFolderKey(ctx, log, founderID, fk, nil); err != nil {
		t.Fatalf("replay err = %v, want nil", err)
	}

	// A non-writer's delivery for a fresh folder is ignored.
	members2, _, _ := openMembers(t)
	group2, _ := members2.Found(ctx)
	cfg2 := openConfig(t, "self-2")
	if err := cfg2.AddFolder(ctx, config.Folder{ID: group2, Root: "/r", ShareID: group2}); err != nil {
		t.Fatalf("AddFolder g2: %v", err)
	}
	rt2 := &syncRuntime{self: "self-2", members: members2, cfg: cfg2, byShare: map[string]config.Folder{
		group2: {ID: group2, ShareID: group2},
	}}
	stranger := "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"
	if err := rt2.receiveFolderKey(ctx, log, stranger, &wirepb.FolderKey{FolderId: group2, Key: secret[:]}, nil); err != nil {
		t.Fatalf("non-writer delivery err = %v, want nil", err)
	}
	if _, err := cfg2.FolderSecret(ctx, group2); !errors.Is(err, config.ErrNoSecret) {
		t.Fatalf("secret stored from a non-writer; want ErrNoSecret")
	}
}

func newFolderStores(t *testing.T) (*model.Store, *chunkstore.Store) {
	t.Helper()
	dir := t.TempDir()
	cdb, err := storage.Open(storage.Options{Path: filepath.Join(dir, "chunk.db"), MaxOpenConns: 8})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cdb.Close() })
	cs, err := chunkstore.Open(chunkstore.Options{DB: cdb, BlobDir: filepath.Join(dir, "blobs")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	mdb, err := storage.Open(storage.Options{Path: filepath.Join(dir, "model.db"), MaxOpenConns: 8})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mdb.Close() })
	ms, err := model.Open(model.Options{DB: mdb, NodeID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"})
	if err != nil {
		t.Fatal(err)
	}
	return ms, cs
}

func putVersion(t *testing.T, ms *model.Store, cs *chunkstore.Store, path, content string) {
	t.Helper()
	ctx := context.Background()
	id, err := cs.Put(ctx, chunkstore.FolderContext{}, []byte(content))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	m := manifest.Manifest{Kind: manifest.KindRegular, Path: path, Mode: 0o644, Chunks: []manifest.ChunkRef{{ID: id, Length: int64(len(content))}}}
	if _, err := ms.PutManifest(ctx, m, model.Metadata{Size: int64(len(content))}); err != nil {
		t.Fatalf("PutManifest: %v", err)
	}
}

func hasContent(t *testing.T, cs *chunkstore.Store, content string) bool {
	t.Helper()
	ok, err := cs.Has(context.Background(), hasher.Sum([]byte(content)))
	if err != nil {
		t.Fatalf("Has: %v", err)
	}
	return ok
}

func gcRuntime(t *testing.T) (*syncRuntime, *model.Store, *chunkstore.Store) {
	t.Helper()
	ms, cs := newFolderStores(t)
	rt := &syncRuntime{folders: []syncengine.FolderConfig{{FolderID: "g1", Role: syncengine.RoleReader, Model: ms, Chunks: cs}}}
	return rt, ms, cs
}

func TestSweepChunksReclaimsUnreachable(t *testing.T) {
	ctx := context.Background()
	rt, ms, cs := gcRuntime(t)
	log := slog.New(slog.DiscardHandler)

	putVersion(t, ms, cs, "a.txt", "version one")
	s1, err := ms.Cut(ctx)
	if err != nil {
		t.Fatalf("Cut: %v", err)
	}
	putVersion(t, ms, cs, "a.txt", "version two")
	if _, err := ms.Cut(ctx); err != nil {
		t.Fatalf("Cut: %v", err)
	}
	if err := ms.Forget(ctx, s1); err != nil {
		t.Fatalf("Forget: %v", err)
	}

	rt.sweepChunks(ctx, log, time.Now().Add(2*gc.DefaultGraceAge))
	if hasContent(t, cs, "version one") {
		t.Fatal("unreachable past-grace chunk not reclaimed")
	}
	if !hasContent(t, cs, "version two") {
		t.Fatal("reachable chunk wrongly reclaimed")
	}
}

func TestSweepChunksSkipsWithinGrace(t *testing.T) {
	ctx := context.Background()
	rt, ms, cs := gcRuntime(t)
	log := slog.New(slog.DiscardHandler)

	putVersion(t, ms, cs, "a.txt", "version one")
	s1, err := ms.Cut(ctx)
	if err != nil {
		t.Fatalf("Cut: %v", err)
	}
	putVersion(t, ms, cs, "a.txt", "version two")
	if _, err := ms.Cut(ctx); err != nil {
		t.Fatalf("Cut: %v", err)
	}
	if err := ms.Forget(ctx, s1); err != nil {
		t.Fatalf("Forget: %v", err)
	}

	rt.sweepChunks(ctx, log, time.Now())
	if !hasContent(t, cs, "version one") {
		t.Fatal("within-grace chunk was reclaimed")
	}
}
