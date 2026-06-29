package node

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/GhentiLabs/Trove/client/internal/config"
	"github.com/GhentiLabs/Trove/client/internal/crypto"
	"github.com/GhentiLabs/Trove/client/internal/holder"
	"github.com/GhentiLabs/Trove/client/internal/membership"
	"github.com/GhentiLabs/Trove/client/internal/netio"
	"github.com/GhentiLabs/Trove/client/internal/session"
)

const (
	holderSelfID  = "hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh"
	restorePeerID = "rrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr"
)

func testMasterKey(b byte) [config.MasterKeyLen]byte {
	var k [config.MasterKeyLen]byte
	for i := range k {
		k[i] = b
	}
	return k
}

// sessionPair establishes a MemNet session where the holder offers nothing (as a restore-mode
// authorize would) and peerID advertises shareID with advertised as its encryption verifier (nil
// advertises none). It returns both sides' sessions.
func sessionPair(t *testing.T, ctx context.Context, peerID, shareID string, advertised []byte) (holderSess, peerSess *session.Session) {
	t.Helper()
	mn := netio.NewMemNet()
	ht := mn.Transport("h", holderSelfID)
	pt := mn.Transport("p", peerID)
	allow := func(context.Context, string) ([]string, bool, error) { return nil, true, nil }

	type res struct {
		s   *session.Session
		err error
	}
	ch := make(chan res, 1)
	go func() {
		conn, err := ht.Accept(ctx)
		if err != nil {
			ch <- res{nil, err}
			return
		}
		s, err := session.Handshake(ctx, session.Config{Conn: conn, Initiator: false, Authorize: allow, Local: session.Local{NodeID: holderSelfID}})
		ch <- res{s, err}
	}()

	folder := session.Folder{ShareID: shareID, Encrypted: true, EncryptionVerifier: advertised}
	pc, err := pt.Dial(ctx, "h", holderSelfID)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	ps, err := session.Handshake(ctx, session.Config{
		Conn: pc, Initiator: true,
		Authorize: func(context.Context, string) ([]string, bool, error) { return []string{shareID}, true, nil },
		Local:     session.Local{NodeID: peerID, Folders: []session.Folder{folder}},
	})
	if err != nil {
		t.Fatalf("peer handshake: %v", err)
	}
	r := <-ch
	if r.err != nil {
		t.Fatalf("holder handshake: %v", r.err)
	}
	go func() { _ = r.s.Run(ctx) }()
	go func() { _ = ps.Run(ctx) }()
	t.Cleanup(func() { _ = r.s.Close(); _ = ps.Close() })
	return r.s, ps
}

// restoreSessionPair is sessionPair with a non-member recovering peer.
func restoreSessionPair(t *testing.T, ctx context.Context, shareID string, advertised []byte) (holderSess, peerSess *session.Session) {
	t.Helper()
	return sessionPair(t, ctx, restorePeerID, shareID, advertised)
}

// TestAuthorizeAcceptsNonMemberOnlyForHolder checks the restore relaxation: a holder accepts a
// stranger's session but offers nothing; a non-holder still rejects the stranger.
func TestAuthorizeAcceptsNonMemberOnlyForHolder(t *testing.T) {
	ctx := t.Context()
	members, _, _ := openMembers(t)
	stranger := "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"

	holderSvc := &Service{opts: Options{NodeID: "holder-self"}, members: members, isHolder: true}
	switch granted, ok, err := holderSvc.authorize(ctx, stranger); {
	case err != nil || !ok:
		t.Fatalf("holder authorize(stranger) = ok=%v, err=%v; want true, nil", ok, err)
	case len(granted) != 0:
		t.Fatalf("holder offered %v to a stranger, want nothing", granted)
	}

	plainSvc := &Service{opts: Options{NodeID: "plain-self"}, members: members}
	if _, ok, _ := plainSvc.authorize(ctx, stranger); ok {
		t.Fatal("a non-holder authorized a stranger")
	}
}

// TestHolderStoresForPeerGatesRestore checks a non-member is served a held folder only when the
// verifier it advertised matches the one the holder persisted, and a served session can fetch
// a blob through the gate.
func TestHolderStoresForPeerGatesRestore(t *testing.T) {
	ctx := t.Context()
	log := slog.New(slog.DiscardHandler)
	const shareID = "g"
	key := testMasterKey(0x6e)
	persisted := crypto.FolderVerifier(key, shareID)

	store, err := holder.Open(t.TempDir())
	if err != nil {
		t.Fatalf("holder.Open: %v", err)
	}
	blind := crypto.BlindID(key, []byte("blob"))
	if err := store.Put(blind, []byte("ciphertext")); err != nil {
		t.Fatalf("store.Put: %v", err)
	}

	members, _, _ := openMembers(t)
	cfg := openConfig(t, "holder-self")
	if err := cfg.AddFolder(ctx, config.Folder{ID: shareID, ShareID: shareID, Encrypted: true, Holder: true}); err != nil {
		t.Fatalf("AddFolder: %v", err)
	}
	if err := cfg.SetHolderVerifier(ctx, shareID, persisted); err != nil {
		t.Fatalf("SetHolderVerifier: %v", err)
	}
	rt := &syncRuntime{
		self: "holder-self", members: members, cfg: cfg,
		holders: map[string]*holder.Store{shareID: store},
		byShare: map[string]config.Folder{shareID: {ID: shareID, ShareID: shareID, Encrypted: true, Holder: true}},
	}

	t.Run("correct verifier is served and fetchable", func(t *testing.T) {
		hs, ps := restoreSessionPair(t, ctx, shareID, persisted)
		served := rt.holderStoresForPeer(ctx, log, hs, map[string]bool{}, false)
		if _, ok := served[shareID]; !ok || len(served) != 1 {
			t.Fatalf("served = %v, want only %s", shareKeys(served), shareID)
		}
		go holder.NewServer(served, rt.holderPutAllowed, log).Serve(ctx, hs.Conn())
		got, err := holder.GetBlobOverConn(ps.Conn(), shareID)(ctx, blind)
		if err != nil {
			t.Fatalf("GetBlobOverConn through gate: %v", err)
		}
		if !bytes.Equal(got, []byte("ciphertext")) {
			t.Fatalf("fetched blob = %q, want %q", got, "ciphertext")
		}
	})

	t.Run("wrong verifier is not served", func(t *testing.T) {
		hs, _ := restoreSessionPair(t, ctx, shareID, bytes.Repeat([]byte{0x01}, crypto.VerifierLen))
		if served := rt.holderStoresForPeer(ctx, log, hs, map[string]bool{}, false); len(served) != 0 {
			t.Fatalf("wrong verifier served %v, want nothing", shareKeys(served))
		}
	})

	t.Run("absent verifier is not served", func(t *testing.T) {
		hs, _ := restoreSessionPair(t, ctx, shareID, nil)
		if served := rt.holderStoresForPeer(ctx, log, hs, map[string]bool{}, false); len(served) != 0 {
			t.Fatalf("absent verifier served %v, want nothing", shareKeys(served))
		}
	})

	t.Run("wrong-length verifier is not served", func(t *testing.T) {
		hs, _ := restoreSessionPair(t, ctx, shareID, []byte{0x01, 0x02, 0x03})
		if served := rt.holderStoresForPeer(ctx, log, hs, map[string]bool{}, false); len(served) != 0 {
			t.Fatalf("wrong-length verifier served %v, want nothing", shareKeys(served))
		}
	})

	t.Run("member is served by shared, not the verifier", func(t *testing.T) {
		hs, _ := restoreSessionPair(t, ctx, shareID, persisted)
		if served := rt.holderStoresForPeer(ctx, log, hs, map[string]bool{shareID: true}, true); len(served) != 1 {
			t.Fatalf("member with shared folder served %v, want [%s]", shareKeys(served), shareID)
		}
		if served := rt.holderStoresForPeer(ctx, log, hs, map[string]bool{}, true); len(served) != 0 {
			t.Fatalf("member without shared folder served %v, want nothing", shareKeys(served))
		}
	})
}

// TestPersistHolderVerifiers checks a holder records a roster member's advertised verifier but
// never a non-member's — the bootstrap that makes a later restore possible without poisoning.
func TestPersistHolderVerifiers(t *testing.T) {
	ctx := t.Context()
	log := slog.New(slog.DiscardHandler)
	members, founderID, _ := openMembers(t)
	group, err := members.Found(ctx)
	if err != nil {
		t.Fatalf("Found: %v", err)
	}
	verifier := crypto.FolderVerifier(testMasterKey(0x6e), group)

	store, err := holder.Open(t.TempDir())
	if err != nil {
		t.Fatalf("holder.Open: %v", err)
	}
	cfg := openConfig(t, "holder-self")
	if err := cfg.AddFolder(ctx, config.Folder{ID: group, ShareID: group, Encrypted: true, Holder: true}); err != nil {
		t.Fatalf("AddFolder: %v", err)
	}
	rt := &syncRuntime{
		self: "holder-self", members: members, cfg: cfg,
		holders: map[string]*holder.Store{group: store},
		byShare: map[string]config.Folder{group: {ID: group, ShareID: group, Encrypted: true, Holder: true}},
	}
	served := map[string]*holder.Store{group: store}

	// The founder is a roster writer: its advertised verifier is persisted.
	hs, _ := sessionPair(t, ctx, founderID, group, verifier)
	rt.persistHolderVerifiers(ctx, log, hs, served)
	switch got, err := cfg.GetHolderVerifier(ctx, group); {
	case err != nil:
		t.Fatalf("GetHolderVerifier: %v", err)
	case !bytes.Equal(got, verifier):
		t.Fatalf("persisted verifier = %x, want %x", got, verifier)
	}

	// A non-member advertising a token cannot poison the stored verifier.
	if err := cfg.SetHolderVerifier(ctx, group, nil); err != nil {
		t.Fatalf("reset verifier: %v", err)
	}
	hs2, _ := sessionPair(t, ctx, restorePeerID, group, []byte("attacker-token"))
	rt.persistHolderVerifiers(ctx, log, hs2, served)
	if got, _ := cfg.GetHolderVerifier(ctx, group); got != nil {
		t.Fatalf("non-member poisoned the verifier: %x", got)
	}
}

// TestPeerIsMember checks the member/non-member classification that gates gossip and the holder
// serve path: the founder and an added member are members; a stranger is not.
func TestPeerIsMember(t *testing.T) {
	ctx := t.Context()
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
		name string
		id   string
		want bool
	}{
		{"founder is a member", founderID, true},
		{"added reader is a member", readerID, true},
		{"stranger is not", "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := rt.peerIsMember(ctx, tc.id)
			if err != nil || got != tc.want {
				t.Fatalf("peerIsMember(%s) = %v, err %v; want %v", tc.id, got, err, tc.want)
			}
		})
	}
}

func shareKeys(m map[string]*holder.Store) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
