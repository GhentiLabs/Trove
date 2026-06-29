package node

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/GhentiLabs/Trove/client/internal/config"
	"github.com/GhentiLabs/Trove/client/internal/crypto"
	"github.com/GhentiLabs/Trove/client/internal/holder"
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

// restoreSessionPair establishes a MemNet session where the holder offers nothing (as a
// restore-mode authorize would) and the recovering peer advertises shareID with advertised
// as its encryption verifier (nil advertises none). It returns both sides' sessions.
func restoreSessionPair(t *testing.T, ctx context.Context, shareID string, advertised []byte) (holderSess, peerSess *session.Session) {
	t.Helper()
	mn := netio.NewMemNet()
	ht := mn.Transport("h", holderSelfID)
	pt := mn.Transport("p", restorePeerID)
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
		Local:     session.Local{NodeID: restorePeerID, Folders: []session.Folder{folder}},
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

// TestAuthorizeAcceptsNonMemberOnlyForHolder checks the restore relaxation: a holder accepts a
// stranger's session but offers nothing; a non-holder still rejects the stranger.
func TestAuthorizeAcceptsNonMemberOnlyForHolder(t *testing.T) {
	ctx := context.Background()
	members, _, _ := openMembers(t)
	stranger := "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"

	holderCfg := openConfig(t, "holder-self")
	if err := holderCfg.AddFolder(ctx, config.Folder{ID: "g", ShareID: "g", Encrypted: true, Holder: true}); err != nil {
		t.Fatalf("AddFolder holder: %v", err)
	}
	holderSvc := &Service{opts: Options{NodeID: "holder-self", Config: holderCfg}, members: members}
	switch granted, ok, err := holderSvc.authorize(ctx, stranger); {
	case err != nil || !ok:
		t.Fatalf("holder authorize(stranger) = ok %v err %v, want true, nil", ok, err)
	case len(granted) != 0:
		t.Fatalf("holder offered %v to a stranger, want nothing", granted)
	}

	plainCfg := openConfig(t, "plain-self")
	if err := plainCfg.AddFolder(ctx, config.Folder{ID: "p", Root: "/p", ShareID: "p"}); err != nil {
		t.Fatalf("AddFolder plain: %v", err)
	}
	plainSvc := &Service{opts: Options{NodeID: "plain-self", Config: plainCfg}, members: members}
	if _, ok, _ := plainSvc.authorize(ctx, stranger); ok {
		t.Fatal("a non-holder authorized a stranger")
	}
}

// TestHolderServeSetGatesRestore checks a non-member is served a held folder only when the
// verifier it advertised matches the one the holder persisted, and a served session can fetch
// a blob through the gate.
func TestHolderServeSetGatesRestore(t *testing.T) {
	ctx := context.Background()
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
		served := rt.holderServeSet(ctx, log, hs, map[string]bool{}, false)
		if _, ok := served[shareID]; !ok || len(served) != 1 {
			t.Fatalf("served = %v, want only %s", keys(served), shareID)
		}
		go holder.NewServer(served, rt.holderPutAllowed, log).Serve(ctx, hs.Conn())
		got, err := holder.GetBlobOverConn(ps.Conn(), shareID)(ctx, blind)
		if err != nil || !bytes.Equal(got, []byte("ciphertext")) {
			t.Fatalf("fetch through gate = %q err %v, want ciphertext", got, err)
		}
	})

	t.Run("wrong verifier is not served", func(t *testing.T) {
		hs, _ := restoreSessionPair(t, ctx, shareID, bytes.Repeat([]byte{0x01}, crypto.VerifierLen))
		if served := rt.holderServeSet(ctx, log, hs, map[string]bool{}, false); len(served) != 0 {
			t.Fatalf("wrong verifier served %v, want nothing", keys(served))
		}
	})

	t.Run("absent verifier is not served", func(t *testing.T) {
		hs, _ := restoreSessionPair(t, ctx, shareID, nil)
		if served := rt.holderServeSet(ctx, log, hs, map[string]bool{}, false); len(served) != 0 {
			t.Fatalf("absent verifier served %v, want nothing", keys(served))
		}
	})

	t.Run("member is served by shared, not the verifier", func(t *testing.T) {
		hs, _ := restoreSessionPair(t, ctx, shareID, persisted)
		if served := rt.holderServeSet(ctx, log, hs, map[string]bool{shareID: true}, true); len(served) != 1 {
			t.Fatalf("member with shared folder served %v, want it", keys(served))
		}
		if served := rt.holderServeSet(ctx, log, hs, map[string]bool{}, true); len(served) != 0 {
			t.Fatalf("member without shared folder served %v, want nothing", keys(served))
		}
	})
}

func keys(m map[string]*holder.Store) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
