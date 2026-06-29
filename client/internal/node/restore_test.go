package node

import (
	"bytes"
	"context"
	"crypto/subtle"
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

// TestAuthorizeAcceptsNonMemberWhenServing checks a node that serves any folder accepts a
// stranger's session but grants nothing; a node that serves nothing rejects the stranger.
func TestAuthorizeAcceptsNonMemberWhenServing(t *testing.T) {
	ctx := t.Context()
	members, _, _ := openMembers(t)
	stranger := "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"

	serving := &Service{opts: Options{NodeID: "serving-self"}, members: members, serves: true}
	switch granted, ok, err := serving.authorize(ctx, stranger); {
	case err != nil || !ok:
		t.Fatalf("serving authorize(stranger) = ok=%v, err=%v; want true, nil", ok, err)
	case len(granted) != 0:
		t.Fatalf("serving node offered %v to a stranger, want nothing", granted)
	}

	idle := &Service{opts: Options{NodeID: "idle-self"}, members: members}
	if _, ok, _ := idle.authorize(ctx, stranger); ok {
		t.Fatal("a node that serves nothing authorized a stranger")
	}
}

// TestResponsiveOfferGatesByVerifier checks a recovery peer is offered a folder only when its
// advertised verifier constant-time-matches this node's, for both a held folder (verifier from
// the persisted token) and a member folder (verifier from the folder secret).
func TestResponsiveOfferGatesByVerifier(t *testing.T) {
	ctx := t.Context()
	members, _, _ := openMembers(t)
	key := testMasterKey(0x6e)

	cfg := openConfig(t, "self")
	if err := cfg.AddFolder(ctx, config.Folder{ID: "held", ShareID: "held", Encrypted: true, Holder: true}); err != nil {
		t.Fatalf("AddFolder held: %v", err)
	}
	if err := cfg.SetHolderVerifier(ctx, "held", crypto.FolderVerifier(key, "held")); err != nil {
		t.Fatalf("SetHolderVerifier: %v", err)
	}
	if err := cfg.AddFolder(ctx, config.Folder{ID: "mem", Root: "/m", ShareID: "mem", Encrypted: true}); err != nil {
		t.Fatalf("AddFolder mem: %v", err)
	}
	if _, err := cfg.GenerateFolderKey(ctx, "mem"); err != nil {
		t.Fatalf("GenerateFolderKey: %v", err)
	}
	memKey, _, _ := cfg.GetFolderKey(ctx, "mem")

	rt := &syncRuntime{
		self: "self", members: members, cfg: cfg, log: slog.New(slog.DiscardHandler),
		byShare: map[string]config.Folder{
			"held": {ID: "held", ShareID: "held", Encrypted: true, Holder: true},
			"mem":  {ID: "mem", ShareID: "mem", Encrypted: true},
		},
	}

	cases := []struct {
		name      string
		shareID   string
		verifier  []byte
		wantHeld  bool
		wantShare bool
	}{
		{"held match", "held", crypto.FolderVerifier(key, "held"), true, true},
		{"held wrong", "held", bytes.Repeat([]byte{0x01}, crypto.VerifierLen), false, false},
		{"held absent", "held", nil, false, false},
		{"held short", "held", []byte{0x01, 0x02}, false, false},
		{"member match", "mem", crypto.FolderVerifier(memKey, "mem"), false, true},
		{"member wrong", "mem", bytes.Repeat([]byte{0x02}, crypto.VerifierLen), false, false},
		{"unknown folder", "nope", crypto.FolderVerifier(key, "nope"), false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			peer := []session.Folder{{ShareID: tc.shareID, Encrypted: true, EncryptionVerifier: tc.verifier}}
			out, err := rt.responsiveOffer(ctx, "peer", peer)
			if err != nil {
				t.Fatalf("responsiveOffer: %v", err)
			}
			if !tc.wantShare {
				if len(out) != 0 {
					t.Fatalf("offered %+v on a bad/absent verifier, want nothing", out)
				}
				return
			}
			if len(out) != 1 || out[0].ShareID != tc.shareID || out[0].Holder != tc.wantHeld {
				t.Fatalf("offered = %+v, want %s (held=%v)", out, tc.shareID, tc.wantHeld)
			}
			if subtle.ConstantTimeCompare(out[0].EncryptionVerifier, tc.verifier) != 1 {
				t.Fatalf("offered verifier does not match the peer's")
			}
		})
	}
}

// TestHolderStoresForPeer checks the holder serves exactly the held folders shared on a session.
func TestHolderStoresForPeer(t *testing.T) {
	store, err := holder.Open(t.TempDir())
	if err != nil {
		t.Fatalf("holder.Open: %v", err)
	}
	rt := &syncRuntime{holders: map[string]*holder.Store{"a": store, "b": store}}
	served := rt.holderStoresForPeer(map[string]bool{"a": true, "c": true})
	if len(served) != 1 || served["a"] == nil {
		t.Fatalf("served = %v, want only a", served)
	}
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
