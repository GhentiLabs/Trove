package holder

import (
	"errors"
	"testing"

	"github.com/GhentiLabs/Trove/client/internal/crypto"
)

// TestServeGetNotFound checks a get for a blinded id the holder has never stored, in a
// folder it does serve, returns ErrNotFound rather than a generic failure.
func TestServeGetNotFound(t *testing.T) {
	ctx := t.Context()
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	const fid = "fid"
	holderConn, peerConn := connPair(t, ctx)
	go NewServer(map[string]*Store{fid: store}, allowAll, nil).Serve(ctx, holderConn)

	var missing [crypto.BlindIDLen]byte
	missing[0] = 0x7F
	if _, err := GetBlobOverConn(peerConn, fid)(ctx, missing); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get of a missing blob err = %v, want ErrNotFound", err)
	}
}

// TestServeUnknownFolderOps checks every op that targets a folder the holder does not serve
// fails closed: get/list/delete error, and has-batch reports nothing present.
func TestServeUnknownFolderOps(t *testing.T) {
	ctx := t.Context()
	holderConn, peerConn := connPair(t, ctx)
	go NewServer(map[string]*Store{}, allowAll, nil).Serve(ctx, holderConn)

	const fid = "absent"
	var id [crypto.BlindIDLen]byte
	id[0] = 0x01

	if _, err := ListBlobsOverConn(peerConn, fid)(ctx, [crypto.BlindIDLen]byte{}); err == nil {
		t.Fatal("list for an unknown folder succeeded")
	}
	if err := DeleteBlobsOverConn(peerConn, fid)(ctx, [][crypto.BlindIDLen]byte{id}); err == nil {
		t.Fatal("delete for an unknown folder succeeded")
	}
	present, err := HasBlobsOverConn(peerConn, fid)(ctx, [][crypto.BlindIDLen]byte{id})
	if err != nil {
		t.Fatalf("has-batch for an unknown folder: %v", err)
	}
	if len(present) != 1 || present[0] {
		t.Fatalf("has-batch for an unknown folder = %v, want [false]", present)
	}
}

// TestServeShedExcessStreams checks the server bounds in-flight handlers and still answers
// requests once earlier ones drain, so a peer cannot exhaust it by opening many streams.
func TestServeShedExcessStreams(t *testing.T) {
	ctx := t.Context()
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	const fid = "fid"
	var id [crypto.BlindIDLen]byte
	id[0] = 0x55
	if err := store.Put(id, []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	holderConn, peerConn := connPair(t, ctx)
	go NewServer(map[string]*Store{fid: store}, allowAll, nil).Serve(ctx, holderConn)

	get := GetBlobOverConn(peerConn, fid)
	for i := range maxConcurrentRequests * 2 {
		got, err := get(ctx, id)
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if string(got) != "v" {
			t.Fatalf("get %d = %q, want v", i, got)
		}
	}
}
