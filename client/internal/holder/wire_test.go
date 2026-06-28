package holder

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/GhentiLabs/Trove/client/internal/crypto"
	"github.com/GhentiLabs/Trove/client/internal/netio"
)

func TestRequestGoldenLayout(t *testing.T) {
	var blinded [crypto.BlindIDLen]byte
	for i := range blinded {
		blinded[i] = byte(i)
	}
	var buf bytes.Buffer
	if err := writeRequest(&buf, opGet, "fid", blinded, nil); err != nil {
		t.Fatalf("writeRequest: %v", err)
	}
	got := buf.Bytes()
	want := []byte{0x54, 0x48, 0x4C, 0x44, 0x01, 0x01, 0x00, 0x03, 'f', 'i', 'd'}
	want = append(want, blinded[:]...)
	if !bytes.Equal(got, want) {
		t.Fatalf("get request layout:\n got %x\nwant %x", got, want)
	}

	buf.Reset()
	if err := writeRequest(&buf, opPut, "fid", blinded, []byte("xy")); err != nil {
		t.Fatalf("writeRequest put: %v", err)
	}
	got = buf.Bytes()
	tail := got[len(got)-6:]
	if !bytes.Equal(tail, []byte{0x00, 0x00, 0x00, 0x02, 'x', 'y'}) {
		t.Fatalf("put payload framing = %x", tail)
	}
}

func TestResponseGoldenLayout(t *testing.T) {
	var buf bytes.Buffer
	if err := writeResponse(&buf, StatusNotFound, nil); err != nil {
		t.Fatalf("writeResponse: %v", err)
	}
	want := []byte{0x54, 0x48, 0x4C, 0x44, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("response layout:\n got %x\nwant %x", buf.Bytes(), want)
	}
}

func TestRequestResponseRoundTrip(t *testing.T) {
	var blinded [crypto.BlindIDLen]byte
	blinded[1] = 0x9
	payload := []byte("sealed bytes")

	var buf bytes.Buffer
	if err := writeRequest(&buf, opPut, "folder-x", blinded, payload); err != nil {
		t.Fatalf("writeRequest: %v", err)
	}
	op, fid, err := readRequestHeader(&buf)
	if err != nil {
		t.Fatalf("readRequestHeader: %v", err)
	}
	gotBlinded, err := readBlinded(&buf)
	if err != nil {
		t.Fatalf("readBlinded: %v", err)
	}
	gotPayload, err := readPayload(&buf)
	if err != nil {
		t.Fatalf("readPayload: %v", err)
	}
	if op != opPut || fid != "folder-x" || gotBlinded != blinded || !bytes.Equal(gotPayload, payload) {
		t.Fatalf("round-trip mismatch: op=%d fid=%q payload=%q", op, fid, gotPayload)
	}

	buf.Reset()
	if err := writeResponse(&buf, StatusOK, payload); err != nil {
		t.Fatalf("writeResponse: %v", err)
	}
	status, gotResp, err := readResponse(&buf)
	if err != nil {
		t.Fatalf("readResponse: %v", err)
	}
	if status != StatusOK || !bytes.Equal(gotResp, payload) {
		t.Fatalf("response round-trip: status=%d payload=%q", status, gotResp)
	}
}

func TestHasBatchGoldenLayout(t *testing.T) {
	var a, b [crypto.BlindIDLen]byte
	a[0], b[0] = 0x11, 0x22
	var buf bytes.Buffer
	if err := writeBlindedList(&buf, opHasBatch, "fid", [][crypto.BlindIDLen]byte{a, b}); err != nil {
		t.Fatalf("writeBlindedList: %v", err)
	}
	want := []byte{0x54, 0x48, 0x4C, 0x44, 0x01, 0x04, 0x00, 0x03, 'f', 'i', 'd', 0x00, 0x02}
	want = append(want, a[:]...)
	want = append(want, b[:]...)
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("has-batch layout:\n got %x\nwant %x", buf.Bytes(), want)
	}

	op, fid, err := readRequestHeader(&buf)
	if err != nil || op != opHasBatch || fid != "fid" {
		t.Fatalf("readRequestHeader op=%d fid=%q err=%v", op, fid, err)
	}
	ids, err := readBlindedList(&buf)
	if err != nil || len(ids) != 2 || ids[0] != a || ids[1] != b {
		t.Fatalf("readBlindedList = %v err=%v", ids, err)
	}

	buf.Reset()
	if err := writeBitmapResponse(&buf, []bool{true, false, true}); err != nil {
		t.Fatalf("writeBitmapResponse: %v", err)
	}
	present, err := readBitmapResponse(&buf, 3)
	if err != nil {
		t.Fatalf("readBitmapResponse: %v", err)
	}
	if !present[0] || present[1] || !present[2] {
		t.Fatalf("bitmap round-trip = %v, want [true false true]", present)
	}
}

func TestReadRequestRejectsBadMagic(t *testing.T) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf, 0xDEADBEEF)
	if _, _, err := readRequestHeader(bytes.NewReader(buf)); err == nil {
		t.Fatal("readRequestHeader accepted bad magic")
	}
}

func allowAll(context.Context, string, string) (bool, error) { return true, nil }

// TestServeExportRestoreOverConn drives Export and Restore through the live holder wire
// protocol over a MemNet connection: a writer pushes blinded blobs to a holder server,
// then a member restores the folder bit-exact by fetching them back.
func TestServeExportRestoreOverConn(t *testing.T) {
	ctx := t.Context()
	key := testKey(0x6E)

	src := newFolder(t, key)
	writeFile(t, src.root, "a.txt", []byte("hello"))
	writeFile(t, src.root, "sub/b.bin", pseudoRandom(3<<20, 5))
	src.scan(t)

	const fid = "group-fid"
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("holder Open: %v", err)
	}
	holderConn, peerConn := connPair(t, ctx)
	srv := NewServer(map[string]*Store{fid: store}, allowAll, nil)
	go srv.Serve(ctx, holderConn)

	if err := Reconcile(ctx, key, src.model, src.chunks, src.fc, HasBlobsOverConn(peerConn, fid), PutBlobOverConn(peerConn, fid)); err != nil {
		t.Fatalf("Export over conn: %v", err)
	}
	dst := newFolder(t, key)
	if err := Restore(ctx, key, dst.chunks, dst.fc, dst.root, GetBlobOverConn(peerConn, fid)); err != nil {
		t.Fatalf("Restore over conn: %v", err)
	}
	assertTreesEqual(t, src.root, dst.root)
}

// TestServeRejectsUnauthorizedPut checks a holder refuses to store a blob from a peer that
// allowPut denies (a reader, or an authorization error), failing closed.
func TestServeRejectsUnauthorizedPut(t *testing.T) {
	cases := []struct {
		name     string
		allowPut func(context.Context, string, string) (bool, error)
	}{
		{"denied", func(context.Context, string, string) (bool, error) { return false, nil }},
		{"error", func(context.Context, string, string) (bool, error) { return false, errBadOp }},
		{"nil callback", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			store, err := Open(t.TempDir())
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			const fid = "fid"
			holderConn, peerConn := connPair(t, ctx)
			srv := NewServer(map[string]*Store{fid: store}, tc.allowPut, nil)
			go srv.Serve(ctx, holderConn)

			var blinded [crypto.BlindIDLen]byte
			blinded[0] = 0xAB
			err = PutBlobOverConn(peerConn, fid)(ctx, blinded, []byte("ciphertext"))
			if err == nil {
				t.Fatal("unauthorized put succeeded")
			}
			if store.Has(blinded) {
				t.Fatal("unauthorized put was stored")
			}
		})
	}
}

// TestServeUnknownFolder checks a get for a folder the holder does not serve returns an error.
func TestServeUnknownFolder(t *testing.T) {
	ctx := t.Context()
	holderConn, peerConn := connPair(t, ctx)
	srv := NewServer(map[string]*Store{}, allowAll, nil)
	go srv.Serve(ctx, holderConn)

	var b [crypto.BlindIDLen]byte
	if _, err := GetBlobOverConn(peerConn, "unknown")(ctx, b); err == nil {
		t.Fatal("get for an unknown folder succeeded")
	}
}

// TestServeListDelete exercises the list and delete ops over the wire, including that a
// delete from an unauthorized peer is refused.
func TestServeListDelete(t *testing.T) {
	ctx := t.Context()
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	var a, b [crypto.BlindIDLen]byte
	a[0], b[0] = 0x01, 0x02
	_ = store.Put(a, []byte("x"))
	_ = store.Put(b, []byte("y"))

	const fid = "f"
	holderConn, peerConn := connPair(t, ctx)
	srv := NewServer(map[string]*Store{fid: store}, allowAll, nil)
	go srv.Serve(ctx, holderConn)

	var zero [crypto.BlindIDLen]byte
	refs, err := ListBlobsOverConn(peerConn, fid)(ctx, zero)
	if err != nil || len(refs) != 2 {
		t.Fatalf("list = %d refs err=%v, want 2", len(refs), err)
	}
	if err := DeleteBlobsOverConn(peerConn, fid)(ctx, [][crypto.BlindIDLen]byte{a}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if store.Has(a) {
		t.Fatal("blob not deleted")
	}

	denyConn1, denyConn2 := connPair(t, ctx)
	denySrv := NewServer(map[string]*Store{fid: store}, func(context.Context, string, string) (bool, error) { return false, nil }, nil)
	go denySrv.Serve(ctx, denyConn1)
	if err := DeleteBlobsOverConn(denyConn2, fid)(ctx, [][crypto.BlindIDLen]byte{b}); err == nil {
		t.Fatal("unauthorized delete succeeded")
	}
	if !store.Has(b) {
		t.Fatal("unauthorized delete removed a blob")
	}
}

func connPair(t *testing.T, ctx context.Context) (a, b netio.Conn) {
	t.Helper()
	mn := netio.NewMemNet()
	ta := mn.Transport("a", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	tb := mn.Transport("b", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	ch := make(chan netio.Conn, 1)
	go func() {
		c, err := ta.Accept(ctx)
		if err != nil {
			t.Errorf("Accept: %v", err)
		}
		ch <- c
	}()
	bc, err := tb.Dial(ctx, "a", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	return <-ch, bc
}
