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
	var blinded [crypto.BlindLen]byte
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
	var blinded [crypto.BlindLen]byte
	blinded[1] = 0x9
	payload := []byte("sealed bytes")

	var buf bytes.Buffer
	if err := writeRequest(&buf, opPut, "folder-x", blinded, payload); err != nil {
		t.Fatalf("writeRequest: %v", err)
	}
	op, fid, gotBlinded, gotPayload, err := readRequest(&buf)
	if err != nil {
		t.Fatalf("readRequest: %v", err)
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

func TestReadRequestRejectsBadMagic(t *testing.T) {
	buf := make([]byte, 12)
	binary.BigEndian.PutUint32(buf, 0xDEADBEEF)
	if _, _, _, _, err := readRequest(bytes.NewReader(buf)); err == nil {
		t.Fatal("readRequest accepted bad magic")
	}
}

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
	srv := NewServer(map[string]*Store{fid: store}, nil, nil)
	go srv.Serve(ctx, holderConn)

	if err := Export(ctx, key, src.model, src.chunks, src.fc, PutBlobOverConn(peerConn, fid)); err != nil {
		t.Fatalf("Export over conn: %v", err)
	}
	dst := newFolder(t, key)
	if err := Restore(ctx, key, dst.chunks, dst.fc, dst.root, GetBlobOverConn(peerConn, fid)); err != nil {
		t.Fatalf("Restore over conn: %v", err)
	}
	assertTreesEqual(t, src.root, dst.root)
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
