package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net"
	"testing"

	"github.com/GhentiLabs/Trove/client/internal/compression"
	"github.com/GhentiLabs/Trove/client/internal/wire/wirepb"
	"google.golang.org/protobuf/proto"
)

// Golden check on the frozen wire constants; changing any is a wire-format break.
func TestFrozenConstantsGolden(t *testing.T) {
	if Magic != 0x54524F56 {
		t.Fatalf("Magic = %#x, want 0x54524F56", Magic)
	}
	if WireFormatVersion != 1 {
		t.Fatalf("WireFormatVersion = %d, want 1", WireFormatVersion)
	}
	if MaxMessageSize != 64<<20 {
		t.Fatalf("MaxMessageSize = %d, want %d", MaxMessageSize, 64<<20)
	}
	if MaxControlMessageSize != 1<<20 {
		t.Fatalf("MaxControlMessageSize = %d, want %d", MaxControlMessageSize, 1<<20)
	}
	if TypeNetworkConfig != 1 || TypePing != 2 || TypeClose != 3 {
		t.Fatalf("message type values drifted: nc=%d ping=%d close=%d", TypeNetworkConfig, TypePing, TypeClose)
	}
	if TypeFolderSummary != 4 || TypeManifestRequest != 5 || TypeManifestDelta != 6 || TypeMembershipGossip != 7 {
		t.Fatalf("M4 message type values drifted: summary=%d req=%d delta=%d gossip=%d",
			TypeFolderSummary, TypeManifestRequest, TypeManifestDelta, TypeMembershipGossip)
	}
	if wirepb.FolderType_FOLDER_TYPE_SEND_RECEIVE != 0 ||
		wirepb.FolderType_FOLDER_TYPE_SEND_ONLY != 1 ||
		wirepb.FolderType_FOLDER_TYPE_RECEIVE_ONLY != 2 ||
		wirepb.FolderType_FOLDER_TYPE_RECEIVE_ENCRYPTED != 3 {
		t.Fatalf("FolderType wire values drifted: sr=%d so=%d ro=%d re=%d",
			wirepb.FolderType_FOLDER_TYPE_SEND_RECEIVE, wirepb.FolderType_FOLDER_TYPE_SEND_ONLY,
			wirepb.FolderType_FOLDER_TYPE_RECEIVE_ONLY, wirepb.FolderType_FOLDER_TYPE_RECEIVE_ENCRYPTED)
	}
}

// Golden: the Hello envelope layout (magic ‖ uint16 size ‖ body) is byte-pinned.
func TestHelloEnvelopeGolden(t *testing.T) {
	h := &wirepb.Hello{NodeId: "n", WireFormatVersion: WireFormatVersion, Name: "alice"}
	var buf bytes.Buffer
	if err := WriteHello(&buf, h); err != nil {
		t.Fatalf("WriteHello: %v", err)
	}
	b := buf.Bytes()
	if len(b) < 6 {
		t.Fatalf("frame too short: %d bytes", len(b))
	}
	if got := binary.BigEndian.Uint32(b[0:4]); got != Magic {
		t.Fatalf("magic on wire = %#x, want %#x", got, Magic)
	}
	size := binary.BigEndian.Uint16(b[4:6])
	if int(size) != len(b)-6 {
		t.Fatalf("size field = %d, body is %d bytes", size, len(b)-6)
	}

	got, err := ReadHello(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("ReadHello: %v", err)
	}
	if !proto.Equal(got, h) {
		t.Fatalf("round-trip mismatch: got %+v want %+v", got, h)
	}
}

func TestReadHelloBadMagic(t *testing.T) {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, uint32(0xDEADBEEF))
	_ = binary.Write(&buf, binary.BigEndian, uint16(0))
	if _, err := ReadHello(&buf); !errors.Is(err, ErrBadMagic) {
		t.Fatalf("ReadHello bad magic err = %v, want ErrBadMagic", err)
	}
}

// Golden: the post-Hello frame layout (uint16 header_len ‖ Header ‖ uint32 msg_len ‖
// body) is byte-pinned.
func TestMessageEnvelopeGolden(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteMessage(&buf, &wirepb.Ping{}); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}
	b := buf.Bytes()

	hlen := binary.BigEndian.Uint16(b[0:2])
	var hdr wirepb.Header
	if err := proto.Unmarshal(b[2:2+hlen], &hdr); err != nil {
		t.Fatalf("unmarshal header: %v", err)
	}
	if hdr.GetType() != uint32(TypePing) {
		t.Fatalf("header type = %d, want %d", hdr.GetType(), TypePing)
	}
	off := 2 + int(hlen)
	mlen := binary.BigEndian.Uint32(b[off : off+4])
	if int(mlen) != len(b)-off-4 {
		t.Fatalf("msg_len = %d, body is %d bytes", mlen, len(b)-off-4)
	}

	typ, _, err := ReadMessage(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("ReadMessage ping: %v", err)
	}
	if typ != TypePing {
		t.Fatalf("read type = %d, want TypePing", typ)
	}
}

func TestMessageRoundTrip(t *testing.T) {
	cfg := &wirepb.NetworkConfig{
		Folders: []*wirepb.Folder{
			{FolderId: "docs-share", FolderType: wirepb.FolderType_FOLDER_TYPE_SEND_RECEIVE},
			{FolderId: "photos-share", Encrypted: true},
		},
	}
	var buf bytes.Buffer
	if err := WriteMessage(&buf, cfg); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}
	typ, msg, err := ReadMessage(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if typ != TypeNetworkConfig {
		t.Fatalf("type = %d, want %d", typ, TypeNetworkConfig)
	}
	if !proto.Equal(msg, cfg) {
		t.Fatalf("round-trip mismatch:\n got %+v\nwant %+v", msg, cfg)
	}
}

func TestSyncMessageRoundTrip(t *testing.T) {
	root := bytes.Repeat([]byte{0xAB}, 32)
	cid := bytes.Repeat([]byte{0xCD}, 32)
	mid := bytes.Repeat([]byte{0xEF}, 32)
	cases := []proto.Message{
		&wirepb.FolderSummary{FolderId: "docs", SnapshotRoot: root, IndexEpochId: 7, HighWaterSequence: 42},
		&wirepb.ManifestRequest{FolderId: "docs", IndexEpochId: 7, SinceSequence: 12},
		&wirepb.ManifestDelta{
			FolderId: "docs", IndexEpochId: 7, HighWaterSequence: 42,
			Manifests: []*wirepb.RemoteManifest{{
				Kind: 0, Path: "a/b.txt", Mode: 0o644,
				Chunks:        []*wirepb.ChunkRef{{ChunkId: cid, Length: 1024}},
				ManifestId:    mid,
				VersionVector: []byte{0x01, 0x02},
				Author:        "node-a",
				AuthoredMs:    1700000000000,
			}},
		},
	}
	gossip := &wirepb.MembershipGossip{
		NetworkId: "net",
		Entries: []*wirepb.MembershipEntry{{
			NetworkId: "net", NodeId: "n", PublicKey: bytes.Repeat([]byte{0x11}, 32),
			Role: 1, AddedBy: "n", AddedAtMs: 7, Sig: bytes.Repeat([]byte{0x22}, 64),
		}},
	}
	cases = append(cases, gossip)
	wantTypes := []MessageType{TypeFolderSummary, TypeManifestRequest, TypeManifestDelta, TypeMembershipGossip}
	for i, msg := range cases {
		var buf bytes.Buffer
		if err := WriteMessage(&buf, msg); err != nil {
			t.Fatalf("WriteMessage %T: %v", msg, err)
		}
		typ, got, err := ReadMessage(bytes.NewReader(buf.Bytes()))
		if err != nil {
			t.Fatalf("ReadMessage %T: %v", msg, err)
		}
		if typ != wantTypes[i] {
			t.Fatalf("%T type = %d, want %d", msg, typ, wantTypes[i])
		}
		if !proto.Equal(got, msg) {
			t.Fatalf("%T round-trip mismatch:\n got %+v\nwant %+v", msg, got, msg)
		}
	}
}

func TestWriteMessageUnknownType(t *testing.T) {
	if err := WriteMessage(&bytes.Buffer{}, &wirepb.Header{}); !errors.Is(err, ErrUnknownType) {
		t.Fatalf("WriteMessage(Header) err = %v, want ErrUnknownType", err)
	}
}

func TestReadMessageTooLarge(t *testing.T) {
	var buf bytes.Buffer
	hb, _ := proto.Marshal(&wirepb.Header{Type: uint32(TypePing)})
	_ = binary.Write(&buf, binary.BigEndian, uint16(len(hb)))
	buf.Write(hb)
	_ = binary.Write(&buf, binary.BigEndian, uint32(MaxMessageSize+1))
	if _, _, err := ReadMessage(&buf); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("ReadMessage oversize err = %v, want ErrMessageTooLarge", err)
	}
}

func TestReadControlMessageTooLarge(t *testing.T) {
	frame := func() *bytes.Buffer {
		var buf bytes.Buffer
		hb, _ := proto.Marshal(&wirepb.Header{Type: uint32(TypePing)})
		_ = binary.Write(&buf, binary.BigEndian, uint16(len(hb)))
		buf.Write(hb)
		_ = binary.Write(&buf, binary.BigEndian, uint32(MaxControlMessageSize+1))
		return &buf
	}
	if _, _, err := ReadControlMessage(frame()); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("ReadControlMessage oversize err = %v, want ErrMessageTooLarge", err)
	}
	// The same length is within the wire cap, so the full reader rejects it for a
	// missing body, never on size.
	if _, _, err := ReadMessage(frame()); errors.Is(err, ErrMessageTooLarge) {
		t.Fatal("ReadMessage rejected a control-oversize frame on size; want a non-size error")
	}
}

func TestReadMessageUnknownType(t *testing.T) {
	var buf bytes.Buffer
	hb, _ := proto.Marshal(&wirepb.Header{Type: 9999})
	_ = binary.Write(&buf, binary.BigEndian, uint16(len(hb)))
	buf.Write(hb)
	_ = binary.Write(&buf, binary.BigEndian, uint32(0))
	if _, _, err := ReadMessage(&buf); !errors.Is(err, ErrUnknownType) {
		t.Fatalf("ReadMessage unknown type err = %v, want ErrUnknownType", err)
	}
}

// A small compressed body can decompress to gigabytes; the reader must reject it.
func TestReadMessageRejectsDecompressionBomb(t *testing.T) {
	bomb := make([]byte, 2*compression.MaxDecodedSize)
	codec, payload := compression.Compress(bomb)
	if codec != compression.CodecZstd {
		t.Fatalf("expected zstd for zero buffer, got codec %d", codec)
	}
	if len(payload) > MaxMessageSize {
		t.Fatalf("compressed bomb %d exceeds wire cap; cannot exercise the decompress path", len(payload))
	}

	var buf bytes.Buffer
	hb, _ := proto.Marshal(&wirepb.Header{Type: uint32(TypeNetworkConfig), Compression: uint32(codec)})
	_ = binary.Write(&buf, binary.BigEndian, uint16(len(hb)))
	buf.Write(hb)
	_ = binary.Write(&buf, binary.BigEndian, uint32(len(payload)))
	buf.Write(payload)

	if _, _, err := ReadMessage(&buf); err == nil {
		t.Fatal("ReadMessage accepted a decompression bomb")
	}
}

// An empty-body message must not stall over a synchronous, unbuffered stream.
func TestEmptyBodyOverSynchronousPipe(t *testing.T) {
	for _, msg := range []proto.Message{&wirepb.Ping{}, &wirepb.NetworkConfig{}} {
		a, b := net.Pipe()
		errc := make(chan error, 1)
		go func() { errc <- WriteMessage(a, msg) }()

		typ, got, err := ReadMessage(b)
		if err != nil {
			t.Fatalf("ReadMessage: %v", err)
		}
		if err := <-errc; err != nil {
			t.Fatalf("WriteMessage: %v", err)
		}
		if !proto.Equal(got, msg) {
			t.Fatalf("type %d round-trip mismatch", typ)
		}
		_ = a.Close()
		_ = b.Close()
	}
}

func TestMessageCompressionRoundTrip(t *testing.T) {
	folders := make([]*wirepb.Folder, 0, 64)
	for range 64 {
		folders = append(folders, &wirepb.Folder{FolderId: "the-same-repeated-share-id-for-compressibility"})
	}
	cfg := &wirepb.NetworkConfig{Folders: folders}

	var buf bytes.Buffer
	if err := WriteMessage(&buf, cfg); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}
	b := buf.Bytes()
	hlen := binary.BigEndian.Uint16(b[0:2])
	var hdr wirepb.Header
	if err := proto.Unmarshal(b[2:2+hlen], &hdr); err != nil {
		t.Fatalf("unmarshal header: %v", err)
	}
	if hdr.GetCompression() != 1 {
		t.Fatalf("expected zstd compression for repetitive body, got codec %d", hdr.GetCompression())
	}

	_, msg, err := ReadMessage(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if !proto.Equal(msg, cfg) {
		t.Fatalf("compressed round-trip mismatch")
	}
}
