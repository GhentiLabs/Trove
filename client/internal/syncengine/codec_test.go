package syncengine

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/GhentiLabs/Trove/client/internal/hasher"
)

func TestDataStreamConstantsGolden(t *testing.T) {
	if DataMagic != 0x54445254 {
		t.Fatalf("DataMagic = %#x, want 0x54445254", DataMagic)
	}
	if DataVersion != 1 {
		t.Fatalf("DataVersion = %d, want 1", DataVersion)
	}
	if msgKindChunk != 0x01 {
		t.Fatalf("msgKindChunk = %#x, want 0x01", msgKindChunk)
	}
	if MaxFolderIDLen != 512 {
		t.Fatalf("MaxFolderIDLen = %d, want 512", MaxFolderIDLen)
	}
	if MaxChunkBytes != 4<<20 {
		t.Fatalf("MaxChunkBytes = %d, want %d", MaxChunkBytes, 4<<20)
	}
}

func TestChunkRequestGoldenLayout(t *testing.T) {
	id := hasher.Sum([]byte("chunk"))
	var buf bytes.Buffer
	if err := writeChunkRequest(&buf, "fld", id); err != nil {
		t.Fatalf("writeChunkRequest: %v", err)
	}
	b := buf.Bytes()
	if len(b) != 8+len("fld")+hasher.IDLen {
		t.Fatalf("len = %d, want %d", len(b), 8+len("fld")+hasher.IDLen)
	}
	if got := binary.BigEndian.Uint32(b[0:4]); got != DataMagic {
		t.Fatalf("magic = %#x, want %#x", got, DataMagic)
	}
	if b[4] != DataVersion {
		t.Fatalf("version = %d, want %d", b[4], DataVersion)
	}
	if b[5] != msgKindChunk {
		t.Fatalf("kind = %d, want %d", b[5], msgKindChunk)
	}
	if got := binary.BigEndian.Uint16(b[6:8]); int(got) != len("fld") {
		t.Fatalf("folder len = %d, want %d", got, len("fld"))
	}
	if string(b[8:8+3]) != "fld" {
		t.Fatalf("folder = %q, want fld", b[8:8+3])
	}
	if !bytes.Equal(b[8+3:], id[:]) {
		t.Fatalf("chunk id bytes mismatch")
	}
}

func TestChunkRequestRoundTrip(t *testing.T) {
	id := hasher.Sum([]byte("c"))
	var buf bytes.Buffer
	if err := writeChunkRequest(&buf, "docs", id); err != nil {
		t.Fatalf("writeChunkRequest: %v", err)
	}
	folder, got, err := readChunkRequest(&buf)
	if err != nil {
		t.Fatalf("readChunkRequest: %v", err)
	}
	if folder != "docs" || got != id {
		t.Fatalf("round-trip = (%q,%v), want (docs,%v)", folder, got, id)
	}
}

func TestChunkResponseGoldenLayout(t *testing.T) {
	var buf bytes.Buffer
	if err := writeChunkResponseHeader(&buf, StatusOK, 1234); err != nil {
		t.Fatalf("writeChunkResponseHeader: %v", err)
	}
	b := buf.Bytes()
	if len(b) != 12 {
		t.Fatalf("len = %d, want 12", len(b))
	}
	if got := binary.BigEndian.Uint32(b[0:4]); got != DataMagic {
		t.Fatalf("magic = %#x, want %#x", got, DataMagic)
	}
	if b[4] != DataVersion {
		t.Fatalf("version = %d, want %d", b[4], DataVersion)
	}
	if b[5] != byte(StatusOK) {
		t.Fatalf("status = %d, want %d", b[5], StatusOK)
	}
	if b[6] != 0 || b[7] != 0 {
		t.Fatalf("reserved bytes nonzero: %d %d", b[6], b[7])
	}
	if got := binary.BigEndian.Uint32(b[8:12]); got != 1234 {
		t.Fatalf("length = %d, want 1234", got)
	}
}

func TestChunkResponseRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	if err := writeChunkResponseHeader(&buf, StatusNotFound, 0); err != nil {
		t.Fatalf("writeChunkResponseHeader: %v", err)
	}
	status, length, err := readChunkResponseHeader(&buf)
	if err != nil {
		t.Fatalf("readChunkResponseHeader: %v", err)
	}
	if status != StatusNotFound || length != 0 {
		t.Fatalf("round-trip = (%d,%d), want (StatusNotFound,0)", status, length)
	}
}

func TestReadChunkRequestRejectsBadMagic(t *testing.T) {
	b := make([]byte, 8+hasher.IDLen)
	binary.BigEndian.PutUint32(b[0:4], 0xDEADBEEF)
	b[4] = DataVersion
	if _, _, err := readChunkRequest(bytes.NewReader(b)); !errors.Is(err, errBadDataMagic) {
		t.Fatalf("err = %v, want errBadDataMagic", err)
	}
}

func TestReadChunkRequestRejectsVersion(t *testing.T) {
	var buf bytes.Buffer
	_ = writeChunkRequest(&buf, "f", hasher.Sum([]byte("x")))
	b := buf.Bytes()
	b[4] = 99
	if _, _, err := readChunkRequest(bytes.NewReader(b)); !errors.Is(err, errDataVersion) {
		t.Fatalf("err = %v, want errDataVersion", err)
	}
}

func TestReadChunkRequestRejectsOversizeFolderID(t *testing.T) {
	b := make([]byte, 8)
	binary.BigEndian.PutUint32(b[0:4], DataMagic)
	b[4] = DataVersion
	b[5] = msgKindChunk
	binary.BigEndian.PutUint16(b[6:8], MaxFolderIDLen+1)
	if _, _, err := readChunkRequest(bytes.NewReader(b)); !errors.Is(err, errFolderIDTooLong) {
		t.Fatalf("err = %v, want errFolderIDTooLong", err)
	}
}

func TestReadChunkResponseRejectsOversizeLength(t *testing.T) {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, DataMagic)
	buf.WriteByte(DataVersion)
	buf.WriteByte(byte(StatusOK))
	buf.Write([]byte{0, 0})
	_ = binary.Write(&buf, binary.BigEndian, uint32(MaxChunkBytes+1))
	if _, _, err := readChunkResponseHeader(&buf); !errors.Is(err, errChunkTooLarge) {
		t.Fatalf("err = %v, want errChunkTooLarge", err)
	}
}

func FuzzReadChunkRequest(f *testing.F) {
	var buf bytes.Buffer
	_ = writeChunkRequest(&buf, "folder", hasher.Sum([]byte("seed")))
	f.Add(buf.Bytes())
	f.Add([]byte{})
	f.Add([]byte{0x54, 0x44, 0x52, 0x54})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _ = readChunkRequest(bytes.NewReader(data))
	})
}

func FuzzReadChunkResponseHeader(f *testing.F) {
	var buf bytes.Buffer
	_ = writeChunkResponseHeader(&buf, StatusOK, 4096)
	f.Add(buf.Bytes())
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _ = readChunkResponseHeader(bytes.NewReader(data))
	})
}
