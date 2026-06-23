package manifest

import (
	"encoding/hex"
	"testing"

	"github.com/GhentiLabs/Trove/client/internal/hasher"
)

func TestIdentityBytesGolden(t *testing.T) {
	m := Manifest{Kind: KindRegular, Path: "a"}
	const want = "74726f76652f6d616e69666573742f76310001000161000000"
	got := hex.EncodeToString(m.IdentityBytes())
	if got != want {
		t.Fatalf("identity bytes\n got %s\nwant %s", got, want)
	}
}

func TestIdentityBytesWithChunksGolden(t *testing.T) {
	var cid hasher.ChunkID
	for i := range cid {
		cid[i] = 0xAB
	}
	m := Manifest{Kind: KindRegular, Path: "f", Chunks: []ChunkRef{{ID: cid, Length: 1024}}}
	const want = "74726f76652f6d616e69666573742f763100" + "01000166000001" +
		"abababababababababababababababababababababababababababababababab" + "8008"
	if got := hex.EncodeToString(m.IdentityBytes()); got != want {
		t.Fatalf("identity bytes with chunks\n got %s\nwant %s", got, want)
	}
}

func TestIDIsHashOfIdentityBytes(t *testing.T) {
	m := Manifest{Kind: KindRegular, Path: "a", Chunks: []ChunkRef{{ID: hasher.Sum([]byte("x")), Length: 1}}}
	if got, want := m.ID(), ID(hasher.Sum(m.IdentityBytes())); got != want {
		t.Fatalf("ID = %s, want hash of identity bytes %s", got, want)
	}
}

func TestIDStringParseRoundTrip(t *testing.T) {
	id := Manifest{Kind: KindRegular, Path: "a"}.ID()
	got, err := ParseID(id.String())
	if err != nil {
		t.Fatalf("ParseID: %v", err)
	}
	if got != id {
		t.Fatalf("round trip: got %s, want %s", got, id)
	}
}

func TestPathNFCNormalizationYieldsSameID(t *testing.T) {
	nfc := "café/résumé"
	nfd := "café/résumé"
	if nfc == nfd {
		t.Fatal("test inputs must differ in raw bytes")
	}
	a := Manifest{Kind: KindRegular, Path: nfc}.ID()
	b := Manifest{Kind: KindRegular, Path: nfd}.ID()
	if a != b {
		t.Fatalf("NFD and NFC spellings produced different ids:\n nfc %s\n nfd %s", a, b)
	}
}

func TestNormalizePathIsNFCAndIdempotent(t *testing.T) {
	nfd := "café"
	once := NormalizePath(nfd)
	if once == nfd {
		t.Fatal("NormalizePath did not change a decomposed path")
	}
	if NormalizePath(once) != once {
		t.Fatal("NormalizePath is not idempotent")
	}
}

func TestCanonicalModeFoldsNonExecBits(t *testing.T) {
	a := Manifest{Kind: KindRegular, Path: "f", Mode: 0o644}.ID()
	b := Manifest{Kind: KindRegular, Path: "f", Mode: 0o640}.ID()
	if a != b {
		t.Fatalf("non-executable permission change must not alter id: %s vs %s", a, b)
	}
	exe := Manifest{Kind: KindRegular, Path: "f", Mode: 0o755}.ID()
	if exe == a {
		t.Fatal("executable bit must alter id")
	}
}

func TestKindAffectsID(t *testing.T) {
	reg := Manifest{Kind: KindRegular, Path: "p"}.ID()
	dir := Manifest{Kind: KindDir, Path: "p"}.ID()
	sym := Manifest{Kind: KindSymlink, Path: "p"}.ID()
	if reg == dir || reg == sym || dir == sym {
		t.Fatalf("kinds must produce distinct ids: reg=%s dir=%s sym=%s", reg, dir, sym)
	}
}

func TestSymlinkTargetNFCNormalizationYieldsSameID(t *testing.T) {
	nfc := "café"  // precomposed é
	nfd := "café" // e + combining acute
	if nfc == nfd {
		t.Fatal("test inputs must differ in raw bytes")
	}
	a := Manifest{Kind: KindSymlink, Path: "link", SymlinkTarget: nfc}.ID()
	b := Manifest{Kind: KindSymlink, Path: "link", SymlinkTarget: nfd}.ID()
	if a != b {
		t.Fatalf("symlink target NFD/NFC must yield same id:\n nfc %s\n nfd %s", a, b)
	}
}

func TestSymlinkTargetAffectsID(t *testing.T) {
	a := Manifest{Kind: KindSymlink, Path: "link", SymlinkTarget: "../a"}.ID()
	b := Manifest{Kind: KindSymlink, Path: "link", SymlinkTarget: "../b"}.ID()
	if a == b {
		t.Fatalf("symlink target must affect id: %s vs %s", a, b)
	}
}

func TestChunkOrderAffectsID(t *testing.T) {
	x := ChunkRef{ID: hasher.Sum([]byte("x")), Length: 1}
	y := ChunkRef{ID: hasher.Sum([]byte("y")), Length: 1}
	ab := Manifest{Kind: KindRegular, Path: "f", Chunks: []ChunkRef{x, y}}.ID()
	ba := Manifest{Kind: KindRegular, Path: "f", Chunks: []ChunkRef{y, x}}.ID()
	if ab == ba {
		t.Fatal("chunk order is content and must affect id")
	}
}
