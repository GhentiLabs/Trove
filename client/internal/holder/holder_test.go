package holder

import (
	"bytes"
	"errors"
	"testing"

	"github.com/GhentiLabs/Trove/client/internal/crypto"
	"github.com/GhentiLabs/Trove/client/internal/hasher"
	"github.com/GhentiLabs/Trove/client/internal/manifest"
)

func TestStoreRoundTrip(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	var id [crypto.BlindIDLen]byte
	id[0] = 0x7
	data := []byte("opaque ciphertext")

	if s.Has(id) {
		t.Fatal("Has true before Put")
	}
	if _, err := s.Get(id); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get before Put err = %v, want ErrNotFound", err)
	}
	if err := s.Put(id, data); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if !s.Has(id) {
		t.Fatal("Has false after Put")
	}
	got, err := s.Get(id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("Get = %q, want %q", got, data)
	}
	if err := s.Put(id, []byte("replacement")); err != nil {
		t.Fatalf("Put replace: %v", err)
	}
	if got, _ := s.Get(id); !bytes.Equal(got, []byte("replacement")) {
		t.Fatalf("Get after replace = %q", got)
	}

	if err := s.Delete(id); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if s.Has(id) {
		t.Fatal("Has true after Delete")
	}
	if err := s.Delete(id); err != nil {
		t.Fatalf("Delete of absent blob: %v", err)
	}
}

func TestCatalogRoundTrip(t *testing.T) {
	cid := hasher.Sum([]byte("chunk"))
	in := []manifest.Manifest{
		{Kind: manifest.KindRegular, Path: "z/last.txt", Mode: 0o644, Chunks: []manifest.ChunkRef{{ID: cid, Length: 5}}},
		{Kind: manifest.KindDir, Path: "a", Mode: 0o755},
		{Kind: manifest.KindSymlink, Path: "a/link", SymlinkTarget: "../z/last.txt"},
		{Kind: manifest.KindRegular, Path: "a/exec", Mode: 0o755, Chunks: []manifest.ChunkRef{
			{ID: hasher.Sum([]byte("c1")), Length: 10}, {ID: hasher.Sum([]byte("c2")), Length: 20},
		}},
	}
	enc := EncodeCatalog(in)

	out, err := DecodeCatalog(enc)
	if err != nil {
		t.Fatalf("DecodeCatalog: %v", err)
	}
	if len(out) != len(in) {
		t.Fatalf("decoded %d entries, want %d", len(out), len(in))
	}
	// Output is sorted by path; check the manifest ids match regardless of input order.
	want := map[string]manifest.ID{}
	for _, m := range in {
		want[m.Path] = m.ID()
	}
	for _, m := range out {
		if m.ID() != want[m.Path] {
			t.Fatalf("manifest %q id mismatch after round-trip", m.Path)
		}
	}
}

func TestEncodeCatalogDeterministic(t *testing.T) {
	a := []manifest.Manifest{{Kind: manifest.KindDir, Path: "a"}, {Kind: manifest.KindDir, Path: "b"}}
	b := []manifest.Manifest{{Kind: manifest.KindDir, Path: "b"}, {Kind: manifest.KindDir, Path: "a"}}
	if !bytes.Equal(EncodeCatalog(a), EncodeCatalog(b)) {
		t.Fatal("catalog encoding is order-dependent")
	}
}

func TestDecodeCatalogRejectsGarbage(t *testing.T) {
	if _, err := DecodeCatalog([]byte("not a catalog")); err == nil {
		t.Fatal("DecodeCatalog accepted garbage")
	}
	enc := EncodeCatalog([]manifest.Manifest{{Kind: manifest.KindDir, Path: "a"}})
	if _, err := DecodeCatalog(enc[:len(enc)-1]); err == nil {
		t.Fatal("DecodeCatalog accepted truncated input")
	}
}
