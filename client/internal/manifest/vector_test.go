package manifest

import (
	"bytes"
	"encoding/hex"
	"strings"
	"testing"
)

func TestVersionVectorCanonicalGolden(t *testing.T) {
	vv := VersionVector{"a": 1}
	if got, want := hex.EncodeToString(vv.Canonical()), "01016101"; got != want {
		t.Fatalf("canonical = %s, want %s", got, want)
	}
}

func TestVersionVectorCanonicalSortsByNode(t *testing.T) {
	vv := VersionVector{"b": 2, "a": 1}
	if got, want := hex.EncodeToString(vv.Canonical()), "02016101016202"; got != want {
		t.Fatalf("canonical = %s, want %s (sorted a-before-b)", got, want)
	}
}

func TestVersionVectorCanonicalRealisticMultiNode(t *testing.T) {
	mk := func(seed string) string { return strings.Repeat(seed, 52)[:52] }
	lo := mk("ax3k7") // 52-char base32, sorts first
	hi := mk("zq9m2") // 52-char base32, sorts last
	if len(lo) != 52 || len(hi) != 52 || lo >= hi {
		t.Fatalf("bad test ids: lo=%q hi=%q", lo, hi)
	}

	a := VersionVector{hi: 7, lo: 9}.Canonical()
	if b := (VersionVector{lo: 9, hi: 7}).Canonical(); !bytes.Equal(a, b) {
		t.Fatal("map insertion order leaked into canonical form")
	}

	want := []byte{2, 0x34}
	want = append(want, lo...)
	want = append(want, 9, 0x34)
	want = append(want, hi...)
	want = append(want, 7)
	if !bytes.Equal(a, want) {
		t.Fatalf("canonical multi-node\n got %x\nwant %x", a, want)
	}
}

func TestVersionVectorParseRoundTrip(t *testing.T) {
	cases := []VersionVector{
		{},
		{"a": 1},
		{strings.Repeat("z", 52): 9, strings.Repeat("a", 52): 4000},
		{"x": 1, "y": 2, "w": 300000},
	}
	for _, vv := range cases {
		got, err := ParseVector(vv.Canonical())
		if err != nil {
			t.Fatalf("ParseVector(%v): %v", vv, err)
		}
		if !bytes.Equal(got.Canonical(), vv.Canonical()) {
			t.Fatalf("round trip: got %v, want %v", got, vv)
		}
	}
}

func TestVersionVectorParseRejectsTrailing(t *testing.T) {
	b := append(VersionVector{"a": 1}.Canonical(), 0xFF)
	if _, err := ParseVector(b); err == nil {
		t.Fatal("expected error on trailing bytes")
	}
}

func TestVersionVectorCanonicalDropsZero(t *testing.T) {
	with := VersionVector{"a": 1, "b": 0}
	without := VersionVector{"a": 1}
	if hex.EncodeToString(with.Canonical()) != hex.EncodeToString(without.Canonical()) {
		t.Fatal("zero-counter entries must be dropped from the canonical form")
	}
}

func TestVersionVectorEmptyCanonical(t *testing.T) {
	if got := hex.EncodeToString(VersionVector{}.Canonical()); got != "00" {
		t.Fatalf("empty canonical = %s, want 00", got)
	}
}

func TestVersionVectorCloneIsIndependent(t *testing.T) {
	orig := VersionVector{"a": 1}
	clone := orig.Clone()
	clone.Bump("a")
	if orig["a"] != 1 {
		t.Fatalf("clone mutated original: %d", orig["a"])
	}
	if clone["a"] != 2 {
		t.Fatalf("bump did not increment clone: %d", clone["a"])
	}
}

func TestVersionVectorBumpOnlyTouchesNode(t *testing.T) {
	vv := VersionVector{"a": 5, "b": 3}
	vv.Bump("a")
	if vv["a"] != 6 || vv["b"] != 3 {
		t.Fatalf("bump altered other counters: a=%d b=%d", vv["a"], vv["b"])
	}
	vv.Bump("c")
	if vv["c"] != 1 {
		t.Fatalf("bump of absent node = %d, want 1", vv["c"])
	}
}
