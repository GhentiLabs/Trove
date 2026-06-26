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

func TestVersionVectorCompare(t *testing.T) {
	cases := []struct {
		name string
		a, b VersionVector
		want Ordering
	}{
		{"both empty", VersionVector{}, VersionVector{}, Equal},
		{"equal", VersionVector{"a": 2, "b": 1}, VersionVector{"a": 2, "b": 1}, Equal},
		{"a dominates by counter", VersionVector{"a": 3}, VersionVector{"a": 2}, Greater},
		{"a dominates by extra node", VersionVector{"a": 2, "b": 1}, VersionVector{"a": 2}, Greater},
		{"a dominated", VersionVector{"a": 2}, VersionVector{"a": 2, "b": 1}, Less},
		{"empty dominated by nonempty", VersionVector{}, VersionVector{"a": 1}, Less},
		{"concurrent disjoint", VersionVector{"a": 1}, VersionVector{"b": 1}, Concurrent},
		{"concurrent mixed", VersionVector{"a": 2, "b": 1}, VersionVector{"a": 1, "b": 2}, Concurrent},
		{"zero entry is identity", VersionVector{"a": 1, "z": 0}, VersionVector{"a": 1}, Equal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.a.Compare(tc.b); got != tc.want {
				t.Fatalf("Compare = %v, want %v", got, tc.want)
			}
			// Compare is antisymmetric: swapping inverts Greater/Less, keeps Equal/Concurrent.
			inv := map[Ordering]Ordering{Equal: Equal, Concurrent: Concurrent, Greater: Less, Less: Greater}
			if got := tc.b.Compare(tc.a); got != inv[tc.want] {
				t.Fatalf("Compare(swapped) = %v, want %v", got, inv[tc.want])
			}
		})
	}
}

func TestVersionVectorDominatesIsStrictDescent(t *testing.T) {
	a := VersionVector{"a": 2}
	b := VersionVector{"a": 1}
	if !a.Dominates(b) {
		t.Fatal("a must dominate b")
	}
	if a.Dominates(a) {
		t.Fatal("Dominates must be strict, not reflexive")
	}
	if b.Dominates(a) {
		t.Fatal("b must not dominate a")
	}
}

func TestVersionVectorIsConcurrentIsSymmetric(t *testing.T) {
	a := VersionVector{"a": 2}
	ordered := VersionVector{"a": 1}
	disjoint := VersionVector{"b": 1}
	if !a.IsConcurrent(disjoint) || !disjoint.IsConcurrent(a) {
		t.Fatal("disjoint vectors must be concurrent, symmetrically")
	}
	if a.IsConcurrent(ordered) {
		t.Fatal("an ordered pair must not be concurrent")
	}
}

func TestJoinIsLeastUpperBound(t *testing.T) {
	a := VersionVector{"a": 2, "b": 1}
	b := VersionVector{"a": 1, "b": 3, "c": 1}
	j := Join(a, b)
	want := VersionVector{"a": 2, "b": 3, "c": 1}
	if !bytes.Equal(j.Canonical(), want.Canonical()) {
		t.Fatalf("Join = %v, want %v", j, want)
	}
	if !j.Dominates(a) || !j.Dominates(b) {
		t.Fatal("join must dominate both inputs")
	}
}

func TestJoinCommutativeAssociativeIdempotent(t *testing.T) {
	a := VersionVector{"a": 2, "b": 1}
	b := VersionVector{"b": 3, "c": 1}
	c := VersionVector{"a": 1, "c": 4}
	eq := func(x, y VersionVector) bool { return bytes.Equal(x.Canonical(), y.Canonical()) }
	if !eq(Join(a, b), Join(b, a)) {
		t.Fatal("join not commutative")
	}
	if !eq(Join(Join(a, b), c), Join(a, Join(b, c))) {
		t.Fatal("join not associative")
	}
	if !eq(Join(a, a), a) {
		t.Fatal("join not idempotent")
	}
}

func TestJoinDoesNotMutateInputs(t *testing.T) {
	a := VersionVector{"a": 2}
	b := VersionVector{"a": 5}
	_ = Join(a, b)
	if a["a"] != 2 || b["a"] != 5 {
		t.Fatalf("join mutated an input: a=%d b=%d", a["a"], b["a"])
	}
}
