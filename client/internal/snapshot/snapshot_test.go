package snapshot

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/GhentiLabs/Trove/client/internal/hasher"
	"github.com/GhentiLabs/Trove/client/internal/manifest"
)

func mid(s string) manifest.ID {
	var id manifest.ID
	copy(id[:], s)
	return id
}

func leaf(path string) Leaf {
	return Leaf{Path: path, ManifestID: mid(path)}
}

func TestRootGolden(t *testing.T) {
	if got, want := (Set{}).Root().String(), "59b8bfb663d7118aa2fbf1b5f450a20373d0cbf0db6b67ac1a888f1c38609a44"; got != want {
		t.Fatalf("empty root = %s, want %s", got, want)
	}
	single := Set{{Path: "f", ManifestID: manifest.ID{0xAB}, Deleted: false}}.Root()
	if got, want := single.String(), "81de7cd92961533e4e57723c270c9d6fbcc1bcc4d797843c98b3c5eb86057c7b"; got != want {
		t.Fatalf("single-leaf root = %s, want %s", got, want)
	}
}

func TestRootIsOrderIndependent(t *testing.T) {
	a, b, c := leaf("a"), leaf("b"), leaf("c")
	if r1, r2 := (Set{a, b, c}).Root(), (Set{c, a, b}).Root(); r1 != r2 {
		t.Fatalf("root depends on input order: %s vs %s", r1, r2)
	}
}

func TestRootDistinctForDistinctState(t *testing.T) {
	empty := Set{}.Root()
	one := Set{leaf("a")}.Root()
	edited := Set{{Path: "a", ManifestID: mid("a2")}}.Root()
	tombstoned := Set{{Path: "a", ManifestID: mid("a"), Deleted: true}}.Root()
	roots := []Root{empty, one, edited, tombstoned}
	for i := range roots {
		for j := i + 1; j < len(roots); j++ {
			if roots[i] == roots[j] {
				t.Fatalf("distinct states share a root at %d,%d", i, j)
			}
		}
	}
}

func TestLeafCommitsPath(t *testing.T) {
	a := Set{{Path: "a", ManifestID: mid("same")}}.Root()
	b := Set{{Path: "b", ManifestID: mid("same")}}.Root()
	if a == b {
		t.Fatal("leaf hash must commit the path, not only the manifest id")
	}
}

func TestRenameChangesRootWithRealIDs(t *testing.T) {
	chunks := []manifest.ChunkRef{{ID: hasher.Sum([]byte("x")), Length: 1}}
	idAt := func(p string) manifest.ID {
		return manifest.Manifest{Kind: manifest.KindRegular, Path: p, Chunks: chunks}.ID()
	}
	ra := Set{{Path: "a.txt", ManifestID: idAt("a.txt")}}.Root()
	rb := Set{{Path: "b.txt", ManifestID: idAt("b.txt")}}.Root()
	if ra == rb {
		t.Fatal("renaming a file with unchanged content must change the root")
	}
}

func TestRootStringParseRoundTrip(t *testing.T) {
	r := Set{leaf("a"), leaf("b")}.Root()
	got, err := ParseRoot(r.String())
	if err != nil {
		t.Fatalf("ParseRoot: %v", err)
	}
	if got != r {
		t.Fatalf("round trip: got %s, want %s", got, r)
	}
}

func TestDiffAgainstSelfIsEmpty(t *testing.T) {
	s := Set{leaf("a"), leaf("b"), {Path: "g", ManifestID: mid("g"), Deleted: true}}
	if d := Diff(s, s); len(d.Added)+len(d.Removed)+len(d.Changed) != 0 {
		t.Fatalf("self-diff not empty: %+v", d)
	}
}

func TestDiffClassifies(t *testing.T) {
	a := Set{leaf("keep"), leaf("remove"), {Path: "change", ManifestID: mid("v1")}}
	b := Set{leaf("keep"), {Path: "change", ManifestID: mid("v2")}, leaf("add")}
	d := Diff(a, b)

	if len(d.Added) != 1 || d.Added[0].Path != "add" {
		t.Fatalf("added = %+v", d.Added)
	}
	if len(d.Removed) != 1 || d.Removed[0].Path != "remove" {
		t.Fatalf("removed = %+v", d.Removed)
	}
	if len(d.Changed) != 1 || d.Changed[0].Before.Path != "change" || d.Changed[0].After.ManifestID != mid("v2") {
		t.Fatalf("changed = %+v", d.Changed)
	}
}

func TestDiffMatchesBruteForceReference(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for range 200 {
		a := randomSet(rng)
		b := randomSet(rng)
		got := Diff(a, b)
		want := referenceDiff(a, b)
		if !sameChanges(got, want) {
			t.Fatalf("diff mismatch\na=%v\nb=%v\ngot=%+v\nwant=%+v", a, b, got, want)
		}
	}
}

func randomSet(rng *rand.Rand) Set {
	var s Set
	for range rng.IntN(8) {
		path := fmt.Sprintf("p%d", rng.IntN(6))
		if slices.ContainsFunc(s, func(l Leaf) bool { return l.Path == path }) {
			continue
		}
		s = append(s, Leaf{Path: path, ManifestID: mid(fmt.Sprintf("v%d", rng.IntN(3))), Deleted: rng.IntN(2) == 0})
	}
	return s
}

func referenceDiff(a, b Set) DiffResult {
	am := map[string]Leaf{}
	for _, l := range a {
		am[l.Path] = l
	}
	bm := map[string]Leaf{}
	for _, l := range b {
		bm[l.Path] = l
	}
	var d DiffResult
	for _, p := range slices.Sorted(maps.Keys(am)) {
		bl, ok := bm[p]
		if !ok {
			d.Removed = append(d.Removed, am[p])
			continue
		}
		if al := am[p]; al.ManifestID != bl.ManifestID || al.Deleted != bl.Deleted {
			d.Changed = append(d.Changed, ChangePair{Before: al, After: bl})
		}
	}
	for _, p := range slices.Sorted(maps.Keys(bm)) {
		if _, ok := am[p]; !ok {
			d.Added = append(d.Added, bm[p])
		}
	}
	return d
}

func sameChanges(x, y DiffResult) bool {
	return slices.Equal(x.Added, y.Added) && slices.Equal(x.Removed, y.Removed) && slices.Equal(x.Changed, y.Changed)
}
