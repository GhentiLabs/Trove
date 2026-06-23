package snapshot

import (
	"slices"
	"strings"
)

// ChangePair is a path whose leaf differs between two sets.
type ChangePair struct {
	Before Leaf
	After  Leaf
}

// DiffResult is the set difference between two snapshots, each list ordered by
// NFC path. A live-to-tombstone transition appears in Changed with
// After.Deleted set.
type DiffResult struct {
	Added   []Leaf
	Removed []Leaf
	Changed []ChangePair
}

// Diff reports the paths that were added, removed, or changed going from a to b.
// It is a merge-join over the path-sorted leaves; a path present in both with an
// identical manifest id and tombstone state is unchanged and omitted. Paths must
// already be NFC-normalized.
func Diff(a, b Set) DiffResult {
	as, bs := sortedByPath(a), sortedByPath(b)
	var d DiffResult
	i, j := 0, 0
	for i < len(as) && j < len(bs) {
		switch cmp := strings.Compare(as[i].Path, bs[j].Path); {
		case cmp < 0:
			d.Removed = append(d.Removed, as[i])
			i++
		case cmp > 0:
			d.Added = append(d.Added, bs[j])
			j++
		default:
			if as[i].ManifestID != bs[j].ManifestID || as[i].Deleted != bs[j].Deleted {
				d.Changed = append(d.Changed, ChangePair{Before: as[i], After: bs[j]})
			}
			i++
			j++
		}
	}
	d.Removed = append(d.Removed, as[i:]...)
	d.Added = append(d.Added, bs[j:]...)
	return d
}

func sortedByPath(s Set) Set {
	out := slices.Clone(s)
	slices.SortFunc(out, func(a, b Leaf) int {
		return strings.Compare(a.Path, b.Path)
	})
	return out
}
