package model

import "time"

// ConflictWinner reports whether the version authored by (aAuthor, aAuthoredAt) is
// the canonical winner over a concurrent version (bAuthor, bAuthoredAt): later
// timestamp wins; the author node id breaks an equal timestamp.
func ConflictWinner(aAuthor string, aAuthoredAt time.Time, bAuthor string, bAuthoredAt time.Time) bool {
	am, bm := aAuthoredAt.UnixMilli(), bAuthoredAt.UnixMilli()
	if am != bm {
		return am > bm
	}
	return aAuthor > bAuthor
}
