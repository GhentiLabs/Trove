package model

import (
	"path"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/manifest"
)

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

const conflictTimeLayout = "20060102T150405.000Z"

// maxFilenameBytes bounds a single path component to the common POSIX NAME_MAX, so a long
// original name cannot make a conflict copy unwritable (ENAMETOOLONG) and stall the folder.
const maxFilenameBytes = 255

// ConflictPath is the keep-both copy path for the losing version of a path, derived
// only from the loser's agreed fields (its edit time and author), so every node names
// the copy identically. The name carries the loser's own identity, so the copy never
// collides with the winner or with a different loser.
func ConflictPath(p, loserAuthor string, loserAuthoredAt time.Time) string {
	p = manifest.NormalizePath(p)
	dir, base := path.Split(p)
	ext := path.Ext(base)
	stem := base[:len(base)-len(ext)]
	if stem == "" { // a dotfile like ".bashrc" is all "extension"; keep the name visible
		stem, ext = base, ""
	}
	suffix := ".conflict-" + loserAuthoredAt.UTC().Format(conflictTimeLayout) + "-" + loserAuthor
	if keep := maxFilenameBytes - len(suffix) - len(ext); keep >= 0 && len(stem) > keep {
		stem = stem[:keep]
	}
	return dir + stem + suffix + ext
}
