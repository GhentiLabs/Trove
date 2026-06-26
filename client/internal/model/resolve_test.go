package model

import (
	"testing"
	"time"
)

func TestConflictWinnerLaterTimestampWins(t *testing.T) {
	early := time.UnixMilli(1000)
	late := time.UnixMilli(2000)
	if !ConflictWinner("a", late, "b", early) {
		t.Fatal("later edit must win regardless of author")
	}
	if ConflictWinner("z", early, "a", late) {
		t.Fatal("earlier edit must lose even with a larger author id")
	}
}

func TestConflictWinnerTiebreaksOnAuthor(t *testing.T) {
	ts := time.UnixMilli(1000)
	if !ConflictWinner("b", ts, "a", ts) {
		t.Fatal("on a timestamp tie the larger author id wins")
	}
	if ConflictWinner("a", ts, "b", ts) {
		t.Fatal("on a timestamp tie the smaller author id loses")
	}
}

func TestConflictWinnerIsTotalAndDeterministic(t *testing.T) {
	versions := []struct {
		author string
		at     time.Time
	}{
		{"node-a", time.UnixMilli(1000)},
		{"node-b", time.UnixMilli(1000)},
		{"node-c", time.UnixMilli(2000)},
		{"node-d", time.UnixMilli(500)},
	}
	for _, x := range versions {
		for _, y := range versions {
			if x.author == y.author {
				continue
			}
			xy := ConflictWinner(x.author, x.at, y.author, y.at)
			yx := ConflictWinner(y.author, y.at, x.author, x.at)
			if xy == yx {
				t.Fatalf("not antisymmetric for %s vs %s: both %v", x.author, y.author, xy)
			}
		}
	}
}

func TestConflictPath(t *testing.T) {
	at := time.UnixMilli(1700000000000) // 2023-11-14T22:13:20.000Z
	author := "node22222222222222222222222222222222222222222222node"
	cases := []struct {
		in, want string
	}{
		{"budget.xlsx", "budget.conflict-20231114T221320.000Z-" + author + ".xlsx"},
		{"dir/sub/report.txt", "dir/sub/report.conflict-20231114T221320.000Z-" + author + ".txt"},
		{"noext", "noext.conflict-20231114T221320.000Z-" + author},
		{"archive.tar.gz", "archive.tar.conflict-20231114T221320.000Z-" + author + ".gz"},
		{".bashrc", ".bashrc.conflict-20231114T221320.000Z-" + author},
	}
	for _, tc := range cases {
		if got := ConflictPath(tc.in, author, at); got != tc.want {
			t.Fatalf("ConflictPath(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestConflictPathStaysWithinFolder(t *testing.T) {
	// Even a hostile author cannot be used here (resolveRemote rejects it), but the path
	// must never gain a leading separator or parent segment from a normal author.
	got := ConflictPath("dir/file.txt", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", time.UnixMilli(0))
	if got == "" || got[0] == '/' || got == "dir/file.txt" {
		t.Fatalf("unexpected conflict path: %q", got)
	}
}

func TestConflictWinnerIgnoresSubMillisecondAndMonotonicClock(t *testing.T) {
	base := time.UnixMilli(1000)
	if ConflictWinner("a", base.Add(400*time.Microsecond), "b", base) {
		t.Fatal("sub-millisecond difference must not override the author tiebreak")
	}
}
