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

func TestConflictWinnerIgnoresSubMillisecondAndMonotonicClock(t *testing.T) {
	base := time.UnixMilli(1000)
	if ConflictWinner("a", base.Add(400*time.Microsecond), "b", base) {
		t.Fatal("sub-millisecond difference must not override the author tiebreak")
	}
}
