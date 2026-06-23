package scanner

import (
	"context"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/chunkstore"
	"github.com/GhentiLabs/Trove/client/internal/model"
	"github.com/GhentiLabs/Trove/client/internal/watcher"
)

func newControlLoopScanner(w watcher.Watcher) *Scanner {
	return &Scanner{
		root:       "/folder",
		watcher:    w,
		debounce:   DefaultDebounceWindow,
		quiesce:    DefaultSnapshotQuiesce,
		nextRescan: func() time.Duration { return time.Hour }, // out of the way of these tests
	}
}

func drain(out chan request) []request {
	var rs []request
	for {
		select {
		case r := <-out:
			rs = append(rs, r)
		default:
			return rs
		}
	}
}

func TestControlLoopDebouncesBurstIntoOneScan(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := watcher.NewFake()
		s := newControlLoopScanner(f)
		out := make(chan request, 16)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go s.controlLoop(ctx, out)

		f.Emit(watcher.Event{Path: "/folder/a.txt", Op: watcher.OpWrite})
		f.Emit(watcher.Event{Path: "/folder/sub/b.txt", Op: watcher.OpWrite})

		time.Sleep(DefaultDebounceWindow - 100*time.Millisecond)
		synctest.Wait()
		if got := drain(out); len(got) != 0 {
			t.Fatalf("scan fired before debounce elapsed: %+v", got)
		}

		time.Sleep(200 * time.Millisecond)
		synctest.Wait()
		got := drain(out)
		if len(got) != 1 || got[0].kind != reqScan {
			t.Fatalf("want one scan after debounce, got %+v", got)
		}
		paths := slices.Clone(got[0].paths)
		slices.Sort(paths)
		if !slices.Equal(paths, []string{"a.txt", "sub/b.txt"}) {
			t.Fatalf("scan batch = %v, want coalesced a.txt + sub/b.txt", paths)
		}
	})
}

func TestControlLoopSnapshotsOnQuiesce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := watcher.NewFake()
		s := newControlLoopScanner(f)
		out := make(chan request, 16)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go s.controlLoop(ctx, out)

		f.Emit(watcher.Event{Path: "/folder/a.txt", Op: watcher.OpWrite})
		time.Sleep(DefaultSnapshotQuiesce + 100*time.Millisecond)
		synctest.Wait()

		var snapshots int
		for _, r := range drain(out) {
			if r.kind == reqSnapshot {
				snapshots++
			}
		}
		if snapshots != 1 {
			t.Fatalf("want exactly one snapshot on quiesce, got %d", snapshots)
		}
	})
}

func TestRescanJitterDefaults(t *testing.T) {
	base := Options{Root: "/r", Chunks: &chunkstore.Store{}, Model: &model.Store{}, Watcher: watcher.NewFake(), RescanInterval: time.Minute}

	// Unset jitter randomizes by default within [interval, interval+default).
	unset, err := New(base)
	if err != nil {
		t.Fatal(err)
	}
	for range 50 {
		d := unset.nextRescan()
		if d < time.Minute || d >= time.Minute+DefaultRescanJitter {
			t.Fatalf("default jitter out of range: %v", d)
		}
	}

	// Negative jitter explicitly disables it: always exactly the interval.
	off := base
	off.RescanJitter = -1
	disabled, err := New(off)
	if err != nil {
		t.Fatal(err)
	}
	if d := disabled.nextRescan(); d != time.Minute {
		t.Fatalf("disabled jitter = %v, want exactly %v", d, time.Minute)
	}
}

func TestControlLoopFiresPeriodicRescan(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := watcher.NewFake()
		s := newControlLoopScanner(f)
		s.nextRescan = func() time.Duration { return 5 * time.Minute }
		out := make(chan request, 16)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go s.controlLoop(ctx, out)

		time.Sleep(time.Minute)
		synctest.Wait()
		if got := drain(out); len(got) != 0 {
			t.Fatalf("rescan fired before its interval: %+v", got)
		}

		time.Sleep(5 * time.Minute)
		synctest.Wait()
		var rescans int
		for _, r := range drain(out) {
			if r.kind == reqRescan {
				rescans++
			}
		}
		if rescans != 1 {
			t.Fatalf("want one rescan after the interval, got %d", rescans)
		}
	})
}

func TestControlLoopIgnoresPathsOutsideRoot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := watcher.NewFake()
		s := newControlLoopScanner(f)
		out := make(chan request, 16)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go s.controlLoop(ctx, out)

		f.Emit(watcher.Event{Path: "/elsewhere/c.txt", Op: watcher.OpWrite})
		time.Sleep(DefaultSnapshotQuiesce + time.Second)
		synctest.Wait()
		if got := drain(out); len(got) != 0 {
			t.Fatalf("event outside root produced requests: %+v", got)
		}
	})
}
