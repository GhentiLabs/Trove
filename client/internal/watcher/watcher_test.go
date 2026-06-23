package watcher

import (
	"errors"
	"testing"
)

func TestFakeDeliversEvents(t *testing.T) {
	f := NewFake()
	t.Cleanup(func() { _ = f.Close() })

	f.Emit(Event{Path: "/folder/a.txt", Op: OpWrite})
	ev := <-f.Events()
	if ev.Path != "/folder/a.txt" || ev.Op != OpWrite {
		t.Fatalf("got %+v", ev)
	}
}

func TestFakeReportsErrors(t *testing.T) {
	f := NewFake()
	t.Cleanup(func() { _ = f.Close() })

	want := errors.New("watch failed")
	f.EmitError(want)
	if got := <-f.Errors(); !errors.Is(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}
