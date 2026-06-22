package analytics

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func openTemp(t *testing.T, cap int64) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "analytics.db")
	s, err := Open(path, cap, nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestStoreAndRead(t *testing.T) {
	s := openTemp(t, 1<<30)
	ctx := context.Background()

	rec := Record{
		NodeID:        "node-a",
		InstallID:     "install-1",
		SchemaVersion: 3,
		EventMillis:   1700,
		SourceIP:      "203.0.113.4",
		Fields:        map[string]any{"os": "linux", "version": "1.2.3", "peers": 4.0},
	}
	if err := s.Insert(ctx, rec); err != nil {
		t.Fatalf("Store: %v", err)
	}

	got, err := s.Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d records, want 1", len(got))
	}
	r := got[0]
	if r.NodeID != rec.NodeID || r.InstallID != rec.InstallID || r.SchemaVersion != rec.SchemaVersion {
		t.Fatalf("scalar fields mismatch: %+v", r)
	}
	if r.Fields["os"] != "linux" || r.Fields["peers"].(float64) != 4 {
		t.Fatalf("open fields not preserved: %+v", r.Fields)
	}
}

func TestStoreNilFields(t *testing.T) {
	s := openTemp(t, 1<<30)
	if err := s.Insert(context.Background(), Record{NodeID: "n", InstallID: "i"}); err != nil {
		t.Fatalf("Store with nil fields: %v", err)
	}
	got, err := s.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Fields == nil {
		t.Fatalf("expected empty non-nil fields, got %+v", got)
	}
}

func TestDiskCapRejects(t *testing.T) {
	// A cap below the freshly created (empty) DB size forces rejection.
	s := openTemp(t, 1)
	err := s.Insert(context.Background(), Record{NodeID: "n", InstallID: "i"})
	if !errors.Is(err, ErrDiskFull) {
		t.Fatalf("err = %v, want ErrDiskFull", err)
	}
	n, err := s.Count(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("rejected record was stored, count=%d", n)
	}
}

func TestCount(t *testing.T) {
	s := openTemp(t, 1<<30)
	ctx := context.Background()
	for range 5 {
		if err := s.Insert(ctx, Record{NodeID: "n", InstallID: "i"}); err != nil {
			t.Fatal(err)
		}
	}
	n, err := s.Count(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Fatalf("count = %d, want 5", n)
	}
}
