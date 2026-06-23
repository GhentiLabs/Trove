package storage

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

func openTest(t *testing.T, maxConns int) *DB {
	t.Helper()
	db, err := Open(Options{Path: filepath.Join(t.TempDir(), "test.db"), MaxOpenConns: maxConns})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(context.Background(), `CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT NOT NULL)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	return db
}

func count(t *testing.T, db *DB) int {
	t.Helper()
	var n int
	if err := db.QueryRow(context.Background(), `SELECT COUNT(*) FROM kv`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

func TestWithTxCommit(t *testing.T) {
	db := openTest(t, 1)
	err := db.WithTx(context.Background(), func(tx *Tx) error {
		_, err := tx.Exec(context.Background(), `INSERT INTO kv (k, v) VALUES (?, ?)`, "a", "1")
		return err
	})
	if err != nil {
		t.Fatalf("WithTx: %v", err)
	}
	if n := count(t, db); n != 1 {
		t.Fatalf("count = %d, want 1", n)
	}
}

func TestWithTxRollbackOnError(t *testing.T) {
	db := openTest(t, 1)
	sentinel := errors.New("boom")
	err := db.WithTx(context.Background(), func(tx *Tx) error {
		if _, err := tx.Exec(context.Background(), `INSERT INTO kv (k, v) VALUES (?, ?)`, "a", "1"); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want sentinel", err)
	}
	if n := count(t, db); n != 0 {
		t.Fatalf("count = %d, want 0 (rolled back)", n)
	}
}

func TestWithTxRollbackOnPanic(t *testing.T) {
	db := openTest(t, 1)
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic to propagate")
			}
		}()
		_ = db.WithTx(context.Background(), func(tx *Tx) error {
			if _, err := tx.Exec(context.Background(), `INSERT INTO kv (k, v) VALUES (?, ?)`, "a", "1"); err != nil {
				t.Fatalf("insert: %v", err)
			}
			panic("boom")
		})
	}()
	if n := count(t, db); n != 0 {
		t.Fatalf("count = %d, want 0 (rolled back on panic)", n)
	}
}

// TestConcurrentReadersAndWriter exercises the chunkindex-style configuration: a
// reader pool plus serialized writers. Readers must not error or deadlock while
// writes are in flight.
func TestConcurrentReadersAndWriter(t *testing.T) {
	db := openTest(t, 8)
	ctx := context.Background()

	var wg sync.WaitGroup
	var errs atomic.Int64

	wg.Go(func() {
		for i := range 200 {
			err := db.WithTx(ctx, func(tx *Tx) error {
				_, err := tx.Exec(ctx, `INSERT OR REPLACE INTO kv (k, v) VALUES (?, ?)`, "k", string(rune('a'+i%26)))
				return err
			})
			if err != nil {
				errs.Add(1)
			}
		}
	})

	for range 8 {
		wg.Go(func() {
			for range 200 {
				var n int
				if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM kv`).Scan(&n); err != nil {
					errs.Add(1)
				}
			}
		})
	}

	wg.Wait()
	if errs.Load() != 0 {
		t.Fatalf("%d concurrent errors", errs.Load())
	}
}
