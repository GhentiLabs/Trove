package model

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/GhentiLabs/Trove/client/internal/storage"
)

var (
	nodeA = strings.Repeat("a", 52)
	nodeB = strings.Repeat("b", 52)
)

func openDB(t *testing.T, path string) *storage.DB {
	t.Helper()
	db, err := storage.Open(storage.Options{Path: path, MaxOpenConns: 4})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func newStore(t *testing.T) *Store {
	t.Helper()
	db := openDB(t, filepath.Join(t.TempDir(), "state.db"))
	s, err := Open(Options{DB: db, NodeID: nodeA})
	if err != nil {
		t.Fatalf("model.Open: %v", err)
	}
	return s
}

func TestOpenBindsNodeID(t *testing.T) {
	if s := newStore(t); s.NodeID() != nodeA {
		t.Fatalf("NodeID = %q, want %q", s.NodeID(), nodeA)
	}
}

func TestOpenRejectsForeignNode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	db := openDB(t, path)
	if _, err := Open(Options{DB: db, NodeID: nodeA}); err != nil {
		t.Fatalf("first open: %v", err)
	}
	if _, err := Open(Options{DB: db, NodeID: nodeB}); !errors.Is(err, ErrNodeMismatch) {
		t.Fatalf("reopen with foreign node: got %v, want ErrNodeMismatch", err)
	}
}

func TestOpenRejectsFutureSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	db := openDB(t, path)
	if _, err := Open(Options{DB: db, NodeID: nodeA}); err != nil {
		t.Fatalf("first open: %v", err)
	}
	if _, err := db.Exec(context.Background(), `UPDATE meta SET value = '999' WHERE key = 'schema_version'`); err != nil {
		t.Fatalf("inject version: %v", err)
	}
	if _, err := Open(Options{DB: db, NodeID: nodeA}); !errors.Is(err, ErrSchemaTooNew) {
		t.Fatalf("got %v, want ErrSchemaTooNew", err)
	}
}
