package config

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/GhentiLabs/Trove/client/internal/storage"
)

const testNode = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func openDB(t *testing.T, path string) *storage.DB {
	t.Helper()
	db, err := storage.Open(storage.Options{Path: path, MaxOpenConns: 1})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func openStore(t *testing.T, db *storage.DB, node string) *Store {
	t.Helper()
	s, err := Open(Options{DB: db, NodeID: node})
	if err != nil {
		t.Fatalf("config.Open: %v", err)
	}
	return s
}

func TestFolderCRUD(t *testing.T) {
	ctx := context.Background()
	s := openStore(t, openDB(t, filepath.Join(t.TempDir(), "c.db")), testNode)

	if s.NodeID() != testNode {
		t.Fatalf("NodeID = %q", s.NodeID())
	}

	want := Folder{ID: "docs", Root: "/home/u/docs", Encrypted: true}
	if err := s.AddFolder(ctx, want); err != nil {
		t.Fatalf("AddFolder: %v", err)
	}
	if err := s.AddFolder(ctx, want); !errors.Is(err, ErrFolderExists) {
		t.Fatalf("duplicate AddFolder err = %v, want ErrFolderExists", err)
	}

	got, err := s.GetFolder(ctx, "docs")
	if err != nil {
		t.Fatalf("GetFolder: %v", err)
	}
	if got != want {
		t.Fatalf("GetFolder = %+v, want %+v", got, want)
	}

	if _, err := s.GetFolder(ctx, "missing"); !errors.Is(err, ErrFolderNotFound) {
		t.Fatalf("GetFolder(missing) err = %v, want ErrFolderNotFound", err)
	}

	if err := s.AddFolder(ctx, Folder{ID: "photos", Root: "/p"}); err != nil {
		t.Fatalf("AddFolder photos: %v", err)
	}
	list, err := s.ListFolders(ctx)
	if err != nil {
		t.Fatalf("ListFolders: %v", err)
	}
	if len(list) != 2 || list[0].ID != "docs" || list[1].ID != "photos" {
		t.Fatalf("ListFolders = %+v", list)
	}

	if err := s.RemoveFolder(ctx, "docs"); err != nil {
		t.Fatalf("RemoveFolder: %v", err)
	}
	if err := s.RemoveFolder(ctx, "docs"); !errors.Is(err, ErrFolderNotFound) {
		t.Fatalf("RemoveFolder(missing) err = %v, want ErrFolderNotFound", err)
	}
}

func TestFolderKeys(t *testing.T) {
	ctx := context.Background()
	s := openStore(t, openDB(t, filepath.Join(t.TempDir(), "c.db")), testNode)
	if err := s.AddFolder(ctx, Folder{ID: "f", Root: "/f", Encrypted: true}); err != nil {
		t.Fatalf("AddFolder: %v", err)
	}

	if _, _, err := s.GetFolderKey(ctx, "f"); !errors.Is(err, ErrNoKey) {
		t.Fatalf("GetFolderKey before set err = %v, want ErrNoKey", err)
	}
	if _, _, err := s.GetFolderKey(ctx, "missing"); !errors.Is(err, ErrFolderNotFound) {
		t.Fatalf("GetFolderKey(missing) err = %v, want ErrFolderNotFound", err)
	}
	if _, err := s.GenerateFolderKey(ctx, "missing"); !errors.Is(err, ErrFolderNotFound) {
		t.Fatalf("GenerateFolderKey(missing) err = %v, want ErrFolderNotFound", err)
	}

	gen, err := s.GenerateFolderKey(ctx, "f")
	if err != nil {
		t.Fatalf("GenerateFolderKey: %v", err)
	}
	got, generation, err := s.GetFolderKey(ctx, "f")
	if err != nil {
		t.Fatalf("GetFolderKey: %v", err)
	}
	if got != gen {
		t.Fatal("stored key does not match generated key")
	}
	if generation != FirstKeyGeneration {
		t.Fatalf("key generation = %d, want %d", generation, FirstKeyGeneration)
	}

	if err := s.AddFolder(ctx, Folder{ID: "g", Root: "/g", Encrypted: true}); err != nil {
		t.Fatalf("AddFolder g: %v", err)
	}
	derived, err := s.DeriveFolderKey(ctx, "g", "correct horse battery staple")
	if err != nil {
		t.Fatalf("DeriveFolderKey: %v", err)
	}
	if derived == gen {
		t.Fatal("derived key collided with previous random key")
	}
	got, _, err = s.GetFolderKey(ctx, "g")
	if err != nil {
		t.Fatalf("GetFolderKey after derive: %v", err)
	}
	if got != derived {
		t.Fatal("stored key does not match derived key")
	}
}

func TestRecoveryCodeRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := openStore(t, openDB(t, filepath.Join(t.TempDir(), "c.db")), testNode)
	if err := s.AddFolder(ctx, Folder{ID: "f", Root: "/f", Encrypted: true}); err != nil {
		t.Fatalf("AddFolder: %v", err)
	}
	key, err := s.GenerateFolderKey(ctx, "f")
	if err != nil {
		t.Fatalf("GenerateFolderKey: %v", err)
	}
	code := EncodeRecoveryCode(key)
	back, err := DecodeRecoveryCode(code)
	if err != nil {
		t.Fatalf("DecodeRecoveryCode: %v", err)
	}
	if back != key {
		t.Fatal("recovery code did not round-trip to the master key")
	}
	if _, err := DecodeRecoveryCode("not base32!!"); err == nil {
		t.Fatal("DecodeRecoveryCode accepted invalid input")
	}
	if _, err := DecodeRecoveryCode(strings.ToLower(code)); err != nil {
		t.Fatalf("DecodeRecoveryCode rejected lowercased code: %v", err)
	}
}

func TestSetFolderKeyGeneration(t *testing.T) {
	ctx := context.Background()
	s := openStore(t, openDB(t, filepath.Join(t.TempDir(), "c.db")), testNode)
	if err := s.AddFolder(ctx, Folder{ID: "f", Root: "/f", Encrypted: true}); err != nil {
		t.Fatalf("AddFolder: %v", err)
	}
	var key [MasterKeyLen]byte
	key[0] = 0x42
	if err := s.SetFolderKey(ctx, "f", key, 7); err != nil {
		t.Fatalf("SetFolderKey: %v", err)
	}
	got, gen, err := s.GetFolderKey(ctx, "f")
	if err != nil {
		t.Fatalf("GetFolderKey: %v", err)
	}
	if got != key || gen != 7 {
		t.Fatalf("got key=%x gen=%d, want key=%x gen=7", got, gen, key)
	}
}

func TestFolderCreatedTimestamp(t *testing.T) {
	ctx := context.Background()
	s := openStore(t, openDB(t, filepath.Join(t.TempDir(), "c.db")), testNode)
	if err := s.AddFolder(ctx, Folder{ID: "f", Root: "/f"}); err != nil {
		t.Fatalf("AddFolder: %v", err)
	}
	var ms int64
	if err := s.db.QueryRow(ctx, `SELECT created_ms FROM folders WHERE id = 'f'`).Scan(&ms); err != nil {
		t.Fatalf("read created_ms: %v", err)
	}
	if ms <= 0 {
		t.Fatalf("created_ms not populated: %d", ms)
	}
}

func TestKeyOverwriteRejected(t *testing.T) {
	ctx := context.Background()
	s := openStore(t, openDB(t, filepath.Join(t.TempDir(), "c.db")), testNode)
	if err := s.AddFolder(ctx, Folder{ID: "f", Root: "/f", Encrypted: true}); err != nil {
		t.Fatalf("AddFolder: %v", err)
	}
	if _, err := s.GenerateFolderKey(ctx, "f"); err != nil {
		t.Fatalf("GenerateFolderKey: %v", err)
	}
	if _, err := s.GenerateFolderKey(ctx, "f"); !errors.Is(err, ErrKeyExists) {
		t.Fatalf("second generate err = %v, want ErrKeyExists", err)
	}
	if _, err := s.DeriveFolderKey(ctx, "f", "pw"); !errors.Is(err, ErrKeyExists) {
		t.Fatalf("derive over existing key err = %v, want ErrKeyExists", err)
	}
}

func TestSchemaTooNew(t *testing.T) {
	ctx := context.Background()
	db := openDB(t, filepath.Join(t.TempDir(), "c.db"))
	openStore(t, db, testNode)

	if _, err := db.Exec(ctx, `UPDATE meta SET value = ? WHERE key = 'schema_version'`, SchemaVersion+1); err != nil {
		t.Fatalf("bump version: %v", err)
	}
	if _, err := Open(Options{DB: db, NodeID: testNode}); !errors.Is(err, ErrSchemaTooNew) {
		t.Fatalf("Open err = %v, want ErrSchemaTooNew", err)
	}
}

func TestNodeMismatch(t *testing.T) {
	db := openDB(t, filepath.Join(t.TempDir(), "c.db"))
	openStore(t, db, testNode)

	other := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if _, err := Open(Options{DB: db, NodeID: other}); !errors.Is(err, ErrNodeMismatch) {
		t.Fatalf("Open err = %v, want ErrNodeMismatch", err)
	}
}

func TestPersistsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "c.db")

	db1, err := storage.Open(storage.Options{Path: path, MaxOpenConns: 1})
	if err != nil {
		t.Fatalf("open db1: %v", err)
	}
	s1 := openStore(t, db1, testNode)
	if err := s1.AddFolder(ctx, Folder{ID: "f", Root: "/f"}); err != nil {
		t.Fatalf("AddFolder: %v", err)
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("close db1: %v", err)
	}

	db2 := openDB(t, path)
	s2 := openStore(t, db2, testNode)
	if _, err := s2.GetFolder(ctx, "f"); err != nil {
		t.Fatalf("GetFolder after reopen: %v", err)
	}
}
