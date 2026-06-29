package config

import (
	"bytes"
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
	held := Folder{ID: "backup", ShareID: "g", Encrypted: true, Holder: true}
	if err := s.AddFolder(ctx, held); err != nil {
		t.Fatalf("AddFolder holder: %v", err)
	}
	if got, err := s.GetFolder(ctx, "backup"); err != nil || got != held {
		t.Fatalf("GetFolder(backup) = %+v err=%v, want %+v", got, err, held)
	}
	list, err := s.ListFolders(ctx)
	if err != nil {
		t.Fatalf("ListFolders: %v", err)
	}
	if len(list) != 3 || list[0].ID != "backup" || !list[0].Holder {
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

func TestDeliverFolderKey(t *testing.T) {
	ctx := context.Background()
	s := openStore(t, openDB(t, filepath.Join(t.TempDir(), "c.db")), testNode)
	if err := s.AddFolder(ctx, Folder{ID: "f", Root: "/f", Encrypted: true}); err != nil {
		t.Fatalf("AddFolder: %v", err)
	}
	var key [MasterKeyLen]byte
	key[0] = 0x42
	if err := s.DeliverFolderKey(ctx, "f", key, 7); err != nil {
		t.Fatalf("DeliverFolderKey: %v", err)
	}
	got, gen, err := s.GetFolderKey(ctx, "f")
	if err != nil {
		t.Fatalf("GetFolderKey: %v", err)
	}
	if got != key || gen != 7 {
		t.Fatalf("got key=%x gen=%d, want key=%x gen=7", got, gen, key)
	}
	var other [MasterKeyLen]byte
	other[0] = 0x99
	if err := s.DeliverFolderKey(ctx, "f", other, 8); !errors.Is(err, ErrKeyExists) {
		t.Fatalf("redelivery err = %v, want ErrKeyExists (must not clobber)", err)
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

func TestFolderSecret(t *testing.T) {
	ctx := context.Background()
	s := openStore(t, openDB(t, filepath.Join(t.TempDir(), "c.db")), testNode)

	// Unencrypted folder: secret comes from the recovery secret.
	if err := s.AddFolder(ctx, Folder{ID: "plain", Root: "/p", ShareID: "plain"}); err != nil {
		t.Fatalf("AddFolder plain: %v", err)
	}
	if _, err := s.FolderSecret(ctx, "plain"); !errors.Is(err, ErrNoSecret) {
		t.Fatalf("FolderSecret before mint = %v, want ErrNoSecret", err)
	}
	secret, err := s.GenerateRecoverySecret(ctx, "plain")
	if err != nil {
		t.Fatalf("GenerateRecoverySecret: %v", err)
	}
	switch got, err := s.FolderSecret(ctx, "plain"); {
	case err != nil:
		t.Fatalf("FolderSecret: %v", err)
	case got != secret:
		t.Fatalf("FolderSecret = %x, want %x", got, secret)
	}
	if _, err := s.GenerateRecoverySecret(ctx, "plain"); !errors.Is(err, ErrSecretExists) {
		t.Fatalf("second GenerateRecoverySecret = %v, want ErrSecretExists", err)
	}

	// Encrypted folder: secret comes from the master key, not the recovery secret.
	if err := s.AddFolder(ctx, Folder{ID: "enc", Root: "/e", ShareID: "enc", Encrypted: true}); err != nil {
		t.Fatalf("AddFolder enc: %v", err)
	}
	key, err := s.GenerateFolderKey(ctx, "enc")
	if err != nil {
		t.Fatalf("GenerateFolderKey: %v", err)
	}
	switch got, err := s.FolderSecret(ctx, "enc"); {
	case err != nil:
		t.Fatalf("FolderSecret enc: %v", err)
	case got != key:
		t.Fatalf("FolderSecret enc = %x, want master key %x", got, key)
	}

	if _, err := s.FolderSecret(ctx, "missing"); !errors.Is(err, ErrFolderNotFound) {
		t.Fatalf("FolderSecret(missing) = %v, want ErrFolderNotFound", err)
	}

	// A delivered secret is stored once and refuses a clobber.
	if err := s.AddFolder(ctx, Folder{ID: "rx", Root: "/r", ShareID: "rx"}); err != nil {
		t.Fatalf("AddFolder rx: %v", err)
	}
	var delivered [MasterKeyLen]byte
	delivered[0] = 0x7
	if err := s.DeliverRecoverySecret(ctx, "rx", delivered); err != nil {
		t.Fatalf("DeliverRecoverySecret: %v", err)
	}
	if got, _ := s.FolderSecret(ctx, "rx"); got != delivered {
		t.Fatalf("delivered secret = %x, want %x", got, delivered)
	}
	var other [MasterKeyLen]byte
	other[0] = 0x9
	if err := s.DeliverRecoverySecret(ctx, "rx", other); !errors.Is(err, ErrSecretExists) {
		t.Fatalf("redelivery = %v, want ErrSecretExists", err)
	}
}

func TestHolderVerifier(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "c.db")
	db := openDB(t, path)
	s := openStore(t, db, testNode)

	if err := s.AddFolder(ctx, Folder{ID: "g", ShareID: "g", Encrypted: true, Holder: true}); err != nil {
		t.Fatalf("AddFolder: %v", err)
	}
	if v, err := s.GetHolderVerifier(ctx, "g"); err != nil || v != nil {
		t.Fatalf("initial verifier = %x, %v; want nil, nil", v, err)
	}

	want := bytes.Repeat([]byte{0xAB}, 32)
	if err := s.SetHolderVerifier(ctx, "g", want); err != nil {
		t.Fatalf("SetHolderVerifier: %v", err)
	}
	switch got, err := s.GetHolderVerifier(ctx, "g"); {
	case err != nil:
		t.Fatalf("GetHolderVerifier: %v", err)
	case !bytes.Equal(got, want):
		t.Fatalf("verifier = %x, want %x", got, want)
	}

	if _, err := s.GetHolderVerifier(ctx, "missing"); !errors.Is(err, ErrFolderNotFound) {
		t.Fatalf("get missing err = %v, want ErrFolderNotFound", err)
	}
	if err := s.SetHolderVerifier(ctx, "missing", want); !errors.Is(err, ErrFolderNotFound) {
		t.Fatalf("set missing err = %v, want ErrFolderNotFound", err)
	}

	// The verifier survives a reopen: a restore can happen long after the writer is gone.
	reopened := openStore(t, openDB(t, path), testNode)
	switch got, err := reopened.GetHolderVerifier(ctx, "g"); {
	case err != nil:
		t.Fatalf("GetHolderVerifier after reopen: %v", err)
	case !bytes.Equal(got, want):
		t.Fatalf("verifier after reopen = %x, want %x", got, want)
	}
}
