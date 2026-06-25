package config

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/GhentiLabs/Trove/client/internal/storage"
)

const v1Schema = `
CREATE TABLE meta (
	key   TEXT PRIMARY KEY,
	value TEXT NOT NULL
);
CREATE TABLE folders (
	id          TEXT PRIMARY KEY,
	root        TEXT    NOT NULL,
	encrypted   INTEGER NOT NULL DEFAULT 0,
	master_key  BLOB,
	kdf_salt    BLOB,
	kdf_time    INTEGER,
	kdf_mem_kib INTEGER,
	kdf_threads INTEGER,
	created_ms  INTEGER NOT NULL
);`

func TestMigrateV1ToV2(t *testing.T) {
	ctx := context.Background()
	db := openDB(t, filepath.Join(t.TempDir(), "c.db"))

	err := db.WithTx(ctx, func(tx *storage.Tx) error {
		if _, err := tx.Exec(ctx, v1Schema); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO meta (key, value) VALUES ('schema_version', '1')`); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO meta (key, value) VALUES ('node_id', ?)`, testNode); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO folders (id, root, encrypted, created_ms) VALUES ('docs', '/d', 0, 1)`)
		return err
	})
	if err != nil {
		t.Fatalf("seed v1: %v", err)
	}

	s := openStore(t, db, testNode)

	f, err := s.GetFolder(ctx, "docs")
	if err != nil {
		t.Fatalf("GetFolder after migrate: %v", err)
	}
	if f.Root != "/d" || f.ShareID != "" {
		t.Fatalf("migrated folder = %+v", f)
	}

	if err := s.SetFolderShareID(ctx, "docs", "docs-share"); err != nil {
		t.Fatalf("SetFolderShareID after migrate: %v", err)
	}

	var v int
	if err := db.QueryRow(ctx, `SELECT value FROM meta WHERE key = 'schema_version'`).Scan(&v); err != nil {
		t.Fatalf("read version: %v", err)
	}
	if v != SchemaVersion {
		t.Fatalf("schema_version after migrate = %d, want %d", v, SchemaVersion)
	}
}
