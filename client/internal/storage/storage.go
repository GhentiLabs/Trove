// Package storage is the client's SQLite boundary: databases are opened here with
// the standard WAL pragmas and accessed through Exec/Query or WithTx, so higher
// layers never touch the driver.
package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"

	_ "modernc.org/sqlite"
)

// DB is a handle to a single SQLite database file. Reads use the connection pool
// directly; writes are serialized through WithTx.
type DB struct {
	sdb     *sql.DB
	path    string
	writeMu sync.Mutex
}

// Options configures Open. MaxOpenConns below 1 defaults to 1.
type Options struct {
	Path         string
	MaxOpenConns int
}

// Open opens, creating if needed, the database at opts.Path.
func Open(opts Options) (*DB, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_txlock=immediate", opts.Path)
	sdb, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("storage: open %s: %w", opts.Path, err)
	}
	sdb.SetMaxOpenConns(max(opts.MaxOpenConns, 1))
	if err := sdb.Ping(); err != nil {
		_ = sdb.Close()
		return nil, fmt.Errorf("storage: ping %s: %w", opts.Path, err)
	}
	// The database may hold secrets (folder master keys), so keep it owner-only.
	// Sidecars are created lazily; chmod those that already exist.
	for _, p := range []string{opts.Path, opts.Path + "-wal", opts.Path + "-shm"} {
		if err := os.Chmod(p, 0o600); err != nil && !errors.Is(err, os.ErrNotExist) {
			_ = sdb.Close()
			return nil, fmt.Errorf("storage: chmod %s: %w", p, err)
		}
	}
	return &DB{sdb: sdb, path: opts.Path}, nil
}

// Path returns the database file path.
func (db *DB) Path() string { return db.path }

// Close closes the database and its connection pool.
func (db *DB) Close() error { return db.sdb.Close() }

// Exec runs a statement outside an explicit transaction.
func (db *DB) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return db.sdb.ExecContext(ctx, query, args...)
}

// Query runs a query outside an explicit transaction.
func (db *DB) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return db.sdb.QueryContext(ctx, query, args...)
}

// QueryRow runs a single-row query outside an explicit transaction.
func (db *DB) QueryRow(ctx context.Context, query string, args ...any) *sql.Row {
	return db.sdb.QueryRowContext(ctx, query, args...)
}

// WithTx runs fn in a transaction, committing if fn returns nil and rolling back
// on error or panic.
func (db *DB) WithTx(ctx context.Context, fn func(tx *Tx) error) (err error) {
	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	stx, err := db.sdb.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("storage: begin: %w", err)
	}
	defer func() {
		if p := recover(); p != nil {
			_ = stx.Rollback()
			panic(p)
		}
	}()
	if err := fn(&Tx{tx: stx}); err != nil {
		_ = stx.Rollback()
		return err
	}
	if err := stx.Commit(); err != nil {
		return fmt.Errorf("storage: commit: %w", err)
	}
	return nil
}

// CheckMeta reads meta[key] within tx: if absent it inserts want, otherwise it
// passes the stored value to validate. It is the shared get-or-bind pattern store
// packages use for schema-version and identity rows in their meta(key, value)
// tables.
func CheckMeta(ctx context.Context, tx *Tx, key, want string, validate func(got string) error) error {
	var got string
	err := tx.QueryRow(ctx, `SELECT value FROM meta WHERE key = ?`, key).Scan(&got)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if _, err := tx.Exec(ctx, `INSERT INTO meta (key, value) VALUES (?, ?)`, key, want); err != nil {
			return fmt.Errorf("storage: set %s: %w", key, err)
		}
		return nil
	case err != nil:
		return fmt.Errorf("storage: read %s: %w", key, err)
	default:
		return validate(got)
	}
}

// Tx is a transaction handle passed to a WithTx callback.
type Tx struct {
	tx *sql.Tx
}

// Exec runs a statement within the transaction.
func (t *Tx) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return t.tx.ExecContext(ctx, query, args...)
}

// Query runs a query within the transaction.
func (t *Tx) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return t.tx.QueryContext(ctx, query, args...)
}

// QueryRow runs a single-row query within the transaction.
func (t *Tx) QueryRow(ctx context.Context, query string, args ...any) *sql.Row {
	return t.tx.QueryRowContext(ctx, query, args...)
}
