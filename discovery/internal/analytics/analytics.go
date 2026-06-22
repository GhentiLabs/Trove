// Package analytics persists client-reported telemetry in its own SQLite
// database, isolated from the discovery hot path. Records are stored verbatim:
// the open-ended field set is kept as JSON so the schema can grow without
// migrations. A disk-usage cap stops ingestion before the disk fills.
package analytics

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	_ "modernc.org/sqlite"
)

// ErrDiskFull is returned when the analytics database has reached its
// configured disk-usage cap. The server stops accepting new analytics rather
// than risk filling the disk.
var ErrDiskFull = errors.New("analytics: disk cap reached")

// Record is a single telemetry submission. Fields is stored as JSON.
type Record struct {
	NodeID        string
	InstallID     string
	SchemaVersion int
	EventMillis   int64
	SourceIP      string
	Fields        map[string]any
}

// Store is the analytics database handle.
type Store struct {
	db           *sql.DB
	path         string
	diskCapBytes int64
	clock        func() time.Time
}

const schema = `
CREATE TABLE IF NOT EXISTS analytics (
	id              INTEGER PRIMARY KEY AUTOINCREMENT,
	node_id         TEXT    NOT NULL,
	install_id      TEXT    NOT NULL,
	schema_version  INTEGER NOT NULL,
	event_millis    INTEGER NOT NULL,
	received_millis INTEGER NOT NULL,
	source_ip       TEXT    NOT NULL,
	fields          TEXT    NOT NULL
);`

// Open opens (creating if needed) the analytics database at path. A clock may
// be injected for tests; nil uses the wall clock.
func Open(path string, diskCapBytes int64, clock func() time.Time) (*Store, error) {
	if clock == nil {
		clock = time.Now
	}
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("analytics: open: %w", err)
	}
	// One writer connection avoids "database is locked" under WAL for this
	// low-volume, write-mostly workload.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("analytics: schema: %w", err)
	}
	return &Store{db: db, path: path, diskCapBytes: diskCapBytes, clock: clock}, nil
}

// Insert persists a record. It returns ErrDiskFull when the cap is reached.
func (s *Store) Insert(ctx context.Context, rec Record) error {
	if used, err := s.diskUsage(); err != nil {
		return fmt.Errorf("analytics: disk usage: %w", err)
	} else if used >= s.diskCapBytes {
		return ErrDiskFull
	}

	fields := rec.Fields
	if fields == nil {
		fields = map[string]any{}
	}
	encoded, err := json.Marshal(fields)
	if err != nil {
		return fmt.Errorf("analytics: encode fields: %w", err)
	}

	_, err = s.db.ExecContext(ctx,
		`INSERT INTO analytics
			(node_id, install_id, schema_version, event_millis, received_millis, source_ip, fields)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		rec.NodeID, rec.InstallID, rec.SchemaVersion, rec.EventMillis,
		s.clock().UnixMilli(), rec.SourceIP, string(encoded))
	if err != nil {
		return fmt.Errorf("analytics: insert: %w", err)
	}
	return nil
}

// Count returns the number of stored records.
func (s *Store) Count(ctx context.Context) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM analytics`).Scan(&n)
	return n, err
}

// Read returns all stored records ordered by insertion. It is intended for
// tests and operational inspection, not a hot path.
func (s *Store) Read(ctx context.Context) ([]Record, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT node_id, install_id, schema_version, event_millis, source_ip, fields
		 FROM analytics ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []Record
	for rows.Next() {
		var (
			rec    Record
			fields string
		)
		if err := rows.Scan(&rec.NodeID, &rec.InstallID, &rec.SchemaVersion, &rec.EventMillis, &rec.SourceIP, &fields); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(fields), &rec.Fields); err != nil {
			return nil, fmt.Errorf("analytics: decode fields: %w", err)
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }

// diskUsage sums the database file and its WAL sidecars. Missing files (e.g.
// before first write) count as zero.
func (s *Store) diskUsage() (int64, error) {
	var total int64
	for _, suffix := range []string{"", "-wal", "-shm"} {
		info, err := os.Stat(s.path + suffix)
		switch {
		case err == nil:
			total += info.Size()
		case errors.Is(err, os.ErrNotExist):
		default:
			return 0, err
		}
	}
	return total, nil
}
