package membership

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/GhentiLabs/Trove/pkg/identity"

	"github.com/GhentiLabs/Trove/client/internal/storage"
)

var (
	// ErrNotWriter is returned when the local node tries to add a member to a group it
	// is not a writer of.
	ErrNotWriter = errors.New("membership: local node is not a writer of this group")
	// ErrUnknownNetwork is returned for an operation on a network this node has not
	// founded or joined.
	ErrUnknownNetwork = errors.New("membership: unknown network")
	// ErrNodeMismatch is returned when the store's node id does not match its key.
	ErrNodeMismatch = errors.New("membership: node id does not match key")
)

const schema = `
CREATE TABLE IF NOT EXISTS networks (
	network_id TEXT PRIMARY KEY,
	joined_ms  INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS members (
	network_id  TEXT    NOT NULL,
	node_id     TEXT    NOT NULL,
	public_key  BLOB    NOT NULL,
	role        INTEGER NOT NULL,
	added_by    TEXT    NOT NULL,
	added_at_ms INTEGER NOT NULL,
	sig         BLOB    NOT NULL,
	PRIMARY KEY (network_id, node_id)
) WITHOUT ROWID;`

// Store is the network roster: networks this node belongs to and their verified,
// signed member sets.
type Store struct {
	db   *storage.DB
	node string
	key  ed25519.PrivateKey
	pub  ed25519.PublicKey
}

// Options configures Open.
type Options struct {
	DB     *storage.DB
	NodeID string
	Key    ed25519.PrivateKey
}

// Open ensures the schema and binds the store to the node's signing key.
func Open(opts Options) (*Store, error) {
	if opts.DB == nil {
		return nil, errors.New("membership: nil database")
	}
	pub, ok := opts.Key.Public().(ed25519.PublicKey)
	if !ok {
		return nil, errors.New("membership: key is not Ed25519")
	}
	id, err := identity.FingerprintKey(pub)
	if err != nil {
		return nil, err
	}
	if id != opts.NodeID {
		return nil, ErrNodeMismatch
	}
	s := &Store{db: opts.DB, node: opts.NodeID, key: opts.Key, pub: pub}
	if _, err := s.db.Exec(context.Background(), schema); err != nil {
		return nil, fmt.Errorf("membership: schema: %w", err)
	}
	return s, nil
}

// groupSep separates a group's founder id from its per-group suffix. It is absent from
// the base32 node-id alphabet, so the split is unambiguous.
const groupSep = "."

// mintGroupID derives a fresh per-folder group id that commits to its founder: the
// founder's node id, a separator, and random bytes. Distinct folders of one founder get
// distinct ids, while any peer can recover the founder from the id alone.
func mintGroupID(founder string) (string, error) {
	var b [10]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("membership: mint group id: %w", err)
	}
	return founder + groupSep + hex.EncodeToString(b[:]), nil
}

// Founder returns the node id that founded groupID, recovered from the id itself, and
// whether groupID is well-formed.
func Founder(groupID string) (string, bool) {
	founder, suffix, ok := strings.Cut(groupID, groupSep)
	if !ok || suffix == "" || !identity.ValidNodeID(founder) {
		return "", false
	}
	return founder, true
}

// Found creates a group with this node as the founding writer and returns its group id.
// The genesis entry is self-signed.
func (s *Store) Found(ctx context.Context) (string, error) {
	networkID, err := mintGroupID(s.node)
	if err != nil {
		return "", err
	}
	entry, err := Sign(s.key, Entry{
		NetworkID: networkID, NodeID: s.node, PublicKey: s.pub,
		Role: RoleWriter, AddedBy: s.node, AddedAtMs: time.Now().UnixMilli(),
	})
	if err != nil {
		return "", err
	}
	err = s.db.WithTx(ctx, func(tx *storage.Tx) error {
		if err := joinTx(ctx, tx, networkID); err != nil {
			return err
		}
		return writeEntry(ctx, tx, entry)
	})
	if err != nil {
		return "", err
	}
	return networkID, nil
}

// Join records a network this node trusts (learned out-of-band by its network_id);
// the roster fills in via gossip once an admin has added this node.
func (s *Store) Join(ctx context.Context, networkID string) error {
	return s.db.WithTx(ctx, func(tx *storage.Tx) error { return joinTx(ctx, tx, networkID) })
}

// Add admits a member to a group this node is a writer of, signing the entry with the
// local key, and returns it for gossip.
func (s *Store) Add(ctx context.Context, networkID, nodeID string, publicKey []byte, role Role) (Entry, error) {
	boundID, err := identity.FingerprintKey(publicKey)
	if err != nil {
		return Entry{}, fmt.Errorf("%w: %w", ErrInvalidEntry, err)
	}
	if boundID != nodeID {
		return Entry{}, ErrKeyMismatch
	}
	var out Entry
	err = s.db.WithTx(ctx, func(tx *storage.Tx) error {
		self, err := loadEntry(ctx, tx, networkID, s.node)
		if err != nil {
			return err
		}
		if self == nil || self.Role != RoleWriter {
			return ErrNotWriter
		}
		entry, err := Sign(s.key, Entry{
			NetworkID: networkID, NodeID: nodeID, PublicKey: publicKey,
			Role: role, AddedBy: s.node, AddedAtMs: time.Now().UnixMilli(),
		})
		if err != nil {
			return err
		}
		if err := writeEntry(ctx, tx, entry); err != nil {
			return err
		}
		out = entry
		return nil
	})
	return out, err
}

// Merge verifies incoming entries against the stored roster and the network root,
// stores the valid, and returns the newly added entries (for re-gossip). Entries are
// add-only; an entry whose chain cannot be verified is dropped.
func (s *Store) Merge(ctx context.Context, networkID string, entries []Entry) ([]Entry, error) {
	// Read the current roster and verify the additions WITHOUT holding the write lock:
	// per-entry Ed25519 verification must not stall every other writer of this database.
	known, err := networkKnown(ctx, s.db, networkID)
	if err != nil || !known {
		return nil, err
	}
	roster, err := loadRoster(ctx, s.db, networkID)
	if err != nil {
		return nil, err
	}
	added := verifyAdditions(networkID, roster, entries)
	if len(added) == 0 {
		return nil, nil
	}

	// Persist the verified entries in a short transaction. Add-only with ON CONFLICT DO
	// NOTHING makes a concurrent write between the read and here harmless.
	err = s.db.WithTx(ctx, func(tx *storage.Tx) error {
		for _, e := range added {
			if err := writeEntry(ctx, tx, e); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return added, nil
}

// verifyAdditions returns the entries whose trust chain verifies against the known
// roster, resolving out-of-order delivery by fixpoint. It performs no I/O.
func verifyAdditions(networkID string, roster, entries []Entry) []Entry {
	accepted := make(map[string]struct{}, len(roster))
	writers := make(map[string]ed25519.PublicKey, len(roster))
	for _, e := range roster {
		accepted[e.NodeID] = struct{}{}
		if e.Role == RoleWriter {
			writers[e.NodeID] = e.PublicKey
		}
	}
	pending := make([]Entry, 0, len(entries))
	for _, e := range entries {
		if _, ok := accepted[e.NodeID]; !ok {
			pending = append(pending, e)
		}
	}
	var added []Entry
	for {
		progress := false
		rest := pending[:0]
		for _, e := range pending {
			if verifyChain(networkID, e, writers) {
				accepted[e.NodeID] = struct{}{}
				if e.Role == RoleWriter {
					writers[e.NodeID] = e.PublicKey
				}
				added = append(added, e)
				progress = true
			} else {
				rest = append(rest, e)
			}
		}
		pending = rest
		if !progress || len(pending) == 0 {
			break
		}
	}
	return added
}

// verifyChain reports whether e is a valid roster entry: the founder's self-signed
// genesis for the group, or an entry signed by an already-verified writer.
func verifyChain(networkID string, e Entry, writers map[string]ed25519.PublicKey) bool {
	if e.NetworkID != networkID {
		return false
	}
	if e.AddedBy == e.NodeID {
		founder, ok := Founder(networkID)
		if !ok || e.NodeID != founder || e.Role != RoleWriter {
			return false
		}
		return e.VerifySig(e.PublicKey) == nil
	}
	signer, ok := writers[e.AddedBy]
	if !ok {
		return false
	}
	return e.VerifySig(signer) == nil
}

// Roster returns the verified member set of a network, ordered by node id.
func (s *Store) Roster(ctx context.Context, networkID string) ([]Entry, error) {
	return loadRoster(ctx, s.db, networkID)
}

// IsMember reports whether nodeID is in networkID's verified roster.
func (s *Store) IsMember(ctx context.Context, networkID, nodeID string) (bool, error) {
	e, err := loadEntry(ctx, s.db, networkID, nodeID)
	return e != nil, err
}

// Networks returns the ids of every network this node belongs to.
func (s *Store) Networks(ctx context.Context) ([]string, error) {
	rows, err := s.db.Query(ctx, `SELECT network_id FROM networks ORDER BY network_id`)
	if err != nil {
		return nil, fmt.Errorf("membership: list networks: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("membership: scan network: %w", err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

type querier interface {
	QueryRow(ctx context.Context, query string, args ...any) *sql.Row
	Query(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func joinTx(ctx context.Context, tx *storage.Tx, networkID string) error {
	if networkID == "" {
		return fmt.Errorf("%w: empty network id", ErrUnknownNetwork)
	}
	_, err := tx.Exec(ctx, `INSERT OR IGNORE INTO networks (network_id, joined_ms) VALUES (?, ?)`, networkID, time.Now().UnixMilli())
	if err != nil {
		return fmt.Errorf("membership: join network: %w", err)
	}
	return nil
}

func networkKnown(ctx context.Context, q querier, networkID string) (bool, error) {
	var one int
	err := q.QueryRow(ctx, `SELECT 1 FROM networks WHERE network_id = ?`, networkID).Scan(&one)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("membership: lookup network: %w", err)
	}
	return true, nil
}

func writeEntry(ctx context.Context, tx *storage.Tx, e Entry) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO members (network_id, node_id, public_key, role, added_by, added_at_ms, sig)
		 VALUES (?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(network_id, node_id) DO NOTHING`,
		e.NetworkID, e.NodeID, e.PublicKey, int(e.Role), e.AddedBy, e.AddedAtMs, e.Sig)
	if err != nil {
		return fmt.Errorf("membership: write entry: %w", err)
	}
	return nil
}

func loadEntry(ctx context.Context, q querier, networkID, nodeID string) (*Entry, error) {
	var (
		e    Entry
		role int
	)
	err := q.QueryRow(ctx,
		`SELECT network_id, node_id, public_key, role, added_by, added_at_ms, sig FROM members WHERE network_id = ? AND node_id = ?`,
		networkID, nodeID).Scan(&e.NetworkID, &e.NodeID, &e.PublicKey, &role, &e.AddedBy, &e.AddedAtMs, &e.Sig)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("membership: load entry: %w", err)
	}
	e.Role = Role(role)
	return &e, nil
}

func loadRoster(ctx context.Context, q querier, networkID string) ([]Entry, error) {
	rows, err := q.Query(ctx,
		`SELECT network_id, node_id, public_key, role, added_by, added_at_ms, sig FROM members WHERE network_id = ? ORDER BY node_id`,
		networkID)
	if err != nil {
		return nil, fmt.Errorf("membership: load roster: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []Entry
	for rows.Next() {
		var (
			e    Entry
			role int
		)
		if err := rows.Scan(&e.NetworkID, &e.NodeID, &e.PublicKey, &role, &e.AddedBy, &e.AddedAtMs, &e.Sig); err != nil {
			return nil, fmt.Errorf("membership: scan entry: %w", err)
		}
		e.Role = Role(role)
		out = append(out, e)
	}
	return out, rows.Err()
}
