package membership

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/GhentiLabs/Trove/pkg/identity"

	"github.com/GhentiLabs/Trove/client/internal/storage"
)

var (
	// ErrNotAdmin is returned when the local node tries to add a member to a network
	// it is not an admin of.
	ErrNotAdmin = errors.New("membership: local node is not an admin of this network")
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

// Found creates a network with this node as the founding admin and returns its
// network_id (this node's id). The genesis entry is self-signed.
func (s *Store) Found(ctx context.Context) (string, error) {
	networkID := s.node
	entry, err := Sign(s.key, Entry{
		NetworkID: networkID, NodeID: s.node, PublicKey: s.pub,
		Role: RoleAdmin, AddedBy: s.node, AddedAtMs: time.Now().UnixMilli(),
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

// Add admits a member to a network this node administers, signing the entry with the
// local key, and returns it for gossip.
func (s *Store) Add(ctx context.Context, networkID, nodeID string, publicKey []byte, role Role) (Entry, error) {
	var out Entry
	err := s.db.WithTx(ctx, func(tx *storage.Tx) error {
		admin, err := loadEntry(ctx, tx, networkID, s.node)
		if err != nil {
			return err
		}
		if admin == nil || admin.Role != RoleAdmin {
			return ErrNotAdmin
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
// add-only in C1; an entry whose chain cannot be verified is dropped.
func (s *Store) Merge(ctx context.Context, networkID string, entries []Entry) ([]Entry, error) {
	var added []Entry
	err := s.db.WithTx(ctx, func(tx *storage.Tx) error {
		known, err := networkKnown(ctx, tx, networkID)
		if err != nil || !known {
			return err
		}
		roster, err := loadRoster(ctx, tx, networkID)
		if err != nil {
			return err
		}
		accepted := make(map[string]Entry, len(roster))
		admins := make(map[string]ed25519.PublicKey, len(roster))
		for _, e := range roster {
			accepted[e.NodeID] = e
			if e.Role == RoleAdmin {
				admins[e.NodeID] = e.PublicKey
			}
		}

		pending := make([]Entry, 0, len(entries))
		for _, e := range entries {
			if _, ok := accepted[e.NodeID]; !ok {
				pending = append(pending, e)
			}
		}
		for {
			progress := false
			rest := pending[:0]
			for _, e := range pending {
				if verifyChain(networkID, e, admins) {
					accepted[e.NodeID] = e
					if e.Role == RoleAdmin {
						admins[e.NodeID] = e.PublicKey
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

// verifyChain reports whether e is a valid roster entry: the genesis self-signature
// for the network root, or an entry signed by an already-verified admin.
func verifyChain(networkID string, e Entry, admins map[string]ed25519.PublicKey) bool {
	if e.NetworkID != networkID {
		return false
	}
	if e.AddedBy == e.NodeID {
		if e.NodeID != networkID || e.Role != RoleAdmin {
			return false
		}
		return e.VerifySig(e.PublicKey) == nil
	}
	signer, ok := admins[e.AddedBy]
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
