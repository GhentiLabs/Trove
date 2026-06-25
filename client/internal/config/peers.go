package config

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/storage"
	"github.com/GhentiLabs/Trove/pkg/identity"
)

// Peer is an authorized remote node and the shared folder ids it participates in.
type Peer struct {
	NodeID  string
	Name    string
	Folders []string
}

// AddPeer authorizes a peer; each Folders entry must name an existing share id.
func (s *Store) AddPeer(ctx context.Context, p Peer) error {
	if !identity.ValidNodeID(p.NodeID) {
		return ErrInvalidNodeID
	}
	return s.db.WithTx(ctx, func(tx *storage.Tx) error {
		var exists int
		err := tx.QueryRow(ctx, `SELECT 1 FROM peers WHERE node_id = ?`, p.NodeID).Scan(&exists)
		if err == nil {
			return ErrPeerExists
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("config: check peer: %w", err)
		}
		for _, fid := range p.Folders {
			if err := shareIDExists(ctx, tx, fid); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO peers (node_id, name, added_ms) VALUES (?, ?, ?)`,
			p.NodeID, p.Name, time.Now().UnixMilli()); err != nil {
			return fmt.Errorf("config: add peer: %w", err)
		}
		for _, fid := range p.Folders {
			if _, err := tx.Exec(ctx,
				`INSERT INTO peer_folders (node_id, folder_id) VALUES (?, ?)`,
				p.NodeID, fid); err != nil {
				return fmt.Errorf("config: add peer folder: %w", err)
			}
		}
		return nil
	})
}

func shareIDExists(ctx context.Context, tx *storage.Tx, shareID string) error {
	var one int
	err := tx.QueryRow(ctx, `SELECT 1 FROM folders WHERE share_id = ? AND share_id != ''`, shareID).Scan(&one)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return fmt.Errorf("%w: %q", ErrUnknownShareID, shareID)
	case err != nil:
		return fmt.Errorf("config: check share id: %w", err)
	}
	return nil
}

// GetPeer returns the peer with the given node id.
func (s *Store) GetPeer(ctx context.Context, nodeID string) (Peer, error) {
	rows, err := s.db.Query(ctx, `
		SELECT p.node_id, p.name, pf.folder_id
		FROM peers p
		LEFT JOIN peer_folders pf ON pf.node_id = p.node_id
		WHERE p.node_id = ?
		ORDER BY pf.folder_id`, nodeID)
	if err != nil {
		return Peer{}, fmt.Errorf("config: get peer: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var p Peer
	found := false
	for rows.Next() {
		var fid sql.NullString
		if err := rows.Scan(&p.NodeID, &p.Name, &fid); err != nil {
			return Peer{}, fmt.Errorf("config: scan peer: %w", err)
		}
		found = true
		if fid.Valid {
			p.Folders = append(p.Folders, fid.String)
		}
	}
	if err := rows.Err(); err != nil {
		return Peer{}, fmt.Errorf("config: get peer: %w", err)
	}
	if !found {
		return Peer{}, ErrPeerNotFound
	}
	return p, nil
}

// ListPeers returns all authorized peers ordered by node id.
func (s *Store) ListPeers(ctx context.Context) ([]Peer, error) {
	rows, err := s.db.Query(ctx, `
		SELECT p.node_id, p.name, pf.folder_id
		FROM peers p
		LEFT JOIN peer_folders pf ON pf.node_id = p.node_id
		ORDER BY p.node_id, pf.folder_id`)
	if err != nil {
		return nil, fmt.Errorf("config: list peers: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []Peer
	for rows.Next() {
		var nodeID, name string
		var fid sql.NullString
		if err := rows.Scan(&nodeID, &name, &fid); err != nil {
			return nil, fmt.Errorf("config: scan peer: %w", err)
		}
		if len(out) == 0 || out[len(out)-1].NodeID != nodeID {
			out = append(out, Peer{NodeID: nodeID, Name: name})
		}
		if fid.Valid {
			last := &out[len(out)-1]
			last.Folders = append(last.Folders, fid.String)
		}
	}
	return out, rows.Err()
}

// RemovePeer deauthorizes a peer and drops its folder participation.
func (s *Store) RemovePeer(ctx context.Context, nodeID string) error {
	return s.db.WithTx(ctx, func(tx *storage.Tx) error {
		res, err := tx.Exec(ctx, `DELETE FROM peers WHERE node_id = ?`, nodeID)
		if err != nil {
			return fmt.Errorf("config: remove peer: %w", err)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return ErrPeerNotFound
		}
		if _, err := tx.Exec(ctx, `DELETE FROM peer_folders WHERE node_id = ?`, nodeID); err != nil {
			return fmt.Errorf("config: remove peer folders: %w", err)
		}
		return nil
	})
}
