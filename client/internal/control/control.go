// Package control is the daemon's local management surface: an HTTP API served
// over a unix socket in the state dir, used by the trove-peer CLI (and any future
// UI). Authorization is the socket file's permissions.
package control

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Sentinel errors a Backend returns to drive the HTTP status mapping.
var (
	ErrNotFound = errors.New("control: not found")
	ErrExists   = errors.New("control: already exists")
	ErrInvalid  = errors.New("control: invalid request")
)

// Error is the client-facing error envelope.
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *Error) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Message) }

// Identity names the daemon's node.
type Identity struct {
	NodeID    string `json:"node_id"`
	PublicKey string `json:"public_key"`
}

// Receipt records the newest completed sync with one peer.
type Receipt struct {
	PeerID    string    `json:"peer_id"`
	HighWater int64     `json:"high_water"`
	SyncedAt  time.Time `json:"synced_at"`
}

// Folder is one configured folder's state.
type Folder struct {
	ID          string `json:"id"`
	Role        string `json:"role"`
	Root        string `json:"root,omitempty"`
	Encrypted   bool   `json:"encrypted"`
	Holder      bool   `json:"holder"`
	KeepHistory bool   `json:"keep_history"`
	QuotaBytes  int64  `json:"quota_bytes"`
	// Synced reports the folder's live stores were readable; UsedBytes and
	// Receipts are only meaningful when true.
	Synced       bool      `json:"synced"`
	UsedBytes    int64     `json:"used_bytes"`
	Receipts     []Receipt `json:"receipts,omitempty"`
	QuotaWarning string    `json:"quota_warning,omitempty"`
}

// Status is the daemon's folder-level view.
type Status struct {
	NodeID  string   `json:"node_id"`
	Folders []Folder `json:"folders"`
}

// Peer is one live session.
type Peer struct {
	NodeID  string   `json:"node_id"`
	Folders []string `json:"folders,omitempty"`
}

// Peers lists the live sessions.
type Peers struct {
	Active []Peer `json:"active"`
}

// FoundRequest creates a folder group this node owns.
type FoundRequest struct {
	Root        string `json:"root"`
	Encrypted   bool   `json:"encrypted"`
	KeepHistory bool   `json:"keep_history"`
	QuotaBytes  int64  `json:"quota_bytes"`
}

// FoundResponse returns the minted group and its recovery code.
type FoundResponse struct {
	GroupID      string `json:"group_id"`
	RecoveryCode string `json:"recovery_code"`
}

// JoinRequest joins a group founded elsewhere.
type JoinRequest struct {
	GroupID     string `json:"group_id"`
	Root        string `json:"root"`
	Encrypted   bool   `json:"encrypted"`
	Holder      bool   `json:"holder"`
	KeepHistory bool   `json:"keep_history"`
	QuotaBytes  int64  `json:"quota_bytes"`
}

// InviteRequest admits a member to a group this node may write to.
type InviteRequest struct {
	NodeID    string `json:"node_id"`
	PublicKey string `json:"public_key"`
	Role      string `json:"role"`
}

// QuotaRequest changes a folder's storage cap; 0 is unlimited.
type QuotaRequest struct {
	QuotaBytes int64 `json:"quota_bytes"`
}

// Backend is the daemon surface the HTTP handlers drive. Implementations map
// domain failures onto the sentinel errors above; anything else is internal.
type Backend interface {
	Identity(ctx context.Context) (Identity, error)
	Status(ctx context.Context) (Status, error)
	Peers(ctx context.Context) (Peers, error)
	Found(ctx context.Context, req FoundRequest) (FoundResponse, error)
	Join(ctx context.Context, req JoinRequest) error
	Invite(ctx context.Context, folderID string, req InviteRequest) error
	SetQuota(ctx context.Context, folderID string, quota int64) (Folder, error)
	Remove(ctx context.Context, folderID string, purge bool) error
}
