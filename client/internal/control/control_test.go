package control

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"
)

// fakeBackend records calls and returns canned data, failing per-folder lookups
// for ids it does not know.
type fakeBackend struct {
	status  Status
	peers   Peers
	found   FoundRequest
	joined  JoinRequest
	invited map[string]InviteRequest
	removed map[string]bool
	quotas  map[string]int64
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{
		status: Status{
			NodeID: "node-a",
			Folders: []Folder{{
				ID: "g1", Role: "owner", Root: "/docs", KeepHistory: true, Synced: true,
				UsedBytes: 42, QuotaBytes: 1000,
				Receipts: []Receipt{{PeerID: "node-b", HighWater: 7, SyncedAt: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)}},
			}},
		},
		peers:   Peers{Active: []Peer{{NodeID: "node-b", Folders: []string{"g1"}}}},
		invited: map[string]InviteRequest{},
		removed: map[string]bool{},
		quotas:  map[string]int64{},
	}
}

func (f *fakeBackend) Identity(context.Context) (Identity, error) {
	return Identity{NodeID: "node-a", PublicKey: "aabb"}, nil
}

func (f *fakeBackend) Status(context.Context) (Status, error) { return f.status, nil }
func (f *fakeBackend) Peers(context.Context) (Peers, error)   { return f.peers, nil }

func (f *fakeBackend) Found(_ context.Context, req FoundRequest) (FoundResponse, error) {
	if req.Root == "" {
		return FoundResponse{}, fmt.Errorf("%w: root is required", ErrInvalid)
	}
	f.found = req
	return FoundResponse{GroupID: "g-new", RecoveryCode: "code"}, nil
}

func (f *fakeBackend) Join(_ context.Context, req JoinRequest) error {
	if req.GroupID == "taken" {
		return fmt.Errorf("%w: folder", ErrExists)
	}
	f.joined = req
	return nil
}

func (f *fakeBackend) Invite(_ context.Context, folderID string, req InviteRequest) error {
	if folderID != "g1" {
		return fmt.Errorf("%w: folder %q", ErrNotFound, folderID)
	}
	f.invited[folderID] = req
	return nil
}

func (f *fakeBackend) SetQuota(_ context.Context, folderID string, quota int64) (Folder, error) {
	if folderID != "g1" {
		return Folder{}, fmt.Errorf("%w: folder %q", ErrNotFound, folderID)
	}
	f.quotas[folderID] = quota
	return Folder{ID: folderID, QuotaBytes: quota}, nil
}

func (f *fakeBackend) Remove(_ context.Context, folderID string, purge bool) error {
	if folderID != "g1" {
		return fmt.Errorf("%w: folder %q", ErrNotFound, folderID)
	}
	f.removed[folderID] = purge
	return nil
}

// startServer serves the API on a real unix socket and returns a client for it.
func startServer(t *testing.T, b Backend) *Client {
	t.Helper()
	dir := t.TempDir()
	ln, err := net.Listen("unix", filepath.Join(dir, SocketName))
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := &http.Server{Handler: New(b, nil).Handler()}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return Dial(dir)
}

func TestClientServerRoundTrip(t *testing.T) {
	ctx := context.Background()
	b := newFakeBackend()
	c := startServer(t, b)

	id, err := c.Identity(ctx)
	if err != nil || id.NodeID != "node-a" || id.PublicKey != "aabb" {
		t.Fatalf("Identity = %+v, %v", id, err)
	}

	st, err := c.Status(ctx)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if len(st.Folders) != 1 || st.Folders[0].ID != "g1" || st.Folders[0].UsedBytes != 42 {
		t.Fatalf("Status folders = %+v", st.Folders)
	}
	if got := st.Folders[0].Receipts; len(got) != 1 || got[0].PeerID != "node-b" || !got[0].SyncedAt.Equal(time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("receipts = %+v", got)
	}

	peers, err := c.Peers(ctx)
	if err != nil || len(peers.Active) != 1 || peers.Active[0].NodeID != "node-b" {
		t.Fatalf("Peers = %+v, %v", peers, err)
	}

	resp, err := c.Found(ctx, FoundRequest{Root: "/new", Encrypted: true, KeepHistory: true, QuotaBytes: 5})
	if err != nil || resp.GroupID != "g-new" || resp.RecoveryCode != "code" {
		t.Fatalf("Found = %+v, %v", resp, err)
	}
	if b.found.Root != "/new" || !b.found.Encrypted || b.found.QuotaBytes != 5 {
		t.Fatalf("backend saw found = %+v", b.found)
	}

	if err := c.Join(ctx, JoinRequest{GroupID: "g2", Root: "/r"}); err != nil {
		t.Fatalf("Join: %v", err)
	}
	if b.joined.GroupID != "g2" {
		t.Fatalf("backend saw join = %+v", b.joined)
	}

	if err := c.Invite(ctx, "g1", InviteRequest{NodeID: "node-c", PublicKey: "cc", Role: "writer"}); err != nil {
		t.Fatalf("Invite: %v", err)
	}
	if got := b.invited["g1"]; got.Role != "writer" || got.NodeID != "node-c" {
		t.Fatalf("backend saw invite = %+v", got)
	}

	f, err := c.SetQuota(ctx, "g1", 777)
	if err != nil || f.QuotaBytes != 777 {
		t.Fatalf("SetQuota = %+v, %v", f, err)
	}

	if err := c.Remove(ctx, "g1", true); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if purge, ok := b.removed["g1"]; !ok || !purge {
		t.Fatalf("backend saw remove purge=%v ok=%v", purge, ok)
	}
}

func TestErrorMapping(t *testing.T) {
	ctx := context.Background()
	c := startServer(t, newFakeBackend())

	wantCode := func(err error, code string) {
		t.Helper()
		var e *Error
		if !errors.As(err, &e) || e.Code != code {
			t.Fatalf("err = %v, want code %q", err, code)
		}
	}

	_, err := c.Found(ctx, FoundRequest{})
	wantCode(err, codeBadRequest)

	err = c.Join(ctx, JoinRequest{GroupID: "taken"})
	wantCode(err, codeConflict)

	err = c.Invite(ctx, "missing", InviteRequest{})
	wantCode(err, codeNotFound)

	_, err = c.SetQuota(ctx, "missing", 1)
	wantCode(err, codeNotFound)

	err = c.Remove(ctx, "missing", false)
	wantCode(err, codeNotFound)
}

func TestUnknownRouteAndBadBody(t *testing.T) {
	c := startServer(t, newFakeBackend())

	resp, err := c.http.Get("http://trove/v1/nope")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown route status = %d, want 404", resp.StatusCode)
	}

	req, _ := http.NewRequest(http.MethodPost, "http://trove/v1/folders", nil)
	resp, err = c.http.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("empty body status = %d, want 400", resp.StatusCode)
	}
}
