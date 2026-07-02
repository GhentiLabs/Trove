package control

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
)

// SocketName is the control socket's file name inside the state dir.
const SocketName = "control.sock"

// SocketPath is the control socket path for a state dir.
func SocketPath(stateDir string) string {
	return filepath.Join(stateDir, SocketName)
}

// Client drives a running daemon's control API over its unix socket.
type Client struct {
	http *http.Client
}

// Dial builds a client for the daemon owning stateDir. Connection failures
// surface on the first call.
func Dial(stateDir string) *Client {
	path := SocketPath(stateDir)
	return &Client{http: &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", path)
			},
		},
	}}
}

func (c *Client) Identity(ctx context.Context) (Identity, error) {
	var out Identity
	err := c.do(ctx, http.MethodGet, "/v1/identity", nil, &out)
	return out, err
}

func (c *Client) Status(ctx context.Context) (Status, error) {
	var out Status
	err := c.do(ctx, http.MethodGet, "/v1/status", nil, &out)
	return out, err
}

func (c *Client) Peers(ctx context.Context) (Peers, error) {
	var out Peers
	err := c.do(ctx, http.MethodGet, "/v1/peers", nil, &out)
	return out, err
}

func (c *Client) Found(ctx context.Context, req FoundRequest) (FoundResponse, error) {
	var out FoundResponse
	err := c.do(ctx, http.MethodPost, "/v1/folders", req, &out)
	return out, err
}

func (c *Client) Join(ctx context.Context, req JoinRequest) error {
	return c.do(ctx, http.MethodPost, "/v1/folders/join", req, nil)
}

func (c *Client) Invite(ctx context.Context, folderID string, req InviteRequest) error {
	return c.do(ctx, http.MethodPost, "/v1/folders/"+folderID+"/invite", req, nil)
}

func (c *Client) SetQuota(ctx context.Context, folderID string, quota int64) (Folder, error) {
	var out Folder
	err := c.do(ctx, http.MethodPatch, "/v1/folders/"+folderID, QuotaRequest{QuotaBytes: quota}, &out)
	return out, err
}

func (c *Client) Remove(ctx context.Context, folderID string, purge bool) error {
	path := "/v1/folders/" + folderID
	if purge {
		path += "?purge=true"
	}
	return c.do(ctx, http.MethodDelete, path, nil, nil)
}

func (c *Client) do(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	// The host is ignored: the transport always dials the socket.
	req, err := http.NewRequestWithContext(ctx, method, "http://trove"+path, body)
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 400 {
		var e Error
		if err := json.NewDecoder(resp.Body).Decode(&e); err != nil || e.Code == "" {
			return fmt.Errorf("control: %s %s: status %d", method, path, resp.StatusCode)
		}
		return &e
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
