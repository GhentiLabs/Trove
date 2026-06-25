// Package discovery is the client of the Trove discovery server.
package discovery

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	disco "github.com/GhentiLabs/Trove/pkg/discovery"
	"github.com/GhentiLabs/Trove/pkg/identity"
)

const httpTimeout = 10 * time.Second

// ErrPeerNotFound is returned by Lookup when the target node has no registration.
var ErrPeerNotFound = errors.New("discovery: peer not found")

// Options configures New.
type Options struct {
	Server string
	Cert   tls.Certificate
	Logger *slog.Logger
}

// Client talks to one Trove discovery server.
type Client struct {
	base string
	addr string
	pin  string
	cert tls.Certificate
	http *http.Client
	log  *slog.Logger
}

// New parses the trove:// connection string and builds a fingerprint-pinned client.
func New(opts Options) (*Client, error) {
	addr, pin, err := parseTroveURL(opts.Server)
	if err != nil {
		return nil, err
	}
	log := opts.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Client{
		base: "https://" + addr,
		addr: addr,
		pin:  pin,
		cert: opts.Cert,
		http: &http.Client{
			Timeout:   httpTimeout,
			Transport: &http.Transport{TLSClientConfig: identity.PinnedClientConfig(opts.Cert, pin)},
		},
		log: log,
	}, nil
}

// Announce publishes this node's candidate addresses and returns the registration.
func (c *Client) Announce(ctx context.Context, addrs []disco.Address, ttl time.Duration) (disco.AnnounceResponse, error) {
	var resp disco.AnnounceResponse
	req := disco.AnnounceRequest{Addresses: addrs, RequestedTTLSecs: int(ttl.Seconds())}
	if err := c.post(ctx, "/v1/announce", req, &resp); err != nil {
		return disco.AnnounceResponse{}, err
	}
	return resp, nil
}

// ServerAddr is the discovery server's host:port, also the STUN probe target.
func (c *Client) ServerAddr() string { return c.addr }

// Lookup resolves a peer's current candidate addresses by node id.
func (c *Client) Lookup(ctx context.Context, nodeID string) (disco.LookupResponse, error) {
	var resp disco.LookupResponse
	if err := c.post(ctx, "/v1/lookup", disco.LookupRequest{TargetNodeID: nodeID}, &resp); err != nil {
		return disco.LookupResponse{}, err
	}
	return resp, nil
}

func (c *Client) post(ctx context.Context, path string, body, out any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("discovery: marshal %s: %w", path, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("discovery: request %s: %w", path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("discovery: %s: %w", path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("discovery: read %s: %w", path, err)
	}
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusNotFound {
			return ErrPeerNotFound
		}
		var e disco.Error
		if json.Unmarshal(data, &e) == nil && e.Code != "" {
			return fmt.Errorf("discovery: %s: %w", path, e)
		}
		return fmt.Errorf("discovery: %s: status %d", path, resp.StatusCode)
	}
	if out != nil {
		if err := json.Unmarshal(data, out); err != nil {
			return fmt.Errorf("discovery: decode %s: %w", path, err)
		}
	}
	return nil
}

func parseTroveURL(s string) (addr, pin string, err error) {
	u, err := url.Parse(s)
	if err != nil {
		return "", "", fmt.Errorf("discovery: parse server url: %w", err)
	}
	if u.Scheme != "trove" {
		return "", "", fmt.Errorf("discovery: server url scheme %q, want trove", u.Scheme)
	}
	if u.Host == "" || u.Port() == "" {
		return "", "", fmt.Errorf("discovery: server url missing host:port")
	}
	if u.Path != "" || u.User != nil {
		return "", "", fmt.Errorf("discovery: server url must not contain a path or userinfo")
	}
	pin = u.Query().Get("id")
	if !identity.ValidNodeID(pin) {
		return "", "", fmt.Errorf("discovery: server url missing or invalid id")
	}
	return u.Host, pin, nil
}
