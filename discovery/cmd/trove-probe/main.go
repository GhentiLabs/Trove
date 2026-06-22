// Command trove-probe verifies a discovery server's public mTLS endpoint. It
// mints an ephemeral client identity, pins the server's fingerprint, announces a
// candidate address, then looks itself back up — proving the full
// TLS 1.3 + mTLS + pinning path end to end.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/GhentiLabs/Trove/pkg/discovery"
	"github.com/GhentiLabs/Trove/pkg/identity"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "trove-probe:", err)
		os.Exit(1)
	}
}

func run() error {
	addr := flag.String("addr", "", "discovery server address as host:port")
	pin := flag.String("pin", "", "expected server fingerprint to pin")
	candidate := flag.String("candidate", "203.0.113.50:4000", "candidate address to announce")
	flag.Parse()

	if *addr == "" || *pin == "" {
		return errors.New("both -addr and -pin are required")
	}
	cand, err := parseCandidate(*candidate)
	if err != nil {
		return err
	}

	_, key, err := identity.GenerateKey()
	if err != nil {
		return err
	}
	cert, err := identity.NewCertificate(key)
	if err != nil {
		return err
	}
	nodeID := identity.FingerprintCert(cert.Leaf)
	fmt.Println("client node_id:", nodeID)

	client := &http.Client{
		Timeout:   10 * time.Second,
		Transport: &http.Transport{TLSClientConfig: identity.PinnedClientConfig(cert, *pin)},
	}
	base := "https://" + *addr

	var ann discovery.AnnounceResponse
	if err := postJSON(client, base+"/v1/announce", discovery.AnnounceRequest{
		Addresses:        []discovery.Address{cand},
		RequestedTTLSecs: 300,
	}, &ann); err != nil {
		return fmt.Errorf("announce: %w", err)
	}
	if ann.NodeID != nodeID {
		return fmt.Errorf("server derived node_id %q, expected %q", ann.NodeID, nodeID)
	}
	fmt.Printf("announce ok: observed=%s ttl=%ds\n", ann.ObservedAddr, ann.GrantedTTLSecs)

	var lk discovery.LookupResponse
	if err := postJSON(client, base+"/v1/lookup", discovery.LookupRequest{TargetNodeID: nodeID}, &lk); err != nil {
		return fmt.Errorf("lookup: %w", err)
	}
	if len(lk.Addresses) != 1 || lk.Addresses[0] != cand {
		return fmt.Errorf("lookup returned %+v, expected the announced candidate", lk.Addresses)
	}
	fmt.Printf("lookup ok: %d address(es) last_seen=%d\n", len(lk.Addresses), lk.LastSeenMillis)

	fmt.Printf("OK: mTLS + fingerprint pinning verified against %s\n", *addr)
	return nil
}

func parseCandidate(s string) (discovery.Address, error) {
	host, portStr, err := net.SplitHostPort(s)
	if err != nil {
		return discovery.Address{}, fmt.Errorf("candidate %q must be host:port: %w", s, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return discovery.Address{}, fmt.Errorf("candidate port %q: %w", portStr, err)
	}
	addr := discovery.Address{IP: host, Port: port, Type: discovery.AddressPublic}
	return addr, addr.Validate()
}

func postJSON(client *http.Client, url string, body, out any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	resp, err := client.Post(url, "application/json", bytes.NewReader(raw))
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d: %s", resp.StatusCode, bytes.TrimSpace(data))
	}
	if out != nil {
		return json.Unmarshal(data, out)
	}
	return nil
}
