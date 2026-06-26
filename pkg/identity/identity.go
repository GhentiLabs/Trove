// Package identity defines a Trove node's cryptographic identity and the TLS
// primitives that authenticate it.
//
// A node owns one Ed25519 keypair. Its node ID is the fingerprint of the public
// key's SubjectPublicKeyInfo, so the same value identifies a node whether it is
// derived from a raw key or from a presented TLS certificate. Authentication is
// mutual TLS: peers verify each other by pinning this fingerprint, not via a
// certificate authority. The cert is disposable; the key is the stable identity.
package identity

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base32"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"strings"
	"time"
)

// nodeIDEncoding is lowercase, unpadded base32. sha256 produces 32 bytes, which
// encodes to a fixed 52-character, URL- and log-friendly identifier.
var nodeIDEncoding = base32.StdEncoding.WithPadding(base32.NoPadding)

// NodeIDLen is the exact length of every node ID / fingerprint string.
const NodeIDLen = 52

// GenerateKey returns a fresh Ed25519 keypair.
func GenerateKey() (ed25519.PublicKey, ed25519.PrivateKey, error) {
	return ed25519.GenerateKey(rand.Reader)
}

// FingerprintCert derives the node identifier from a certificate's public key.
func FingerprintCert(cert *x509.Certificate) string {
	return fingerprintSPKI(cert.RawSubjectPublicKeyInfo)
}

// FingerprintKey derives a node identifier directly from an Ed25519 public key,
// yielding the same value as FingerprintCert for that key's certificate. It lets a
// signed payload (e.g. a membership entry) bind a public key to its node id.
func FingerprintKey(pub ed25519.PublicKey) (string, error) {
	spki, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return "", fmt.Errorf("identity: marshal public key: %w", err)
	}
	return fingerprintSPKI(spki), nil
}

// PeerFingerprint derives the node identifier of the connection's peer.
func PeerFingerprint(state *tls.ConnectionState) (string, error) {
	if state == nil || len(state.PeerCertificates) == 0 {
		return "", errors.New("identity: no peer certificate")
	}
	return FingerprintCert(state.PeerCertificates[0]), nil
}

func fingerprintSPKI(spki []byte) string {
	sum := sha256.Sum256(spki)
	return strings.ToLower(nodeIDEncoding.EncodeToString(sum[:]))
}

// ValidNodeID reports whether s has the shape of a fingerprint. It is a cheap
// structural check, not proof that any node holds the matching key.
func ValidNodeID(s string) bool {
	if len(s) != NodeIDLen || s != strings.ToLower(s) {
		return false
	}
	_, err := nodeIDEncoding.DecodeString(strings.ToUpper(s))
	return err == nil
}

// LoadOrCreateKey loads the persisted Ed25519 key at path, creating and writing
// one (mode 0600) if it does not yet exist.
func LoadOrCreateKey(path string) (ed25519.PrivateKey, error) {
	data, err := os.ReadFile(path)
	switch {
	case err == nil:
		block, _ := pem.Decode(data)
		if block == nil {
			return nil, fmt.Errorf("identity: no PEM block in %s", path)
		}
		parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return nil, err
		}
		key, ok := parsed.(ed25519.PrivateKey)
		if !ok {
			return nil, fmt.Errorf("identity: %s is not an Ed25519 key", path)
		}
		return key, nil
	case errors.Is(err, os.ErrNotExist):
		_, key, err := GenerateKey()
		if err != nil {
			return nil, err
		}
		der, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			return nil, err
		}
		encoded := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
		// Write-then-rename so a crash mid-write can't leave a corrupt key that
		// would fail to parse (and brick startup) on the next boot.
		tmp := path + ".tmp"
		if err := os.WriteFile(tmp, encoded, 0o600); err != nil {
			return nil, err
		}
		if err := os.Rename(tmp, path); err != nil {
			return nil, err
		}
		return key, nil
	default:
		return nil, err
	}
}

// LoadOrCreateCert returns a self-signed certificate for key, loading the one at
// path if it exists and matches key, otherwise generating and persisting a fresh
// one. Regenerating with the same key preserves the fingerprint.
func LoadOrCreateCert(path string, key ed25519.PrivateKey) (tls.Certificate, error) {
	data, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return tls.Certificate{}, err
	}
	if err == nil {
		if cert, ok := matchingCert(data, key); ok {
			return cert, nil
		}
	}
	cert, err := NewCertificate(key)
	if err != nil {
		return tls.Certificate{}, err
	}
	encoded := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Leaf.Raw})
	if err := os.WriteFile(path, encoded, 0o644); err != nil {
		return tls.Certificate{}, err
	}
	return cert, nil
}

// NewCertificate mints an in-memory self-signed certificate for key. Clients and
// tests use it to present their identity over mTLS.
func NewCertificate(key ed25519.PrivateKey) (tls.Certificate, error) {
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return tls.Certificate{}, err
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "trove-node"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.AddDate(100, 0, 0),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, key.Public(), key)
	if err != nil {
		return tls.Certificate{}, err
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, nil
}

func matchingCert(pemData []byte, key ed25519.PrivateKey) (tls.Certificate, bool) {
	block, _ := pem.Decode(pemData)
	if block == nil {
		return tls.Certificate{}, false
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return tls.Certificate{}, false
	}
	pub, ok := cert.PublicKey.(ed25519.PublicKey)
	if !ok || !pub.Equal(key.Public()) {
		return tls.Certificate{}, false
	}
	return tls.Certificate{Certificate: [][]byte{block.Bytes}, PrivateKey: key, Leaf: cert}, true
}

// ServerTLSConfig builds the discovery server's TLS 1.3 config. It demands a
// client certificate but performs no CA verification; the caller derives the
// client's node ID from the presented cert.
func ServerTLSConfig(cert tls.Certificate) *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
		ClientAuth:   tls.RequireAnyClientCert,
	}
}

// PinnedClientConfig builds a TLS 1.3 client config that presents clientCert and
// trusts the server by pinning its SPKI fingerprint instead of a CA or hostname.
func PinnedClientConfig(clientCert tls.Certificate, serverPin string) *tls.Config {
	return &tls.Config{
		Certificates:       []tls.Certificate{clientCert},
		MinVersion:         tls.VersionTLS13,
		InsecureSkipVerify: true, // CA/hostname checks replaced by the pin below
		VerifyConnection: func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return errors.New("identity: server presented no certificate")
			}
			if got := FingerprintCert(cs.PeerCertificates[0]); got != serverPin {
				return fmt.Errorf("identity: server fingerprint %q does not match pin %q", got, serverPin)
			}
			return nil
		},
	}
}

// DialPinned opens an mTLS connection that authenticates the server by pin.
func DialPinned(ctx context.Context, network, addr string, clientCert tls.Certificate, serverPin string) (net.Conn, error) {
	d := &tls.Dialer{Config: PinnedClientConfig(clientCert, serverPin)}
	return d.DialContext(ctx, network, addr)
}
