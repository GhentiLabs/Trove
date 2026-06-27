// Package membership is the client's network roster: a self-authenticating set of
// signed member entries that gossips between peers, rooted at the founding admin. An
// entry binds a node id to its Ed25519 public key and is signed by an existing admin
// (the founder self-signs the genesis), so any node can verify the roster without a
// central authority. Signatures are over canonical hand-rolled bytes, never protobuf.
package membership

import (
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/GhentiLabs/Trove/pkg/identity"
)

const signingDomain = "trove/membership/v1\x00"

var (
	// ErrInvalidEntry is returned when an entry is malformed.
	ErrInvalidEntry = errors.New("membership: invalid entry")
	// ErrKeyMismatch is returned when an entry's public key does not match its node id.
	ErrKeyMismatch = errors.New("membership: public key does not match node id")
	// ErrBadSignature is returned when an entry's signature does not verify.
	ErrBadSignature = errors.New("membership: signature verification failed")
)

// Role is a member's authority in the network. Values are frozen.
type Role uint8

const (
	// RoleReader may read and restore the folder; it cannot add members.
	RoleReader Role = 0
	// RoleWriter may read, write, and add members. The owner is the founding writer.
	RoleWriter Role = 1
	// RoleHolder stores a folder's ciphertext without its key: it serves opaque blinded
	// blobs back to trusted members but cannot read, write, or add members.
	RoleHolder Role = 2
)

// Entry is one signed roster record: a member's identity and the admin attestation
// that admitted it. Sig is by AddedBy over the canonical signing bytes.
type Entry struct {
	NetworkID string
	NodeID    string
	PublicKey []byte
	Role      Role
	AddedBy   string
	AddedAtMs int64
	Sig       []byte
}

// signingBytes is the canonical, domain-separated, length-prefixed encoding signed
// for an entry. The field set and order are frozen; this is never protobuf.
func (e Entry) signingBytes() []byte {
	b := make([]byte, 0, len(signingDomain)+len(e.NetworkID)+len(e.NodeID)+len(e.PublicKey)+len(e.AddedBy)+32)
	b = append(b, signingDomain...)
	b = appendField(b, []byte(e.NetworkID))
	b = appendField(b, []byte(e.NodeID))
	b = appendField(b, e.PublicKey)
	b = append(b, byte(e.Role))
	b = appendField(b, []byte(e.AddedBy))
	b = binary.BigEndian.AppendUint64(b, uint64(e.AddedAtMs))
	return b
}

func appendField(b, p []byte) []byte {
	b = binary.AppendUvarint(b, uint64(len(p)))
	return append(b, p...)
}

// Sign returns e with its signature set, signed by key (which must be AddedBy's
// private key).
func Sign(key ed25519.PrivateKey, e Entry) (Entry, error) {
	if err := e.validate(); err != nil {
		return Entry{}, err
	}
	e.Sig = ed25519.Sign(key, e.signingBytes())
	return e, nil
}

// VerifySig checks that the entry is well-formed, its public key matches its node id,
// and its signature verifies against signerPub (AddedBy's public key). It does not
// check the trust chain — that is the roster's job.
func (e Entry) VerifySig(signerPub ed25519.PublicKey) error {
	if err := e.validate(); err != nil {
		return err
	}
	id, err := identity.FingerprintKey(e.PublicKey)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidEntry, err)
	}
	if id != e.NodeID {
		return ErrKeyMismatch
	}
	if len(signerPub) != ed25519.PublicKeySize || !ed25519.Verify(signerPub, e.signingBytes(), e.Sig) {
		return ErrBadSignature
	}
	return nil
}

func (e Entry) validate() error {
	switch {
	case e.NetworkID == "":
		return fmt.Errorf("%w: empty network id", ErrInvalidEntry)
	case e.NodeID == "":
		return fmt.Errorf("%w: empty node id", ErrInvalidEntry)
	case len(e.PublicKey) != ed25519.PublicKeySize:
		return fmt.Errorf("%w: public key length %d", ErrInvalidEntry, len(e.PublicKey))
	case e.AddedBy == "":
		return fmt.Errorf("%w: empty added_by", ErrInvalidEntry)
	case e.Role != RoleReader && e.Role != RoleWriter && e.Role != RoleHolder:
		return fmt.Errorf("%w: unknown role %d", ErrInvalidEntry, e.Role)
	}
	return nil
}
