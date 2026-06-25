package membership

import (
	"crypto/ed25519"
	"encoding/hex"
	"testing"

	"github.com/GhentiLabs/Trove/pkg/identity"
)

func selfEntry(t *testing.T) (Entry, ed25519.PublicKey) {
	t.Helper()
	pub, key, err := identity.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	id, err := identity.FingerprintKey(pub)
	if err != nil {
		t.Fatalf("FingerprintKey: %v", err)
	}
	e := Entry{NetworkID: "netid", NodeID: id, PublicKey: pub, Role: RoleWriter, AddedBy: id, AddedAtMs: 1700000000000}
	signed, err := Sign(key, e)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	return signed, pub
}

func TestSignVerifyRoundTrip(t *testing.T) {
	e, pub := selfEntry(t)
	if err := e.VerifySig(pub); err != nil {
		t.Fatalf("VerifySig: %v", err)
	}
}

func TestVerifyRejectsTamperedField(t *testing.T) {
	e, pub := selfEntry(t)
	e.Role = RoleReader // changed after signing
	if err := e.VerifySig(pub); err == nil {
		t.Fatal("VerifySig accepted a tampered role")
	}
	e2, pub2 := selfEntry(t)
	e2.AddedAtMs++
	if err := e2.VerifySig(pub2); err == nil {
		t.Fatal("VerifySig accepted a tampered timestamp")
	}
}

func TestVerifyRejectsKeyNodeMismatch(t *testing.T) {
	e, _ := selfEntry(t)
	other, _, _ := identity.GenerateKey()
	e.PublicKey = other // fingerprint(PublicKey) no longer equals NodeID
	if err := e.VerifySig(other); err == nil {
		t.Fatal("VerifySig accepted a public key that does not match the node id")
	}
}

func TestVerifyRejectsWrongSigner(t *testing.T) {
	e, _ := selfEntry(t)
	wrong, _, _ := identity.GenerateKey()
	if err := e.VerifySig(wrong); err == nil {
		t.Fatal("VerifySig accepted a signature from the wrong signer")
	}
}

// Golden: the canonical signing-byte layout is a frozen cross-node contract.
func TestSigningBytesGolden(t *testing.T) {
	e := Entry{NetworkID: "net", NodeID: "node", PublicKey: make([]byte, 32), Role: RoleWriter, AddedBy: "adm", AddedAtMs: 1}
	for i := range e.PublicKey {
		e.PublicKey[i] = 0x01
	}
	const want = "74726f76652f6d656d626572736869702f763100036e6574046e6f6465200101010101010101010101010101010101010101010101010101010101010101010361646d0000000000000001"
	if got := hex.EncodeToString(e.signingBytes()); got != want {
		t.Fatalf("signingBytes drifted:\n got %s\nwant %s", got, want)
	}
}
