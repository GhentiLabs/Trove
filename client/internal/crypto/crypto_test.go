package crypto

import (
	"bytes"
	"testing"

	"github.com/GhentiLabs/Trove/client/internal/hasher"
)

func TestFolderVerifier(t *testing.T) {
	var a, b [MasterKeyLen]byte
	for i := range a {
		a[i] = byte(i)
		b[i] = byte(255 - i)
	}
	v := FolderVerifier(a, "folder-1")
	if len(v) != VerifierLen {
		t.Fatalf("verifier len = %d, want %d", len(v), VerifierLen)
	}
	if !bytes.Equal(v, FolderVerifier(a, "folder-1")) {
		t.Fatal("verifier not deterministic")
	}
	if bytes.Equal(v, FolderVerifier(b, "folder-1")) {
		t.Fatal("different keys produced the same verifier")
	}
	if bytes.Equal(v, FolderVerifier(a, "folder-2")) {
		t.Fatal("different folder ids produced the same verifier")
	}
}

func TestBlindID(t *testing.T) {
	var a, b [MasterKeyLen]byte
	for i := range a {
		a[i] = byte(i)
		b[i] = byte(255 - i)
	}
	id1 := []byte("chunk-one")
	blind := BlindID(a, id1)
	if len(blind) != BlindLen {
		t.Fatalf("blind len = %d, want %d", len(blind), BlindLen)
	}
	if blind != BlindID(a, id1) {
		t.Fatal("blind id not deterministic")
	}
	if blind == BlindID(b, id1) {
		t.Fatal("different keys produced the same blind id")
	}
	if blind == BlindID(a, []byte("chunk-two")) {
		t.Fatal("different ids produced the same blind id")
	}
}

func TestSealOpenRoundTrip(t *testing.T) {
	var master [MasterKeyLen]byte
	for i := range master {
		master[i] = byte(i)
	}
	plain := []byte("the quick brown fox jumps over the lazy dog")
	id := hasher.Sum(plain)

	ct, err := Seal(master, id, plain)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if bytes.Equal(ct, plain) {
		t.Fatal("ciphertext equals plaintext")
	}

	got, err := Open(master, id, ct)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("round trip mismatch")
	}
}

// Convergence: the same (master key, plaintext) must yield byte-identical
// ciphertext, or dedup would not survive encryption.
func TestConvergentDeterministic(t *testing.T) {
	var master [MasterKeyLen]byte
	plain := []byte("convergent encryption keeps dedup working")
	id := hasher.Sum(plain)

	a, err := Seal(master, id, plain)
	if err != nil {
		t.Fatalf("Seal a: %v", err)
	}
	b, err := Seal(master, id, plain)
	if err != nil {
		t.Fatalf("Seal b: %v", err)
	}
	if !bytes.Equal(a, b) {
		t.Fatal("ciphertext not deterministic for same key+plaintext")
	}
}

// Different plaintext (hence different identity) derives a different key, so
// ciphertexts must differ and the nonce is never reused under a key.
func TestDistinctIdentitiesDiverge(t *testing.T) {
	var master [MasterKeyLen]byte
	p1, p2 := []byte("plaintext one"), []byte("plaintext two")
	c1, _ := Seal(master, hasher.Sum(p1), p1)
	c2, _ := Seal(master, hasher.Sum(p2), p2)
	if bytes.Equal(c1, c2) {
		t.Fatal("distinct plaintexts produced identical ciphertext")
	}
}

func TestTamperFailsOpen(t *testing.T) {
	var master [MasterKeyLen]byte
	plain := []byte("authenticate me")
	id := hasher.Sum(plain)
	ct, _ := Seal(master, id, plain)

	tampered := bytes.Clone(ct)
	tampered[0] ^= 0xFF
	if _, err := Open(master, id, tampered); err == nil {
		t.Fatal("expected Open to fail on tampered ciphertext")
	}

	var wrong [MasterKeyLen]byte
	wrong[0] = 1
	if _, err := Open(wrong, id, ct); err == nil {
		t.Fatal("expected Open to fail with wrong master key")
	}
}

func TestDeriveMasterKeyReproducible(t *testing.T) {
	salt := []byte("0123456789abcdef")
	a := DeriveMasterKey("correct horse battery staple", salt)
	b := DeriveMasterKey("correct horse battery staple", salt)
	if a != b {
		t.Fatal("Argon2id derivation not reproducible")
	}
	c := DeriveMasterKey("different passphrase", salt)
	if a == c {
		t.Fatal("different passphrases produced same key")
	}
}
