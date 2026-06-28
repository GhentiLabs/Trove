// Package crypto provides convergent per-chunk authenticated encryption: each
// chunk is sealed under its own key, derived via HKDF from the folder master key
// and the chunk's BLAKE3 identity. Identical plaintext therefore yields identical
// ciphertext, so deduplication survives encryption.
//
// Invariant: the per-chunk key is unique because the identity is a hash of the
// plaintext, so each key seals exactly one message and the deterministic nonce is
// never reused. Do not switch to a per-folder key (breaks dedup) or reuse a key
// across plaintexts (reuses a nonce).
//
// Accepted tradeoff: deterministic ciphertext lets an observer with read access
// to the store confirm whether a known plaintext is present.
package crypto

import (
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"

	"github.com/GhentiLabs/Trove/client/internal/hasher"
	"golang.org/x/crypto/argon2"
	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/hkdf"
)

// MasterKeyLen is the length of a folder master key.
const MasterKeyLen = 32

// Argon2id parameters for passphrase-derived master keys.
const (
	ArgonTime      uint32 = 3
	ArgonMemoryKiB uint32 = 64 * 1024
	ArgonThreads   uint8  = 4
)

// Fixed, non-secret domain separators.
var (
	hkdfLabel = []byte("trove/chunk/v1")
	hkdfSalt  = []byte("trove/chunk-keys")

	verifyLabel = []byte("trove/folder/verify/v1")
	verifySalt  = []byte("trove/folder-verify")

	blindLabel = []byte("trove/holder/blind/v1")
	blindSalt  = []byte("trove/holder-blind")

	mutableSalt = []byte("trove/mutable")
)

// VerifierLen is the length of a folder key-mismatch verifier.
const VerifierLen = 32

// BlindLen is the length of a holder-blinded id.
const BlindLen = 32

// MutableOverhead is the byte overhead SealMutable adds: a prepended nonce plus the AEAD tag.
const MutableOverhead = chacha20poly1305.NonceSize + chacha20poly1305.Overhead

func deriveKeyNonce(master [MasterKeyLen]byte, id hasher.ChunkID) (key [chacha20poly1305.KeySize]byte, nonce [chacha20poly1305.NonceSize]byte) {
	info := make([]byte, 0, len(id)+len(hkdfLabel))
	info = append(info, id[:]...)
	info = append(info, hkdfLabel...)

	r := hkdf.New(sha256.New, master[:], hkdfSalt, info)
	if _, err := io.ReadFull(r, key[:]); err != nil {
		panic(fmt.Sprintf("crypto: hkdf key: %v", err))
	}
	if _, err := io.ReadFull(r, nonce[:]); err != nil {
		panic(fmt.Sprintf("crypto: hkdf nonce: %v", err))
	}
	return key, nonce
}

// Seal encrypts data under the folder master key, bound to the chunk identity.
func Seal(master [MasterKeyLen]byte, id hasher.ChunkID, data []byte) ([]byte, error) {
	key, nonce := deriveKeyNonce(master, id)
	a, err := chacha20poly1305.New(key[:])
	if err != nil {
		return nil, fmt.Errorf("crypto: aead init: %w", err)
	}
	return a.Seal(nil, nonce[:], data, id[:]), nil
}

// Open reverses Seal, returning an error if the tag check fails.
func Open(master [MasterKeyLen]byte, id hasher.ChunkID, ciphertext []byte) ([]byte, error) {
	key, nonce := deriveKeyNonce(master, id)
	a, err := chacha20poly1305.New(key[:])
	if err != nil {
		return nil, fmt.Errorf("crypto: aead init: %w", err)
	}
	out, err := a.Open(nil, nonce[:], ciphertext, id[:])
	if err != nil {
		return nil, fmt.Errorf("crypto: open: %w", err)
	}
	return out, nil
}

// FolderVerifier derives a non-secret key-mismatch token from the folder master key.
func FolderVerifier(master [MasterKeyLen]byte, folderID string) []byte {
	info := make([]byte, 0, len(verifyLabel)+len(folderID))
	info = append(info, verifyLabel...)
	info = append(info, folderID...)
	r := hkdf.New(sha256.New, master[:], verifySalt, info)
	out := make([]byte, VerifierLen)
	if _, err := io.ReadFull(r, out); err != nil {
		panic(fmt.Sprintf("crypto: hkdf verifier: %v", err))
	}
	return out
}

// BlindID derives the opaque id under which a holder stores a blob, from the folder
// master key and the blob's true id.
func BlindID(master [MasterKeyLen]byte, id []byte) [BlindLen]byte {
	info := make([]byte, 0, len(blindLabel)+len(id))
	info = append(info, blindLabel...)
	info = append(info, id...)
	r := hkdf.New(sha256.New, master[:], blindSalt, info)
	var out [BlindLen]byte
	if _, err := io.ReadFull(r, out[:]); err != nil {
		panic(fmt.Sprintf("crypto: hkdf blind: %v", err))
	}
	return out
}

// SealMutable encrypts plaintext under a key derived from the master key and label, with a
// fresh random nonce prepended to the output. Unlike Seal it is non-convergent: use it for
// a mutable blob kept under a fixed id (the holder catalog), where Seal's deterministic
// nonce would repeat across versions and reuse a keystream.
func SealMutable(master [MasterKeyLen]byte, label string, plaintext []byte) ([]byte, error) {
	key := mutableKey(master, label)
	a, err := chacha20poly1305.New(key[:])
	if err != nil {
		return nil, fmt.Errorf("crypto: aead init: %w", err)
	}
	nonce := make([]byte, chacha20poly1305.NonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("crypto: nonce: %w", err)
	}
	return a.Seal(nonce, nonce, plaintext, []byte(label)), nil
}

// OpenMutable reverses SealMutable.
func OpenMutable(master [MasterKeyLen]byte, label string, blob []byte) ([]byte, error) {
	if len(blob) < chacha20poly1305.NonceSize {
		return nil, fmt.Errorf("crypto: mutable blob too short")
	}
	key := mutableKey(master, label)
	a, err := chacha20poly1305.New(key[:])
	if err != nil {
		return nil, fmt.Errorf("crypto: aead init: %w", err)
	}
	nonce, ct := blob[:chacha20poly1305.NonceSize], blob[chacha20poly1305.NonceSize:]
	out, err := a.Open(nil, nonce, ct, []byte(label))
	if err != nil {
		return nil, fmt.Errorf("crypto: open: %w", err)
	}
	return out, nil
}

func mutableKey(master [MasterKeyLen]byte, label string) [chacha20poly1305.KeySize]byte {
	r := hkdf.New(sha256.New, master[:], mutableSalt, []byte(label))
	var key [chacha20poly1305.KeySize]byte
	if _, err := io.ReadFull(r, key[:]); err != nil {
		panic(fmt.Sprintf("crypto: hkdf mutable key: %v", err))
	}
	return key
}

// DeriveMasterKey derives a folder master key from a passphrase and salt via Argon2id.
func DeriveMasterKey(passphrase string, salt []byte) [MasterKeyLen]byte {
	k := argon2.IDKey([]byte(passphrase), salt, ArgonTime, ArgonMemoryKiB, ArgonThreads, MasterKeyLen)
	var out [MasterKeyLen]byte
	copy(out[:], k)
	return out
}
