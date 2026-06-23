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

// The chunk identity is bound through the HKDF info; the salt is a fixed,
// non-secret domain separator.
var (
	hkdfLabel = []byte("trove/chunk/v1")
	hkdfSalt  = []byte("trove/chunk-keys")
)

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

// DeriveMasterKey derives a folder master key from a passphrase and salt via Argon2id.
func DeriveMasterKey(passphrase string, salt []byte) [MasterKeyLen]byte {
	k := argon2.IDKey([]byte(passphrase), salt, ArgonTime, ArgonMemoryKiB, ArgonThreads, MasterKeyLen)
	var out [MasterKeyLen]byte
	copy(out[:], k)
	return out
}
