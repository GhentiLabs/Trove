// Package hasher computes the content identity of a chunk: the BLAKE3-256 hash of
// its plaintext bytes, independent of how the chunk is stored.
package hasher

import (
	"encoding/hex"
	"fmt"

	"github.com/zeebo/blake3"
)

// IDLen is the length in bytes of a ChunkID.
const IDLen = 32

// ChunkID is the BLAKE3-256 hash of a chunk's plaintext.
type ChunkID [IDLen]byte

// Sum returns the ChunkID of plaintext.
func Sum(plaintext []byte) ChunkID {
	return blake3.Sum256(plaintext)
}

// String returns the lowercase hex encoding of the ID.
func (id ChunkID) String() string {
	return hex.EncodeToString(id[:])
}

// Bytes returns a copy of the raw 32-byte identity.
func (id ChunkID) Bytes() []byte {
	b := make([]byte, IDLen)
	copy(b, id[:])
	return b
}

// Parse decodes a lowercase hex string produced by String back into a ChunkID.
func Parse(s string) (ChunkID, error) {
	var id ChunkID
	if len(s) != IDLen*2 {
		return id, fmt.Errorf("hasher: invalid chunk id length %d, want %d", len(s), IDLen*2)
	}
	if _, err := hex.Decode(id[:], []byte(s)); err != nil {
		return id, fmt.Errorf("hasher: invalid chunk id: %w", err)
	}
	return id, nil
}

// FromBytes builds a ChunkID from a raw 32-byte slice, e.g. a BLOB read from the
// index.
func FromBytes(b []byte) (ChunkID, error) {
	var id ChunkID
	if len(b) != IDLen {
		return id, fmt.Errorf("hasher: invalid chunk id length %d, want %d", len(b), IDLen)
	}
	copy(id[:], b)
	return id, nil
}

// SetMinus returns the ids in a that are not in b.
func SetMinus(a, b []ChunkID) []ChunkID {
	if len(a) == 0 {
		return nil
	}
	exclude := make(map[ChunkID]struct{}, len(b))
	for _, id := range b {
		exclude[id] = struct{}{}
	}
	out := make([]ChunkID, 0, len(a))
	for _, id := range a {
		if _, ok := exclude[id]; !ok {
			out = append(out, id)
		}
	}
	return out
}

// Hasher computes a ChunkID incrementally.
type Hasher struct {
	h *blake3.Hasher
}

// New returns a streaming Hasher.
func New() *Hasher { return &Hasher{h: blake3.New()} }

// Write adds bytes to the running hash.
func (h *Hasher) Write(p []byte) (int, error) { return h.h.Write(p) }

// Sum returns the ChunkID of everything written so far.
func (h *Hasher) Sum() ChunkID {
	var id ChunkID
	copy(id[:], h.h.Sum(nil))
	return id
}

// Reset returns the Hasher to its initial state for reuse.
func (h *Hasher) Reset() { h.h.Reset() }
