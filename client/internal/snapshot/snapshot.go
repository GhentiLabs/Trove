// Package snapshot reduces a folder's manifest set to a single content-addressed
// root and diffs two such sets. The root is a binary Merkle tree over the
// path-sorted leaves with domain-separated leaf and node hashes; it depends only
// on content (manifest identity and tombstone state), never on causal metadata,
// scan order, or wall-clock, so the same folder state yields the same root on any
// machine. The hashing and its domain tags are a frozen forward contract.
package snapshot

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"github.com/GhentiLabs/Trove/client/internal/hasher"
	"github.com/GhentiLabs/Trove/client/internal/manifest"
)

const (
	leafDomain  = "trove/snapshot/leaf/v1\x00"
	nodeDomain  = "trove/snapshot/node/v1\x00"
	emptyDomain = "trove/snapshot/empty/v1\x00"
)

// RootLen is the length in bytes of a snapshot Root.
const RootLen = 32

// Root identifies a folder state: the Merkle root over its manifest set.
type Root [RootLen]byte

// String returns the lowercase hex encoding of the Root.
func (r Root) String() string { return hex.EncodeToString(r[:]) }

// Bytes returns a copy of the raw 32-byte root.
func (r Root) Bytes() []byte {
	b := make([]byte, RootLen)
	copy(b, r[:])
	return b
}

// RootFromBytes builds a Root from a raw 32-byte slice, e.g. a BLOB read from the
// model database.
func RootFromBytes(b []byte) (Root, error) {
	var r Root
	if len(b) != RootLen {
		return r, fmt.Errorf("snapshot: invalid root length %d, want %d", len(b), RootLen)
	}
	copy(r[:], b)
	return r, nil
}

// ParseRoot decodes a lowercase hex string produced by String back into a Root.
func ParseRoot(s string) (Root, error) {
	var r Root
	if len(s) != RootLen*2 {
		return r, fmt.Errorf("snapshot: invalid root length %d, want %d", len(s), RootLen*2)
	}
	if _, err := hex.Decode(r[:], []byte(s)); err != nil {
		return r, fmt.Errorf("snapshot: invalid root: %w", err)
	}
	return r, nil
}

// Leaf is one path's contribution to a snapshot: its manifest identity and
// whether it is a tombstone. Path must already be NFC-normalized; it is both the
// sort key and a committed field of the leaf hash.
type Leaf struct {
	Path       string
	ManifestID manifest.ID
	Deleted    bool
}

// Set is a folder's manifest set, at most one leaf per path.
type Set []Leaf

// Root returns the Merkle root over the set. Input order does not matter; leaves
// are sorted by path. Paths must already be NFC-normalized.
func (s Set) Root() Root {
	if len(s) == 0 {
		return Root(hasher.Sum([]byte(emptyDomain)))
	}
	sorted := sortedByPath(s)

	level := make([][RootLen]byte, len(sorted))
	for i, l := range sorted {
		level[i] = leafHash(l)
	}
	for len(level) > 1 {
		next := level[:0:0]
		for i := 0; i < len(level); i += 2 {
			if i+1 == len(level) {
				next = append(next, level[i])
				continue
			}
			next = append(next, nodeHash(level[i], level[i+1]))
		}
		level = next
	}
	return Root(level[0])
}

func leafHash(l Leaf) [RootLen]byte {
	h := hasher.New()
	_, _ = h.Write([]byte(leafDomain))
	var lp [binary.MaxVarintLen64]byte
	_, _ = h.Write(lp[:binary.PutUvarint(lp[:], uint64(len(l.Path)))])
	_, _ = h.Write([]byte(l.Path))
	_, _ = h.Write(l.ManifestID[:])
	_, _ = h.Write([]byte{boolByte(l.Deleted)})
	return [RootLen]byte(h.Sum())
}

func nodeHash(left, right [RootLen]byte) [RootLen]byte {
	h := hasher.New()
	_, _ = h.Write([]byte(nodeDomain))
	_, _ = h.Write(left[:])
	_, _ = h.Write(right[:])
	return [RootLen]byte(h.Sum())
}

func boolByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}
