// Package manifest defines the content identity of a single path in a synced
// folder. A manifest hashes to a stable ID over a frozen canonical encoding, so
// the same file state produces the same ID on any machine. The encoding and the
// constants it depends on are a forward contract and must never change without a
// format-version bump.
package manifest

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"golang.org/x/text/unicode/norm"

	"github.com/GhentiLabs/Trove/client/internal/hasher"
)

// IDLen is the length in bytes of a manifest ID.
const IDLen = 32

// ID is the BLAKE3-256 hash of a manifest's canonical identity bytes.
type ID [IDLen]byte

// String returns the lowercase hex encoding of the ID.
func (id ID) String() string { return hex.EncodeToString(id[:]) }

// Bytes returns a copy of the raw 32-byte identity.
func (id ID) Bytes() []byte {
	b := make([]byte, IDLen)
	copy(b, id[:])
	return b
}

// ParseID decodes a lowercase hex string produced by String back into an ID.
func ParseID(s string) (ID, error) {
	var id ID
	if len(s) != IDLen*2 {
		return id, fmt.Errorf("manifest: invalid id length %d, want %d", len(s), IDLen*2)
	}
	if _, err := hex.Decode(id[:], []byte(s)); err != nil {
		return id, fmt.Errorf("manifest: invalid id: %w", err)
	}
	return id, nil
}

// IDFromBytes builds an ID from a raw 32-byte slice, e.g. a BLOB read from the
// model database.
func IDFromBytes(b []byte) (ID, error) {
	var id ID
	if len(b) != IDLen {
		return id, fmt.Errorf("manifest: invalid id length %d, want %d", len(b), IDLen)
	}
	copy(id[:], b)
	return id, nil
}

// FormatVersion is the canonical identity encoding version, written as the first
// byte after the domain tag. It is frozen; bump only on a breaking change.
const FormatVersion = 1

const manifestDomain = "trove/manifest/v1\x00"

// Kind is the type of a path. Its values are frozen.
type Kind uint8

const (
	KindRegular Kind = 0
	KindDir     Kind = 1
	KindSymlink Kind = 2
)

// ChunkRef is one ordered chunk reference: a chunk identity and its plaintext
// length. Offsets are derivable from the running length sum and are not stored.
type ChunkRef struct {
	ID     hasher.ChunkID
	Length int64
}

// Manifest describes one path. Its identity is derived from Kind, Path, the
// executable bit of Mode (regular files only), SymlinkTarget, and Chunks; all
// other fields, and any other Mode bits, are metadata that never enter the ID.
type Manifest struct {
	Kind          Kind
	Path          string
	Mode          uint32
	SymlinkTarget string
	Chunks        []ChunkRef
}

// ID returns the manifest's content identity.
func (m Manifest) ID() ID {
	return ID(hasher.Sum(m.IdentityBytes()))
}

// IdentityBytes returns the canonical bytes hashed to produce the ID.
func (m Manifest) IdentityBytes() []byte {
	b := make([]byte, 0, len(manifestDomain)+len(m.Path)+len(m.SymlinkTarget)+len(m.Chunks)*40+16) // 40 = 32-byte chunk id + up to 8-byte uvarint length
	b = append(b, manifestDomain...)
	b = append(b, FormatVersion, byte(m.Kind))
	b = appendBytes(b, []byte(NormalizePath(m.Path)))
	b = append(b, canonicalMode(m.Kind, m.Mode))
	b = appendBytes(b, []byte(NormalizePath(m.SymlinkTarget)))
	b = binary.AppendUvarint(b, uint64(len(m.Chunks)))
	for _, c := range m.Chunks {
		b = append(b, c.ID[:]...)
		b = binary.AppendUvarint(b, uint64(c.Length))
	}
	return b
}

func appendBytes(b, p []byte) []byte {
	b = binary.AppendUvarint(b, uint64(len(p)))
	return append(b, p...)
}

// NormalizePath returns the Unicode NFC form of p, the form used for every path
// and symlink target before hashing or indexing.
func NormalizePath(p string) string {
	return norm.NFC.String(p)
}

func canonicalMode(kind Kind, mode uint32) byte {
	if kind == KindRegular && mode&0o111 != 0 {
		return 1
	}
	return 0
}
