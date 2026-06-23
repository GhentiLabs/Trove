package manifest

import (
	"encoding/binary"
	"errors"
	"maps"
	"slices"
)

// ErrMalformedVector is returned when bytes do not decode to a version vector.
var ErrMalformedVector = errors.New("manifest: malformed version vector")

// VersionVector maps a node id to its edit counter for a path. It carries
// causality: a local edit bumps this node's counter. It is metadata and never
// enters a manifest's content identity.
type VersionVector map[string]uint64

// Bump increments node's counter, starting from zero if absent.
func (vv VersionVector) Bump(node string) {
	vv[node]++
}

// Clone returns an independent copy.
func (vv VersionVector) Clone() VersionVector {
	return maps.Clone(vv)
}

// Canonical returns the deterministic encoding of the vector: a uvarint count of
// nonzero entries, then each entry in ascending node-id order as a
// length-prefixed node id and a uvarint counter. Zero counters are omitted.
func (vv VersionVector) Canonical() []byte {
	nodes := make([]string, 0, len(vv))
	for node, c := range vv {
		if c != 0 {
			nodes = append(nodes, node)
		}
	}
	slices.Sort(nodes)

	b := binary.AppendUvarint(make([]byte, 0, len(nodes)*16+1), uint64(len(nodes)))
	for _, node := range nodes {
		b = binary.AppendUvarint(b, uint64(len(node)))
		b = append(b, node...)
		b = binary.AppendUvarint(b, vv[node])
	}
	return b
}

// ParseVector decodes the canonical encoding produced by Canonical.
func ParseVector(b []byte) (VersionVector, error) {
	count, n := binary.Uvarint(b)
	if n <= 0 {
		return nil, ErrMalformedVector
	}
	b = b[n:]
	vv := make(VersionVector, count)
	for range count {
		nameLen, n := binary.Uvarint(b)
		if n <= 0 || uint64(len(b[n:])) < nameLen {
			return nil, ErrMalformedVector
		}
		b = b[n:]
		node := string(b[:nameLen])
		b = b[nameLen:]
		c, n := binary.Uvarint(b)
		if n <= 0 {
			return nil, ErrMalformedVector
		}
		b = b[n:]
		vv[node] = c
	}
	if len(b) != 0 {
		return nil, ErrMalformedVector
	}
	return vv, nil
}
