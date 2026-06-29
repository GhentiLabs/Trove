package holder

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"github.com/GhentiLabs/Trove/client/internal/hasher"
	"github.com/GhentiLabs/Trove/client/internal/manifest"
)

const catalogDomain = "trove/holder/catalog/v1\x00"

const (
	maxCatalogEntries = 1 << 24
	maxCatalogChunks  = 1 << 28
)

var errBadCatalog = errors.New("holder: malformed catalog")

// EncodeCatalog renders the folder's live manifests as canonical bytes, sorted by path.
func EncodeCatalog(manifests []manifest.Manifest) []byte {
	sorted := append([]manifest.Manifest(nil), manifests...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Path < sorted[j].Path })

	b := make([]byte, 0, len(catalogDomain)+8+len(sorted)*64)
	b = append(b, catalogDomain...)
	b = binary.AppendUvarint(b, uint64(len(sorted)))
	for _, m := range sorted {
		b = append(b, byte(m.Kind))
		b = appendBytes(b, []byte(m.Path))
		b = binary.AppendUvarint(b, uint64(m.Mode))
		b = appendBytes(b, []byte(m.SymlinkTarget))
		b = binary.AppendUvarint(b, uint64(len(m.Chunks)))
		for _, c := range m.Chunks {
			b = append(b, c.ID[:]...)
			b = binary.AppendUvarint(b, uint64(c.Length))
		}
	}
	return b
}

// DecodeCatalog parses bytes produced by EncodeCatalog. The input must already be
// AEAD-authenticated; this only guards against a malformed structure.
func DecodeCatalog(b []byte) ([]manifest.Manifest, error) {
	if len(b) < len(catalogDomain) || string(b[:len(catalogDomain)]) != catalogDomain {
		return nil, errBadCatalog
	}
	r := &catalogReader{b: b[len(catalogDomain):]}
	n, err := r.uvarint()
	if err != nil || n > maxCatalogEntries {
		return nil, errBadCatalog
	}
	out := make([]manifest.Manifest, 0, n)
	for range n {
		kind, err := r.byte()
		if err != nil {
			return nil, errBadCatalog
		}
		path, err := r.bytes()
		if err != nil {
			return nil, errBadCatalog
		}
		mode, err := r.uvarint()
		if err != nil {
			return nil, errBadCatalog
		}
		target, err := r.bytes()
		if err != nil {
			return nil, errBadCatalog
		}
		nc, err := r.uvarint()
		if err != nil || nc > maxCatalogChunks {
			return nil, errBadCatalog
		}
		chunks := make([]manifest.ChunkRef, 0, nc)
		for range nc {
			id, err := r.chunkID()
			if err != nil {
				return nil, errBadCatalog
			}
			length, err := r.uvarint()
			if err != nil {
				return nil, errBadCatalog
			}
			chunks = append(chunks, manifest.ChunkRef{ID: id, Length: int64(length)})
		}
		out = append(out, manifest.Manifest{
			Kind: manifest.Kind(kind), Path: string(path), Mode: uint32(mode),
			SymlinkTarget: string(target), Chunks: chunks,
		})
	}
	if len(r.b) != 0 {
		return nil, fmt.Errorf("%w: %d trailing bytes", errBadCatalog, len(r.b))
	}
	return out, nil
}

func appendBytes(b, v []byte) []byte {
	b = binary.AppendUvarint(b, uint64(len(v)))
	return append(b, v...)
}

type catalogReader struct{ b []byte }

func (r *catalogReader) byte() (byte, error) {
	if len(r.b) < 1 {
		return 0, errBadCatalog
	}
	v := r.b[0]
	r.b = r.b[1:]
	return v, nil
}

func (r *catalogReader) uvarint() (uint64, error) {
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		return 0, errBadCatalog
	}
	r.b = r.b[n:]
	return v, nil
}

func (r *catalogReader) bytes() ([]byte, error) {
	n, err := r.uvarint()
	if err != nil || n > uint64(len(r.b)) {
		return nil, errBadCatalog
	}
	v := r.b[:n]
	r.b = r.b[n:]
	return v, nil
}

func (r *catalogReader) chunkID() (hasher.ChunkID, error) {
	var id hasher.ChunkID
	if len(r.b) < hasher.IDLen {
		return id, errBadCatalog
	}
	copy(id[:], r.b[:hasher.IDLen])
	r.b = r.b[hasher.IDLen:]
	return id, nil
}
