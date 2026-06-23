// Package compression applies per-chunk zstd compression on the storage path,
// after chunking and before encryption. Each chunk records the codec used so the
// read path knows how to reverse it. If a chunk does not compress smaller it is
// stored uncompressed (CodecNone), so stored size never exceeds plaintext size.
package compression

import (
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// Codec identifies how a chunk's stored bytes were compressed. Values are
// persisted in the chunk index and must remain stable.
type Codec uint8

const (
	CodecNone Codec = 0
	CodecZstd Codec = 1
)

// Pooled and reused via EncodeAll/DecodeAll; never Closed, which would free them.
var (
	encoders = sync.Pool{New: func() any {
		e, err := zstd.NewWriter(nil)
		if err != nil {
			panic(fmt.Sprintf("compression: new encoder: %v", err))
		}
		return e
	}}
	decoders = sync.Pool{New: func() any {
		d, err := zstd.NewReader(nil)
		if err != nil {
			panic(fmt.Sprintf("compression: new decoder: %v", err))
		}
		return d
	}}
)

// Compress returns the smaller of the zstd-compressed and original bytes and the
// codec used; CodecNone passes src through unchanged.
func Compress(src []byte) (Codec, []byte) {
	enc := encoders.Get().(*zstd.Encoder)
	defer encoders.Put(enc)

	out := enc.EncodeAll(src, nil)
	if len(out) < len(src) {
		return CodecZstd, out
	}
	return CodecNone, src
}

// Decompress reverses Compress for the given codec.
func Decompress(codec Codec, data []byte) ([]byte, error) {
	switch codec {
	case CodecNone:
		return data, nil
	case CodecZstd:
		dec := decoders.Get().(*zstd.Decoder)
		defer decoders.Put(dec)
		out, err := dec.DecodeAll(data, nil)
		if err != nil {
			return nil, fmt.Errorf("compression: decode: %w", err)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("compression: unknown codec %d", codec)
	}
}
