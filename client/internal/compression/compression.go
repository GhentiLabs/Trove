// Package compression applies per-chunk zstd compression, after chunking and before
// encryption. A chunk that does not compress smaller is stored uncompressed
// (CodecNone), so stored size never exceeds plaintext size.
package compression

import (
	"errors"
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// ErrTooLarge is returned when decoded output would exceed the caller's limit.
var ErrTooLarge = errors.New("compression: decoded size exceeds limit")

// Codec identifies how a chunk's stored bytes were compressed. Values are persisted
// and must remain stable.
type Codec uint8

const (
	CodecNone Codec = 0
	CodecZstd Codec = 1
)

// MaxDecodedSize is the largest plaintext any caller may decode: the 64 MiB wire
// message cap (wire.MaxMessageSize). It also bounds the pooled decoder against
// decompression bombs.
const MaxDecodedSize = 64 << 20

var (
	encoders = sync.Pool{New: func() any {
		e, err := zstd.NewWriter(nil)
		if err != nil {
			panic(fmt.Sprintf("compression: new encoder: %v", err))
		}
		return e
	}}
	decoders = sync.Pool{New: func() any {
		d, err := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(MaxDecodedSize))
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

// Decompress reverses Compress for the given codec, rejecting output larger than
// maxDecoded bytes.
func Decompress(codec Codec, data []byte, maxDecoded int) ([]byte, error) {
	switch codec {
	case CodecNone:
		if len(data) > maxDecoded {
			return nil, ErrTooLarge
		}
		return data, nil
	case CodecZstd:
		dec, release, err := decoderFor(maxDecoded)
		if err != nil {
			return nil, err
		}
		defer release()
		out, err := dec.DecodeAll(data, nil)
		if err != nil {
			return nil, fmt.Errorf("compression: decode: %w", err)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("compression: unknown codec %d", codec)
	}
}

// decoderFor returns a decoder bounded to maxDecoded bytes. The full-size path
// reuses the pooled decoder; a tighter limit gets a single-use one, which only a
// compressed control frame — never produced by Compress — reaches.
func decoderFor(maxDecoded int) (*zstd.Decoder, func(), error) {
	if maxDecoded >= MaxDecodedSize {
		dec := decoders.Get().(*zstd.Decoder)
		return dec, func() { decoders.Put(dec) }, nil
	}
	dec, err := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(uint64(maxDecoded)))
	if err != nil {
		return nil, nil, fmt.Errorf("compression: decoder: %w", err)
	}
	return dec, dec.Close, nil
}
