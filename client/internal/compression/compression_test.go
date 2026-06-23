package compression

import (
	"bytes"
	"crypto/rand"
	"sync"
	"testing"
)

func TestCompressibleRoundTrip(t *testing.T) {
	src := bytes.Repeat([]byte("the quick brown fox "), 1000)
	codec, packed := Compress(src)
	if codec != CodecZstd {
		t.Fatalf("codec = %d, want CodecZstd", codec)
	}
	if len(packed) >= len(src) {
		t.Fatalf("compressed size %d not smaller than %d", len(packed), len(src))
	}
	got, err := Decompress(codec, packed)
	if err != nil {
		t.Fatalf("Decompress: %v", err)
	}
	if !bytes.Equal(got, src) {
		t.Fatal("round trip mismatch")
	}
}

func TestIncompressibleStoredVerbatim(t *testing.T) {
	src := make([]byte, 4096)
	if _, err := rand.Read(src); err != nil {
		t.Fatalf("rand: %v", err)
	}
	codec, packed := Compress(src)
	if codec != CodecNone {
		t.Fatalf("codec = %d, want CodecNone for random data", codec)
	}
	if !bytes.Equal(packed, src) {
		t.Fatal("CodecNone must pass bytes through unchanged")
	}
	got, err := Decompress(codec, packed)
	if err != nil {
		t.Fatalf("Decompress: %v", err)
	}
	if !bytes.Equal(got, src) {
		t.Fatal("round trip mismatch")
	}
}

func TestUnknownCodec(t *testing.T) {
	if _, err := Decompress(Codec(99), []byte("x")); err == nil {
		t.Fatal("expected error for unknown codec")
	}
}

func TestConcurrentPoolReuse(t *testing.T) {
	src := bytes.Repeat([]byte("payload"), 500)
	var wg sync.WaitGroup
	var bad int64
	var mu sync.Mutex
	for range 32 {
		wg.Go(func() {
			for range 50 {
				codec, packed := Compress(src)
				got, err := Decompress(codec, packed)
				if err != nil || !bytes.Equal(got, src) {
					mu.Lock()
					bad++
					mu.Unlock()
				}
			}
		})
	}
	wg.Wait()
	if bad != 0 {
		t.Fatalf("%d concurrent round trips failed", bad)
	}
}
