package chunker

import (
	"bytes"
	"errors"
	"io"
	"math/bits"
	"math/rand/v2"
	"testing"

	"github.com/GhentiLabs/Trove/client/internal/hasher"
)

func genData(n int, seed uint64) []byte {
	r := rand.New(rand.NewPCG(seed, 0x9e3779b9))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

func chunkWith(data []byte, bufSize int) []Chunk {
	c := New(Options{Reader: bytes.NewReader(data), BufSize: bufSize})
	var out []Chunk
	for {
		ch, err := c.Next()
		if errors.Is(err, io.EOF) {
			return out
		}
		out = append(out, ch)
	}
}

func TestGearGolden(t *testing.T) {
	want := map[int]uint64{
		0:   0xC0E16B163A85A4DC,
		1:   0x890ACD8DD443C47C,
		2:   0xB3889D8A6DC47761,
		255: 0xFD4CBB1B3007D376,
	}
	for i, w := range want {
		if gear[i] != w {
			t.Errorf("gear[%d] = 0x%016X, want 0x%016X", i, gear[i], w)
		}
	}
}

func TestMasksFrozen(t *testing.T) {
	if got := bits.OnesCount64(MaskS); got != 22 {
		t.Errorf("MaskS popcount = %d, want 22", got)
	}
	if got := bits.OnesCount64(MaskL); got != 18 {
		t.Errorf("MaskL popcount = %d, want 18", got)
	}
	if MaskS&0xFFFF != 0 || MaskL&0xFFFF != 0 {
		t.Error("mask bits must sit in the upper region, not the low 16 bits")
	}
}

func TestDeterministic(t *testing.T) {
	data := genData(16<<20, 1)
	a, b := Split(data), Split(data)
	if len(a) != len(b) {
		t.Fatalf("chunk counts differ: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("chunk %d differs: %+v vs %+v", i, a[i], b[i])
		}
	}
}

func TestBufferSizeIndependence(t *testing.T) {
	data := genData(20<<20, 2)
	ref := Split(data)
	for _, bufSize := range []int{1, 7, 4096, MaxSize + 1} {
		got := chunkWith(data, bufSize)
		if len(got) != len(ref) {
			t.Fatalf("bufSize %d: %d chunks, want %d", bufSize, len(got), len(ref))
		}
		for i := range ref {
			if got[i] != ref[i] {
				t.Fatalf("bufSize %d: chunk %d = %+v, want %+v", bufSize, i, got[i], ref[i])
			}
		}
	}
}

func TestSizeBounds(t *testing.T) {
	data := genData(32<<20, 3)
	chunks := Split(data)
	if len(chunks) < 2 {
		t.Fatalf("expected multiple chunks, got %d", len(chunks))
	}
	var total int64
	for i, ch := range chunks {
		if ch.Offset != total {
			t.Fatalf("chunk %d offset = %d, want %d", i, ch.Offset, total)
		}
		if ch.Length > MaxSize {
			t.Fatalf("chunk %d length %d exceeds MaxSize", i, ch.Length)
		}
		if i < len(chunks)-1 && ch.Length < MinSize {
			t.Fatalf("non-final chunk %d length %d below MinSize", i, ch.Length)
		}
		total += int64(ch.Length)
	}
	if total != int64(len(data)) {
		t.Fatalf("chunk lengths sum to %d, want %d", total, len(data))
	}
}

func contentIDs(data []byte, chunks []Chunk) map[hasher.ChunkID]struct{} {
	ids := make(map[hasher.ChunkID]struct{}, len(chunks))
	for _, ch := range chunks {
		ids[hasher.Sum(data[ch.Offset:ch.Offset+int64(ch.Length)])] = struct{}{}
	}
	return ids
}

func TestShiftResistance(t *testing.T) {
	data := genData(32<<20, 4)
	orig := Split(data)
	origIDs := contentIDs(data, orig)

	insertAt := 100 << 10
	modified := make([]byte, 0, len(data)+1024)
	modified = append(modified, data[:insertAt]...)
	modified = append(modified, genData(1024, 99)...)
	modified = append(modified, data[insertAt:]...)

	modChunks := Split(modified)
	modIDs := contentIDs(modified, modChunks)

	shared := 0
	for id := range origIDs {
		if _, ok := modIDs[id]; ok {
			shared++
		}
	}
	ratio := float64(shared) / float64(len(origIDs))
	if ratio < 0.85 {
		t.Fatalf("only %.0f%% of chunks survived a 1 KiB insertion, want >= 85%%", ratio*100)
	}
}

func FuzzChunkerRoundTrip(f *testing.F) {
	f.Add([]byte(""))
	f.Add([]byte("hello world"))
	f.Add(bytes.Repeat([]byte("ab"), 300_000))
	f.Add(genData(5<<20, 42))

	f.Fuzz(func(t *testing.T, data []byte) {
		chunks := Split(data)

		var total int64
		var rebuilt []byte
		for i, ch := range chunks {
			if ch.Offset != total {
				t.Fatalf("chunk %d offset = %d, want %d", i, ch.Offset, total)
			}
			if ch.Length > MaxSize {
				t.Fatalf("chunk %d length %d exceeds MaxSize", i, ch.Length)
			}
			if i < len(chunks)-1 && ch.Length < MinSize {
				t.Fatalf("non-final chunk %d length %d below MinSize", i, ch.Length)
			}
			rebuilt = append(rebuilt, data[ch.Offset:ch.Offset+int64(ch.Length)]...)
			total += int64(ch.Length)
		}
		if !bytes.Equal(rebuilt, data) {
			t.Fatal("concatenated chunks do not reproduce input")
		}

		for _, bufSize := range []int{1, 4096} {
			got := chunkWith(data, bufSize)
			if len(got) != len(chunks) {
				t.Fatalf("bufSize %d: %d chunks, want %d", bufSize, len(got), len(chunks))
			}
			for i := range chunks {
				if got[i] != chunks[i] {
					t.Fatalf("bufSize %d: chunk %d differs", bufSize, i)
				}
			}
		}
	})
}

func TestSmallInputs(t *testing.T) {
	for _, n := range []int{0, 1, MinSize - 1, MinSize, MinSize + 1} {
		data := genData(n, uint64(n)+1)
		chunks := Split(data)
		if n == 0 {
			if len(chunks) != 0 {
				t.Fatalf("n=0: got %d chunks", len(chunks))
			}
			continue
		}
		if len(chunks) != 1 {
			t.Fatalf("n=%d: got %d chunks, want 1", n, len(chunks))
		}
		if chunks[0].Length != n {
			t.Fatalf("n=%d: length %d", n, chunks[0].Length)
		}
	}
}
