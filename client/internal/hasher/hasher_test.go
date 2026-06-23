package hasher

import (
	"slices"
	"testing"
)

// Official BLAKE3 test vectors (input = bytes 0,1,2,... mod 251).
var blake3Vectors = []struct {
	inputLen int
	hex      string
}{
	{0, "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"},
	{1, "2d3adedff11b61f14c886e35afa036736dcd87a74d27b5c1510225d0f592e213"},
	{1024, "42214739f095a406f3fc83deb889744ac00df831c10daa55189b5d121c855af7"},
}

func vectorInput(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

func TestSumKnownVectors(t *testing.T) {
	for _, v := range blake3Vectors {
		got := Sum(vectorInput(v.inputLen)).String()
		if got != v.hex {
			t.Errorf("Sum(len=%d) = %s, want %s", v.inputLen, got, v.hex)
		}
	}
}

func TestStreamingMatchesSum(t *testing.T) {
	data := vectorInput(4096)
	want := Sum(data)

	h := New()
	for chunk := range slices.Chunk(data, 100) {
		if _, err := h.Write(chunk); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if got := h.Sum(); got != want {
		t.Fatalf("streaming = %s, want %s", got, want)
	}

	h.Reset()
	if got := h.Sum(); got != Sum(nil) {
		t.Fatalf("after Reset = %s, want empty hash %s", got, Sum(nil))
	}
}

func TestParseRoundTrip(t *testing.T) {
	id := Sum([]byte("trove"))
	parsed, err := Parse(id.String())
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if parsed != id {
		t.Fatalf("round trip mismatch: %s vs %s", parsed, id)
	}

	from, err := FromBytes(id.Bytes())
	if err != nil {
		t.Fatalf("FromBytes: %v", err)
	}
	if from != id {
		t.Fatalf("FromBytes mismatch")
	}

	if _, err := Parse("tooshort"); err == nil {
		t.Fatal("expected error for short hex")
	}
	if _, err := FromBytes([]byte{1, 2, 3}); err == nil {
		t.Fatal("expected error for short bytes")
	}
}
