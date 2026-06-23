// Package chunker splits a byte stream into content-defined chunks using
// normalized FastCDC. Boundaries are determined by a Gear rolling hash, so the
// same content always cuts at the same points and an edit re-chunks only locally.
// The Gear table, the two masks, and the cut-point convention (cut returns the
// chunk length, excluding the boundary byte) are frozen protocol constants:
// changing any of them changes every chunk boundary and therefore every chunk
// identity.
package chunker

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

const (
	MinSize = 256 << 10
	AvgSize = 1 << 20
	MaxSize = 4 << 20
)

// Frozen masks. MaskS (popcount 22) makes a cut harder before the average size,
// suppressing tiny chunks; MaskL (popcount 18) makes one easier after it,
// suppressing oversized chunks. Bits sit in the upper word where the Gear hash
// has mixed most.
const (
	MaskS uint64 = 0x954AA552A9550000
	MaskL uint64 = 0x924A494929250000
)

const gearSeed uint64 = 0x2545F4914F6CDD1D

const defaultReadSize = 64 << 10

var gear [256]uint64

func init() {
	state := gearSeed
	for i := range gear {
		state += 0x9E3779B97F4A7C15
		z := state
		z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
		z = (z ^ (z >> 27)) * 0x94D049BB133111EB
		z ^= z >> 31
		gear[i] = z
	}
}

// Chunk is a boundary in the input stream.
type Chunk struct {
	Offset int64
	Length int
}

// Options configures New. BufSize is a read-granularity hint only; it never
// affects boundaries.
type Options struct {
	Reader  io.Reader
	BufSize int
}

// Chunker yields successive chunk boundaries without loading the whole input.
type Chunker struct {
	r       io.Reader
	scratch []byte
	buf     []byte
	off     int64
	eof     bool
}

// New returns a Chunker reading from opts.Reader.
func New(opts Options) *Chunker {
	size := opts.BufSize
	if size <= 0 {
		size = defaultReadSize
	}
	return &Chunker{r: opts.Reader, scratch: make([]byte, size)}
}

// Next returns the next chunk boundary, or io.EOF once the input is exhausted.
func (c *Chunker) Next() (Chunk, error) {
	ch, _, err := c.step(false)
	return ch, err
}

// NextChunk returns the next chunk boundary and a copy of its bytes, or io.EOF.
func (c *Chunker) NextChunk() (Chunk, []byte, error) {
	return c.step(true)
}

func (c *Chunker) step(wantBytes bool) (Chunk, []byte, error) {
	if err := c.fill(MaxSize); err != nil {
		return Chunk{}, nil, err
	}
	if len(c.buf) == 0 {
		return Chunk{}, nil, io.EOF
	}
	n := cut(c.buf)
	ch := Chunk{Offset: c.off, Length: n}
	var data []byte
	if wantBytes {
		data = make([]byte, n)
		copy(data, c.buf[:n])
	}
	c.off += int64(n)
	c.buf = c.buf[:copy(c.buf, c.buf[n:])]
	return ch, data, nil
}

func (c *Chunker) fill(target int) error {
	for len(c.buf) < target && !c.eof {
		n, err := c.r.Read(c.scratch)
		if n > 0 {
			c.buf = append(c.buf, c.scratch[:n]...)
		}
		switch {
		case errors.Is(err, io.EOF):
			c.eof = true
		case err != nil:
			return err
		}
	}
	return nil
}

func cut(buf []byte) int {
	n := len(buf)
	if n <= MinSize {
		return n
	}
	normal, maxb := AvgSize, MaxSize
	if n < maxb {
		maxb = n
	}
	if n < normal {
		normal = n
	}

	var fp uint64
	i := MinSize
	for ; i < normal; i++ {
		fp = (fp << 1) + gear[buf[i]]
		if fp&MaskS == 0 {
			return i
		}
	}
	for ; i < maxb; i++ {
		fp = (fp << 1) + gear[buf[i]]
		if fp&MaskL == 0 {
			return i
		}
	}
	return i
}

// Split returns all chunk boundaries in data. It shares the streaming core, so
// results match Next over any reader.
func Split(data []byte) []Chunk {
	c := New(Options{Reader: bytes.NewReader(data)})
	var out []Chunk
	for {
		ch, err := c.Next()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			panic(fmt.Sprintf("chunker: Split over bytes.Reader returned %v", err))
		}
		out = append(out, ch)
	}
}
