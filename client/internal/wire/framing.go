// Package wire is the frozen cross-node protocol contract: a hand-rolled two-tier
// framing around protobuf message bodies, governed by WireFormatVersion. The widths,
// magic, max size, and type values are golden-pinned; see docs/m3-wire-constants.md.
package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/GhentiLabs/Trove/client/internal/wire/wirepb"
	"google.golang.org/protobuf/proto"
)

const (
	// Magic prefixes the Hello frame: "TROV" big-endian.
	Magic uint32 = 0x54524F56
	// WireFormatVersion gates the whole layout; an incompatible peer is rejected.
	WireFormatVersion uint32 = 1
	// MaxMessageSize bounds a single post-Hello message body on the wire.
	MaxMessageSize = 64 << 20
	// MaxControlMessageSize caps a control-stream frame, on the wire and after
	// decompression. The handshake carries only small messages, so it stays far
	// below MaxMessageSize, bounding what an authenticated peer can make the
	// receiver allocate on the control path.
	MaxControlMessageSize = 1 << 20

	maxHelloSize  = 1<<16 - 1
	maxHeaderSize = 1<<16 - 1
)

var (
	// ErrBadMagic is returned when a Hello frame does not start with Magic.
	ErrBadMagic = errors.New("wire: bad magic")
	// ErrMessageTooLarge is returned when a frame's body exceeds MaxMessageSize.
	ErrMessageTooLarge = errors.New("wire: message exceeds maximum size")
	// ErrUnknownType is returned for a message type with no registered mapping.
	ErrUnknownType = errors.New("wire: unknown message type")
)

// WriteHello writes the pre-authorization Hello frame: Magic ‖ uint16 size ‖ body.
func WriteHello(w io.Writer, h *wirepb.Hello) error {
	body, err := proto.Marshal(h)
	if err != nil {
		return fmt.Errorf("wire: marshal hello: %w", err)
	}
	if len(body) > maxHelloSize {
		return fmt.Errorf("wire: hello too large: %d bytes", len(body))
	}
	var hdr [6]byte
	binary.BigEndian.PutUint32(hdr[0:4], Magic)
	binary.BigEndian.PutUint16(hdr[4:6], uint16(len(body)))
	if _, err := w.Write(hdr[:]); err != nil {
		return fmt.Errorf("wire: write hello: %w", err)
	}
	if len(body) == 0 {
		return nil
	}
	if _, err := w.Write(body); err != nil {
		return fmt.Errorf("wire: write hello: %w", err)
	}
	return nil
}

// ReadHello reads and validates a Hello frame, returning ErrBadMagic on mismatch.
func ReadHello(r io.Reader) (*wirepb.Hello, error) {
	var hdr [6]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, fmt.Errorf("wire: read hello: %w", err)
	}
	if binary.BigEndian.Uint32(hdr[0:4]) != Magic {
		return nil, ErrBadMagic
	}
	body := make([]byte, binary.BigEndian.Uint16(hdr[4:6]))
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, fmt.Errorf("wire: read hello: %w", err)
	}
	h := &wirepb.Hello{}
	if err := proto.Unmarshal(body, h); err != nil {
		return nil, fmt.Errorf("wire: unmarshal hello: %w", err)
	}
	return h, nil
}
