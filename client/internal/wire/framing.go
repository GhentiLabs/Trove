// Package wire is the frozen cross-node protocol contract: a hand-rolled two-tier
// framing around protobuf message bodies. The framing widths, the magic, the
// maximum message size, and the message type values are golden-pinned and governed
// by WireFormatVersion; a peer expecting a different layout must fail at Hello
// rather than silently misparse. Message bodies are protobuf (see wirepb); only the
// envelope and the bulk data plane (M4) are not.
package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/GhentiLabs/Trove/client/internal/wire/wirepb"
	"google.golang.org/protobuf/proto"
)

// Frozen wire constants. Changing any of these is a wire-format break.
const (
	// Magic prefixes the Hello frame: "TROV" big-endian. Distinct from BEP's magic,
	// it gives instant protocol identification and fast-rejects non-protocol peers.
	Magic uint32 = 0x54524F56
	// WireFormatVersion gates the whole layout; an incompatible peer is rejected.
	WireFormatVersion uint32 = 1
	// MaxMessageSize bounds a single post-Hello message body on the wire, sized for
	// M4's larger index/manifest messages. An oversize frame closes the connection.
	MaxMessageSize = 64 << 20

	// maxHelloSize and maxHeaderSize bound two separate uint16 length fields (the
	// Hello body and the post-Hello header). They share a value today; keeping them
	// distinct makes a future divergence a visible change, not a silent misbound.
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

// ReadHello reads and validates a Hello frame, returning ErrBadMagic if the magic
// does not match. It does not check WireFormatVersion; the session decides that so
// it can still report who tried before rejecting.
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
