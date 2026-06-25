package wire

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/GhentiLabs/Trove/client/internal/compression"
	"github.com/GhentiLabs/Trove/client/internal/wire/wirepb"
	"google.golang.org/protobuf/proto"
)

// MessageType identifies a post-Hello message body. Values are frozen.
type MessageType uint32

const (
	// TypeNetworkConfig is the first post-Hello message, sent once each way.
	TypeNetworkConfig MessageType = 1
	// TypePing is the idle keepalive.
	TypePing MessageType = 2
	// TypeClose requests a graceful shutdown.
	TypeClose MessageType = 3
	// TypeFolderSummary announces an owner folder's current root and resync cursor.
	TypeFolderSummary MessageType = 4
	// TypeManifestRequest asks an owner for the manifest delta since a cursor.
	TypeManifestRequest MessageType = 5
	// TypeManifestDelta carries the requested manifest delta back to a replica.
	TypeManifestDelta MessageType = 6
	// TypeMembershipGossip carries a network roster for anti-entropy convergence.
	TypeMembershipGossip MessageType = 7
)

// WriteMessage frames m as uint16 header_len ‖ Header ‖ uint32 msg_len ‖ body.
func WriteMessage(w io.Writer, m proto.Message) error {
	t, ok := typeOf(m)
	if !ok {
		return ErrUnknownType
	}
	body, err := proto.Marshal(m)
	if err != nil {
		return fmt.Errorf("wire: marshal body: %w", err)
	}
	if len(body) > MaxMessageSize {
		return ErrMessageTooLarge
	}
	codec, payload := compression.Compress(body)
	if len(payload) > MaxMessageSize {
		return ErrMessageTooLarge
	}
	hb, err := proto.Marshal(&wirepb.Header{Type: uint32(t), Compression: uint32(codec)})
	if err != nil {
		return fmt.Errorf("wire: marshal header: %w", err)
	}
	if len(hb) > maxHeaderSize {
		return fmt.Errorf("wire: header too large: %d bytes", len(hb))
	}
	var prefix [2]byte
	binary.BigEndian.PutUint16(prefix[:], uint16(len(hb)))
	if _, err := w.Write(prefix[:]); err != nil {
		return fmt.Errorf("wire: write header: %w", err)
	}
	if _, err := w.Write(hb); err != nil {
		return fmt.Errorf("wire: write header: %w", err)
	}
	var mlen [4]byte
	binary.BigEndian.PutUint32(mlen[:], uint32(len(payload)))
	if _, err := w.Write(mlen[:]); err != nil {
		return fmt.Errorf("wire: write length: %w", err)
	}
	if len(payload) == 0 {
		return nil
	}
	if _, err := w.Write(payload); err != nil {
		return fmt.Errorf("wire: write body: %w", err)
	}
	return nil
}

// ReadMessage reads one post-Hello frame, returning its type and decoded body.
func ReadMessage(r io.Reader) (MessageType, proto.Message, error) {
	return readMessage(r, MaxMessageSize)
}

// ReadControlMessage reads a control-stream frame, rejecting bodies larger than
// MaxControlMessageSize.
func ReadControlMessage(r io.Reader) (MessageType, proto.Message, error) {
	return readMessage(r, MaxControlMessageSize)
}

func readMessage(r io.Reader, maxBody uint32) (MessageType, proto.Message, error) {
	var prefix [2]byte
	if _, err := io.ReadFull(r, prefix[:]); err != nil {
		return 0, nil, fmt.Errorf("wire: read header length: %w", err)
	}
	hb := make([]byte, binary.BigEndian.Uint16(prefix[:]))
	if _, err := io.ReadFull(r, hb); err != nil {
		return 0, nil, fmt.Errorf("wire: read header: %w", err)
	}
	var hdr wirepb.Header
	if err := proto.Unmarshal(hb, &hdr); err != nil {
		return 0, nil, fmt.Errorf("wire: unmarshal header: %w", err)
	}
	var mlen [4]byte
	if _, err := io.ReadFull(r, mlen[:]); err != nil {
		return 0, nil, fmt.Errorf("wire: read length: %w", err)
	}
	n := binary.BigEndian.Uint32(mlen[:])
	if n > maxBody {
		return 0, nil, ErrMessageTooLarge
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, fmt.Errorf("wire: read body: %w", err)
	}
	body, err := compression.Decompress(compression.Codec(hdr.GetCompression()), payload, int(maxBody))
	if err != nil {
		return 0, nil, err
	}
	t := MessageType(hdr.GetType())
	m, ok := newMessage(t)
	if !ok {
		return 0, nil, fmt.Errorf("%w: %d", ErrUnknownType, t)
	}
	if err := proto.Unmarshal(body, m); err != nil {
		return 0, nil, fmt.Errorf("wire: unmarshal body: %w", err)
	}
	return t, m, nil
}

func typeOf(m proto.Message) (MessageType, bool) {
	switch m.(type) {
	case *wirepb.NetworkConfig:
		return TypeNetworkConfig, true
	case *wirepb.Ping:
		return TypePing, true
	case *wirepb.Close:
		return TypeClose, true
	case *wirepb.FolderSummary:
		return TypeFolderSummary, true
	case *wirepb.ManifestRequest:
		return TypeManifestRequest, true
	case *wirepb.ManifestDelta:
		return TypeManifestDelta, true
	case *wirepb.MembershipGossip:
		return TypeMembershipGossip, true
	default:
		return 0, false
	}
}

func newMessage(t MessageType) (proto.Message, bool) {
	switch t {
	case TypeNetworkConfig:
		return &wirepb.NetworkConfig{}, true
	case TypePing:
		return &wirepb.Ping{}, true
	case TypeClose:
		return &wirepb.Close{}, true
	case TypeFolderSummary:
		return &wirepb.FolderSummary{}, true
	case TypeManifestRequest:
		return &wirepb.ManifestRequest{}, true
	case TypeManifestDelta:
		return &wirepb.ManifestDelta{}, true
	case TypeMembershipGossip:
		return &wirepb.MembershipGossip{}, true
	default:
		return nil, false
	}
}
