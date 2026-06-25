package syncengine

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/GhentiLabs/Trove/client/internal/chunker"
	"github.com/GhentiLabs/Trove/client/internal/hasher"
)

// The data-stream protocol frames chunk transfer on dedicated QUIC streams, outside
// the protobuf control framing. A replica opens a stream, writes a request header,
// and reads a response header followed by the raw plaintext chunk bytes; stream
// close signals completion. The layout is a frozen cross-node contract.
const (
	// DataMagic prefixes every data-stream header: "TDRT" big-endian.
	DataMagic uint32 = 0x54445254
	// DataVersion gates the data-stream layout independently of the control framing.
	DataVersion uint8 = 1
	// MaxFolderIDLen bounds the folder id in a request header.
	MaxFolderIDLen = 512
	// MaxChunkBytes bounds a response payload, matching the chunker's hard cap.
	MaxChunkBytes = chunker.MaxSize

	msgKindChunk byte = 0x01
)

// ChunkStatus is the result byte in a chunk response header.
type ChunkStatus uint8

const (
	// StatusOK precedes a payload of the requested chunk's plaintext bytes.
	StatusOK ChunkStatus = 0
	// StatusNotFound means the owner no longer holds the chunk (snapshot advanced).
	StatusNotFound ChunkStatus = 1
	// StatusError means the owner failed to read the chunk.
	StatusError ChunkStatus = 2
)

var (
	errBadDataMagic    = errors.New("syncengine: bad data-stream magic")
	errDataVersion     = errors.New("syncengine: unsupported data-stream version")
	errBadMsgKind      = errors.New("syncengine: unknown data-stream message kind")
	errFolderIDTooLong = errors.New("syncengine: folder id exceeds maximum length")
	errChunkTooLarge   = errors.New("syncengine: chunk length exceeds maximum")
)

// writeChunkRequest frames a chunk request: DataMagic ‖ DataVersion ‖ kind ‖
// uint16 folder-id length ‖ folder id ‖ 32-byte chunk id.
func writeChunkRequest(w io.Writer, folderID string, id hasher.ChunkID) error {
	if len(folderID) > MaxFolderIDLen {
		return errFolderIDTooLong
	}
	buf := make([]byte, 0, 8+len(folderID)+hasher.IDLen)
	buf = binary.BigEndian.AppendUint32(buf, DataMagic)
	buf = append(buf, DataVersion, msgKindChunk)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(folderID)))
	buf = append(buf, folderID...)
	buf = append(buf, id[:]...)
	if _, err := w.Write(buf); err != nil {
		return fmt.Errorf("syncengine: write request: %w", err)
	}
	return nil
}

// readChunkRequest decodes a request header, enforcing the magic, version, kind, and
// folder-id bound before allocating.
func readChunkRequest(r io.Reader) (string, hasher.ChunkID, error) {
	var head [8]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return "", hasher.ChunkID{}, fmt.Errorf("syncengine: read request header: %w", err)
	}
	if binary.BigEndian.Uint32(head[0:4]) != DataMagic {
		return "", hasher.ChunkID{}, errBadDataMagic
	}
	if head[4] != DataVersion {
		return "", hasher.ChunkID{}, errDataVersion
	}
	if head[5] != msgKindChunk {
		return "", hasher.ChunkID{}, errBadMsgKind
	}
	folderLen := binary.BigEndian.Uint16(head[6:8])
	if folderLen > MaxFolderIDLen {
		return "", hasher.ChunkID{}, errFolderIDTooLong
	}
	body := make([]byte, int(folderLen)+hasher.IDLen)
	if _, err := io.ReadFull(r, body); err != nil {
		return "", hasher.ChunkID{}, fmt.Errorf("syncengine: read request body: %w", err)
	}
	var id hasher.ChunkID
	copy(id[:], body[folderLen:])
	return string(body[:folderLen]), id, nil
}

// writeChunkResponseHeader frames a response header: DataMagic ‖ DataVersion ‖
// status ‖ two reserved zero bytes ‖ uint32 payload length.
func writeChunkResponseHeader(w io.Writer, status ChunkStatus, length uint32) error {
	var head [12]byte
	binary.BigEndian.PutUint32(head[0:4], DataMagic)
	head[4] = DataVersion
	head[5] = byte(status)
	binary.BigEndian.PutUint32(head[8:12], length)
	if _, err := w.Write(head[:]); err != nil {
		return fmt.Errorf("syncengine: write response: %w", err)
	}
	return nil
}

// readChunkResponseHeader decodes a response header, enforcing the magic, version,
// and payload bound before the caller reads the payload.
func readChunkResponseHeader(r io.Reader) (ChunkStatus, uint32, error) {
	var head [12]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return 0, 0, fmt.Errorf("syncengine: read response header: %w", err)
	}
	if binary.BigEndian.Uint32(head[0:4]) != DataMagic {
		return 0, 0, errBadDataMagic
	}
	if head[4] != DataVersion {
		return 0, 0, errDataVersion
	}
	length := binary.BigEndian.Uint32(head[8:12])
	if length > MaxChunkBytes {
		return 0, 0, errChunkTooLarge
	}
	return ChunkStatus(head[5]), length, nil
}
