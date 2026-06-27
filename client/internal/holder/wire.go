package holder

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/GhentiLabs/Trove/client/internal/crypto"
)

// The holder data-stream protocol frames blind blob transfer on dedicated QUIC streams,
// distinct from the chunk-sync data streams and the protobuf control framing. A peer
// opens a stream, writes a request, and reads a response; stream close ends the exchange.
// The layout is a frozen cross-node contract (golden-pinned).
const (
	// HolderMagic prefixes every holder data-stream frame: "THLD" big-endian.
	HolderMagic uint32 = 0x54484C44
	// HolderVersion gates the holder data-stream layout.
	HolderVersion uint8 = 1
	// MaxBlobBytes bounds one blob (a sealed chunk or the sealed catalog).
	MaxBlobBytes uint32 = 64 << 20
	// MaxHolderFolderIDLen bounds the folder id in a request.
	MaxHolderFolderIDLen = 512

	opGet byte = 0x01
	opPut byte = 0x02
)

// BlobStatus is the result byte in a holder response.
type BlobStatus uint8

const (
	// StatusOK precedes a payload (the requested blob, or empty on a successful put).
	StatusOK BlobStatus = 0
	// StatusNotFound means the holder has no blob under the blinded id.
	StatusNotFound BlobStatus = 1
	// StatusError means the holder failed to serve or store the blob.
	StatusError BlobStatus = 2
)

var (
	errBadHolderMagic  = errors.New("holder: bad data-stream magic")
	errHolderVersion   = errors.New("holder: unsupported data-stream version")
	errBadOp           = errors.New("holder: unknown data-stream op")
	errHolderIDTooLong = errors.New("holder: folder id exceeds maximum length")
	errBlobTooLarge    = errors.New("holder: blob exceeds maximum length")
)

func writeRequest(w io.Writer, op byte, folderID string, blinded [crypto.BlindLen]byte, payload []byte) error {
	if len(folderID) > MaxHolderFolderIDLen {
		return errHolderIDTooLong
	}
	if uint32(len(payload)) > MaxBlobBytes {
		return errBlobTooLarge
	}
	buf := make([]byte, 0, 8+len(folderID)+crypto.BlindLen+4+len(payload))
	buf = binary.BigEndian.AppendUint32(buf, HolderMagic)
	buf = append(buf, HolderVersion, op)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(folderID)))
	buf = append(buf, folderID...)
	buf = append(buf, blinded[:]...)
	if op == opPut {
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(payload)))
		buf = append(buf, payload...)
	}
	if _, err := w.Write(buf); err != nil {
		return fmt.Errorf("holder: write request: %w", err)
	}
	return nil
}

func readRequest(r io.Reader) (op byte, folderID string, blinded [crypto.BlindLen]byte, payload []byte, err error) {
	var head [8]byte
	if _, err = io.ReadFull(r, head[:]); err != nil {
		return 0, "", blinded, nil, fmt.Errorf("holder: read request header: %w", err)
	}
	if binary.BigEndian.Uint32(head[0:4]) != HolderMagic {
		return 0, "", blinded, nil, errBadHolderMagic
	}
	if head[4] != HolderVersion {
		return 0, "", blinded, nil, errHolderVersion
	}
	op = head[5]
	if op != opGet && op != opPut {
		return 0, "", blinded, nil, errBadOp
	}
	folderLen := binary.BigEndian.Uint16(head[6:8])
	if folderLen > MaxHolderFolderIDLen {
		return 0, "", blinded, nil, errHolderIDTooLong
	}
	body := make([]byte, int(folderLen)+crypto.BlindLen)
	if _, err = io.ReadFull(r, body); err != nil {
		return 0, "", blinded, nil, fmt.Errorf("holder: read request body: %w", err)
	}
	folderID = string(body[:folderLen])
	copy(blinded[:], body[folderLen:])
	if op == opPut {
		var lenBuf [4]byte
		if _, err = io.ReadFull(r, lenBuf[:]); err != nil {
			return 0, "", blinded, nil, fmt.Errorf("holder: read payload length: %w", err)
		}
		n := binary.BigEndian.Uint32(lenBuf[:])
		if n > MaxBlobBytes {
			return 0, "", blinded, nil, errBlobTooLarge
		}
		payload = make([]byte, n)
		if _, err = io.ReadFull(r, payload); err != nil {
			return 0, "", blinded, nil, fmt.Errorf("holder: read payload: %w", err)
		}
	}
	return op, folderID, blinded, payload, nil
}

func writeResponse(w io.Writer, status BlobStatus, payload []byte) error {
	if uint32(len(payload)) > MaxBlobBytes {
		return errBlobTooLarge
	}
	head := make([]byte, 0, 12+len(payload))
	head = binary.BigEndian.AppendUint32(head, HolderMagic)
	head = append(head, HolderVersion, byte(status), 0, 0)
	head = binary.BigEndian.AppendUint32(head, uint32(len(payload)))
	head = append(head, payload...)
	if _, err := w.Write(head); err != nil {
		return fmt.Errorf("holder: write response: %w", err)
	}
	return nil
}

func readResponse(r io.Reader) (BlobStatus, []byte, error) {
	var head [12]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return 0, nil, fmt.Errorf("holder: read response header: %w", err)
	}
	if binary.BigEndian.Uint32(head[0:4]) != HolderMagic {
		return 0, nil, errBadHolderMagic
	}
	if head[4] != HolderVersion {
		return 0, nil, errHolderVersion
	}
	n := binary.BigEndian.Uint32(head[8:12])
	if n > MaxBlobBytes {
		return 0, nil, errBlobTooLarge
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, fmt.Errorf("holder: read response payload: %w", err)
	}
	return BlobStatus(head[5]), payload, nil
}
