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
	// MaxFolderIDLen bounds the folder id in a request.
	MaxFolderIDLen = 512
	// MaxHasBatch bounds the number of blinded ids in one has-batch request.
	MaxHasBatch = 4096

	opGet      byte = 0x01
	opPut      byte = 0x02
	opHasBatch byte = 0x04
	opList     byte = 0x05
	opDelete   byte = 0x06
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

func writeRequest(w io.Writer, op byte, folderID string, blinded [crypto.BlindIDLen]byte, payload []byte) error {
	if len(folderID) > MaxFolderIDLen {
		return errHolderIDTooLong
	}
	if uint32(len(payload)) > MaxBlobBytes {
		return errBlobTooLarge
	}
	buf := make([]byte, 0, 8+len(folderID)+crypto.BlindIDLen+4+len(payload))
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

// readRequestHeader reads a request's op and folder id, leaving the op-specific body (a
// blinded id, an id list, or a payload) for the handler to read once it has authorized.
func readRequestHeader(r io.Reader) (op byte, folderID string, err error) {
	var head [8]byte
	if _, err = io.ReadFull(r, head[:]); err != nil {
		return 0, "", fmt.Errorf("holder: read request header: %w", err)
	}
	if binary.BigEndian.Uint32(head[0:4]) != HolderMagic {
		return 0, "", errBadHolderMagic
	}
	if head[4] != HolderVersion {
		return 0, "", errHolderVersion
	}
	op = head[5]
	if op != opGet && op != opPut && op != opHasBatch && op != opList && op != opDelete {
		return 0, "", errBadOp
	}
	folderLen := binary.BigEndian.Uint16(head[6:8])
	if folderLen > MaxFolderIDLen {
		return 0, "", errHolderIDTooLong
	}
	id := make([]byte, folderLen)
	if _, err = io.ReadFull(r, id); err != nil {
		return 0, "", fmt.Errorf("holder: read folder id: %w", err)
	}
	return op, string(id), nil
}

func readBlinded(r io.Reader) ([crypto.BlindIDLen]byte, error) {
	var b [crypto.BlindIDLen]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return b, fmt.Errorf("holder: read blinded id: %w", err)
	}
	return b, nil
}

func writeBlindedList(w io.Writer, op byte, folderID string, ids [][crypto.BlindIDLen]byte) error {
	if len(folderID) > MaxFolderIDLen {
		return errHolderIDTooLong
	}
	if len(ids) > MaxHasBatch {
		return fmt.Errorf("holder: has-batch of %d exceeds %d", len(ids), MaxHasBatch)
	}
	buf := make([]byte, 0, 8+len(folderID)+2+len(ids)*crypto.BlindIDLen)
	buf = binary.BigEndian.AppendUint32(buf, HolderMagic)
	buf = append(buf, HolderVersion, op)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(folderID)))
	buf = append(buf, folderID...)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(ids)))
	for _, id := range ids {
		buf = append(buf, id[:]...)
	}
	if _, err := w.Write(buf); err != nil {
		return fmt.Errorf("holder: write id list: %w", err)
	}
	return nil
}

func readBlindedList(r io.Reader) ([][crypto.BlindIDLen]byte, error) {
	var countBuf [2]byte
	if _, err := io.ReadFull(r, countBuf[:]); err != nil {
		return nil, fmt.Errorf("holder: read id count: %w", err)
	}
	n := binary.BigEndian.Uint16(countBuf[:])
	if n > MaxHasBatch {
		return nil, fmt.Errorf("holder: has-batch of %d exceeds %d", n, MaxHasBatch)
	}
	ids := make([][crypto.BlindIDLen]byte, n)
	for i := range ids {
		if _, err := io.ReadFull(r, ids[i][:]); err != nil {
			return nil, fmt.Errorf("holder: read id list: %w", err)
		}
	}
	return ids, nil
}

// MaxListPage bounds the blobs returned in one list page.
const MaxListPage = 4096

func writeListRequest(w io.Writer, folderID string, after [crypto.BlindIDLen]byte, limit uint16) error {
	if len(folderID) > MaxFolderIDLen {
		return errHolderIDTooLong
	}
	buf := make([]byte, 0, 8+len(folderID)+crypto.BlindIDLen+2)
	buf = binary.BigEndian.AppendUint32(buf, HolderMagic)
	buf = append(buf, HolderVersion, opList)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(folderID)))
	buf = append(buf, folderID...)
	buf = append(buf, after[:]...)
	buf = binary.BigEndian.AppendUint16(buf, limit)
	if _, err := w.Write(buf); err != nil {
		return fmt.Errorf("holder: write list request: %w", err)
	}
	return nil
}

func readListBody(r io.Reader) (after [crypto.BlindIDLen]byte, limit int, err error) {
	after, err = readBlinded(r)
	if err != nil {
		return after, 0, err
	}
	var lim [2]byte
	if _, err = io.ReadFull(r, lim[:]); err != nil {
		return after, 0, fmt.Errorf("holder: read list limit: %w", err)
	}
	limit = int(binary.BigEndian.Uint16(lim[:]))
	if limit == 0 || limit > MaxListPage {
		limit = MaxListPage
	}
	return after, limit, nil
}

func encodeBlobRefs(refs []BlobRef) []byte {
	buf := make([]byte, 0, 2+len(refs)*(crypto.BlindIDLen+8))
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(refs)))
	for _, ref := range refs {
		buf = append(buf, ref.ID[:]...)
		buf = binary.BigEndian.AppendUint64(buf, uint64(ref.ModMillis))
	}
	return buf
}

func decodeBlobRefs(payload []byte) ([]BlobRef, error) {
	if len(payload) < 2 {
		return nil, fmt.Errorf("holder: short list response")
	}
	n := int(binary.BigEndian.Uint16(payload[:2]))
	rest := payload[2:]
	const sz = crypto.BlindIDLen + 8
	if len(rest) < n*sz {
		return nil, fmt.Errorf("holder: truncated list response")
	}
	refs := make([]BlobRef, n)
	for i := range refs {
		copy(refs[i].ID[:], rest[i*sz:])
		refs[i].ModMillis = int64(binary.BigEndian.Uint64(rest[i*sz+crypto.BlindIDLen:]))
	}
	return refs, nil
}

// writeBitmapResponse answers a has-batch: one bit per queried id, set when present.
func writeBitmapResponse(w io.Writer, present []bool) error {
	bitmap := make([]byte, (len(present)+7)/8)
	for i, p := range present {
		if p {
			bitmap[i/8] |= 1 << (uint(i) % 8)
		}
	}
	head := make([]byte, 0, 12+len(bitmap))
	head = binary.BigEndian.AppendUint32(head, HolderMagic)
	head = append(head, HolderVersion, byte(StatusOK), 0, 0)
	head = binary.BigEndian.AppendUint32(head, uint32(len(bitmap)))
	head = append(head, bitmap...)
	if _, err := w.Write(head); err != nil {
		return fmt.Errorf("holder: write bitmap: %w", err)
	}
	return nil
}

func readBitmapResponse(r io.Reader, count int) ([]bool, error) {
	status, bitmap, err := readResponse(r)
	if err != nil {
		return nil, err
	}
	if status != StatusOK {
		return nil, fmt.Errorf("holder: has-batch failed with status %d", status)
	}
	if len(bitmap) < (count+7)/8 {
		return nil, fmt.Errorf("holder: short has-batch bitmap")
	}
	present := make([]bool, count)
	for i := range present {
		present[i] = bitmap[i/8]&(1<<(uint(i)%8)) != 0
	}
	return present, nil
}

func readPayload(r io.Reader) ([]byte, error) {
	n, err := payloadLen(r)
	if err != nil {
		return nil, err
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, fmt.Errorf("holder: read payload: %w", err)
	}
	return payload, nil
}

// drainPayload reads and discards a put payload without allocating it, so the server can
// reject an unauthorized put without holding its bytes or stalling the sender.
func drainPayload(r io.Reader) error {
	n, err := payloadLen(r)
	if err != nil {
		return err
	}
	_, err = io.CopyN(io.Discard, r, int64(n))
	return err
}

func payloadLen(r io.Reader) (uint32, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return 0, fmt.Errorf("holder: read payload length: %w", err)
	}
	n := binary.BigEndian.Uint32(lenBuf[:])
	if n > MaxBlobBytes {
		return 0, errBlobTooLarge
	}
	return n, nil
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
