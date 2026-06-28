package holder

import (
	"context"
	"fmt"

	"github.com/GhentiLabs/Trove/client/internal/crypto"
	"github.com/GhentiLabs/Trove/client/internal/netio"
)

// GetBlobOverConn returns a GetBlob that fetches a folder's blobs from a holder over conn,
// one stream per blob.
func GetBlobOverConn(conn netio.Conn, folderID string) GetBlob {
	return func(ctx context.Context, blinded [crypto.BlindIDLen]byte) ([]byte, error) {
		stream, err := conn.OpenStream(ctx)
		if err != nil {
			return nil, fmt.Errorf("holder: open stream: %w", err)
		}
		defer func() { _ = stream.Close() }()
		if err := writeRequest(stream, opGet, folderID, blinded, nil); err != nil {
			return nil, err
		}
		status, payload, err := readResponse(stream)
		if err != nil {
			return nil, err
		}
		switch status {
		case StatusOK:
			return payload, nil
		case StatusNotFound:
			return nil, ErrNotFound
		default:
			return nil, fmt.Errorf("holder: get failed with status %d", status)
		}
	}
}

// HasBlobsOverConn returns a HasBlobs that asks a holder which of up to MaxHasBatch ids it
// already stores, in one stream.
func HasBlobsOverConn(conn netio.Conn, folderID string) HasBlobs {
	return func(ctx context.Context, ids [][crypto.BlindIDLen]byte) ([]bool, error) {
		if len(ids) == 0 {
			return nil, nil
		}
		stream, err := conn.OpenStream(ctx)
		if err != nil {
			return nil, fmt.Errorf("holder: open stream: %w", err)
		}
		defer func() { _ = stream.Close() }()
		if err := writeBlindedList(stream, opHasBatch, folderID, ids); err != nil {
			return nil, err
		}
		return readBitmapResponse(stream, len(ids))
	}
}

// PutBlobOverConn returns a PutBlob that stores a folder's blobs on a holder over conn,
// one stream per blob.
func PutBlobOverConn(conn netio.Conn, folderID string) PutBlob {
	return func(ctx context.Context, blinded [crypto.BlindIDLen]byte, data []byte) error {
		stream, err := conn.OpenStream(ctx)
		if err != nil {
			return fmt.Errorf("holder: open stream: %w", err)
		}
		defer func() { _ = stream.Close() }()
		if err := writeRequest(stream, opPut, folderID, blinded, data); err != nil {
			return err
		}
		status, _, err := readResponse(stream)
		if err != nil {
			return err
		}
		if status != StatusOK {
			return fmt.Errorf("holder: put failed with status %d", status)
		}
		return nil
	}
}
