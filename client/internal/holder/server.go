package holder

import (
	"context"
	"errors"
	"log/slog"

	"github.com/GhentiLabs/Trove/client/internal/netio"
)

// Server answers blind blob requests over a peer connection from the per-folder stores
// this holder keeps. It holds no key and serves only opaque ciphertext.
type Server struct {
	stores   map[string]*Store
	allowPut func(folderID, peerID string) (bool, error)
	log      *slog.Logger
}

// NewServer builds a holder server over folderID->store. allowPut authorizes a peer to
// store blobs for a folder (a writer); nil allows any connected peer. Reads are open to
// any connected peer, since the session already authorized folder membership.
func NewServer(stores map[string]*Store, allowPut func(folderID, peerID string) (bool, error), log *slog.Logger) *Server {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Server{stores: stores, allowPut: allowPut, log: log}
}

// Serve accepts streams on conn and answers each as a blind get or put until ctx is
// cancelled or the connection closes.
func (s *Server) Serve(ctx context.Context, conn netio.Conn) {
	for {
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			return
		}
		go s.handle(stream, conn.PeerNodeID())
	}
}

func (s *Server) handle(stream netio.Stream, peerID string) {
	defer func() { _ = stream.Close() }()
	op, folderID, blinded, payload, err := readRequest(stream)
	if err != nil {
		s.log.Debug("holder: read request", "peer", peerID, "err", err)
		return
	}
	store := s.stores[folderID]
	if store == nil {
		_ = writeResponse(stream, StatusError, nil)
		return
	}
	switch op {
	case opGet:
		data, err := store.Get(blinded)
		switch {
		case errors.Is(err, ErrNotFound):
			_ = writeResponse(stream, StatusNotFound, nil)
		case err != nil:
			s.log.Debug("holder: get", "folder", folderID, "err", err)
			_ = writeResponse(stream, StatusError, nil)
		default:
			_ = writeResponse(stream, StatusOK, data)
		}
	case opPut:
		if !s.putAllowed(folderID, peerID) {
			s.log.Warn("holder: rejecting put from unauthorized peer", "folder", folderID, "peer", peerID)
			_ = writeResponse(stream, StatusError, nil)
			return
		}
		if err := store.Put(blinded, payload); err != nil {
			s.log.Debug("holder: put", "folder", folderID, "err", err)
			_ = writeResponse(stream, StatusError, nil)
			return
		}
		_ = writeResponse(stream, StatusOK, nil)
	}
}

func (s *Server) putAllowed(folderID, peerID string) bool {
	if s.allowPut == nil {
		return true
	}
	ok, err := s.allowPut(folderID, peerID)
	if err != nil {
		s.log.Warn("holder: put authorization", "folder", folderID, "peer", peerID, "err", err)
		return false
	}
	return ok
}
