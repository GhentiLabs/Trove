package holder

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/GhentiLabs/Trove/client/internal/netio"
)

// maxConcurrentRequests bounds the holder's in-flight request handlers per connection,
// shedding excess streams so a peer cannot exhaust memory by opening many at once.
const maxConcurrentRequests = 64

// Server answers blind blob requests over a peer connection from the per-folder stores
// this holder keeps.
type Server struct {
	stores   map[string]*Store
	allowPut func(ctx context.Context, folderID, peerID string) (bool, error)
	log      *slog.Logger
	sem      chan struct{}
	wg       sync.WaitGroup
}

// NewServer builds a holder server over folderID->store. allowPut authorizes a peer to
// store blobs for a folder; a nil allowPut rejects every put (fail closed).
func NewServer(stores map[string]*Store, allowPut func(ctx context.Context, folderID, peerID string) (bool, error), log *slog.Logger) *Server {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Server{stores: stores, allowPut: allowPut, log: log, sem: make(chan struct{}, maxConcurrentRequests)}
}

// Serve answers blind blob requests on conn until ctx is cancelled or the connection
// closes, then waits for in-flight requests to finish.
func (s *Server) Serve(ctx context.Context, conn netio.Conn) {
	for {
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			s.wg.Wait()
			return
		}
		select {
		case s.sem <- struct{}{}:
		default:
			s.log.Warn("holder: shedding request, too many in flight", "peer", conn.PeerNodeID())
			_ = stream.Close()
			continue
		}
		s.wg.Go(func() {
			defer func() { <-s.sem }()
			s.handle(ctx, stream, conn.PeerNodeID())
		})
	}
}

func (s *Server) handle(ctx context.Context, stream netio.Stream, peerID string) {
	defer func() { _ = stream.Close() }()
	op, folderID, blinded, err := readRequestHeader(stream)
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
		if !s.putAllowed(ctx, folderID, peerID) {
			s.log.Warn("holder: rejecting put from unauthorized peer", "folder", folderID, "peer", peerID)
			_ = drainPayload(stream)
			_ = writeResponse(stream, StatusError, nil)
			return
		}
		payload, err := readPayload(stream)
		if err != nil {
			s.log.Debug("holder: read payload", "folder", folderID, "err", err)
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

func (s *Server) putAllowed(ctx context.Context, folderID, peerID string) bool {
	if s.allowPut == nil {
		s.log.Error("holder: no put authorization configured, rejecting", "peer", peerID)
		return false
	}
	ok, err := s.allowPut(ctx, folderID, peerID)
	if err != nil {
		s.log.Warn("holder: put authorization", "folder", folderID, "peer", peerID, "err", err)
		return false
	}
	return ok
}
