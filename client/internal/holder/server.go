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
	stores     map[string]*Store
	allowWrite func(ctx context.Context, folderID, peerID string) (bool, error)
	log        *slog.Logger
	sem        chan struct{}
	wg         sync.WaitGroup
}

// NewServer builds a holder server over folderID->store. allowWrite authorizes a peer to
// store blobs for a folder; a nil allowWrite rejects every put (fail closed).
func NewServer(stores map[string]*Store, allowWrite func(ctx context.Context, folderID, peerID string) (bool, error), log *slog.Logger) *Server {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Server{stores: stores, allowWrite: allowWrite, log: log, sem: make(chan struct{}, maxConcurrentRequests)}
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
	op, folderID, err := readRequestHeader(stream)
	if err != nil {
		s.log.Debug("holder: read request", "peer", peerID, "err", err)
		return
	}
	// Each handler reads the full request body before replying, so a synchronous transport
	// never deadlocks with the sender mid-write on an early-error path.
	switch op {
	case opGet:
		s.serveGet(stream, s.stores[folderID], folderID)
	case opHasBatch:
		s.serveHasBatch(stream, s.stores[folderID], folderID)
	case opList:
		s.serveList(stream, s.stores[folderID], folderID)
	case opPut:
		s.servePut(ctx, stream, s.stores[folderID], folderID, peerID)
	case opDelete:
		s.serveDelete(ctx, stream, s.stores[folderID], folderID, peerID)
	}
}

func (s *Server) serveList(stream netio.Stream, store *Store, folderID string) {
	after, limit, err := readListBody(stream)
	if err != nil {
		s.log.Debug("holder: read list", "folder", folderID, "err", err)
		return
	}
	if store == nil {
		_ = writeResponse(stream, StatusError, nil)
		return
	}
	refs, err := store.List(after, limit)
	if err != nil {
		s.log.Debug("holder: list", "folder", folderID, "err", err)
		_ = writeResponse(stream, StatusError, nil)
		return
	}
	_ = writeResponse(stream, StatusOK, encodeBlobRefs(refs))
}

func (s *Server) serveDelete(ctx context.Context, stream netio.Stream, store *Store, folderID, peerID string) {
	ids, err := readBlindedList(stream)
	if err != nil {
		s.log.Debug("holder: read delete", "folder", folderID, "err", err)
		return
	}
	if store == nil || !s.writeAllowed(ctx, folderID, peerID) {
		s.log.Warn("holder: rejecting delete", "folder", folderID, "peer", peerID)
		_ = writeResponse(stream, StatusError, nil)
		return
	}
	for _, id := range ids {
		if err := store.Delete(id); err != nil {
			s.log.Debug("holder: delete", "folder", folderID, "err", err)
			_ = writeResponse(stream, StatusError, nil)
			return
		}
	}
	_ = writeResponse(stream, StatusOK, nil)
}

func (s *Server) serveGet(stream netio.Stream, store *Store, folderID string) {
	blinded, err := readBlinded(stream)
	if err != nil {
		return
	}
	if store == nil {
		_ = writeResponse(stream, StatusError, nil)
		return
	}
	switch data, err := store.Get(blinded); {
	case errors.Is(err, ErrNotFound):
		_ = writeResponse(stream, StatusNotFound, nil)
	case err != nil:
		s.log.Debug("holder: get", "folder", folderID, "err", err)
		_ = writeResponse(stream, StatusError, nil)
	default:
		_ = writeResponse(stream, StatusOK, data)
	}
}

func (s *Server) serveHasBatch(stream netio.Stream, store *Store, folderID string) {
	ids, err := readBlindedList(stream)
	if err != nil {
		s.log.Debug("holder: read has-batch", "folder", folderID, "err", err)
		return
	}
	present := make([]bool, len(ids))
	for i, id := range ids {
		present[i] = store != nil && store.Has(id)
	}
	_ = writeBitmapResponse(stream, present)
}

func (s *Server) servePut(ctx context.Context, stream netio.Stream, store *Store, folderID, peerID string) {
	blinded, err := readBlinded(stream)
	if err != nil {
		return
	}
	if store == nil || !s.writeAllowed(ctx, folderID, peerID) {
		s.log.Warn("holder: rejecting put", "folder", folderID, "peer", peerID)
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

func (s *Server) writeAllowed(ctx context.Context, folderID, peerID string) bool {
	if s.allowWrite == nil {
		s.log.Error("holder: no put authorization configured, rejecting", "peer", peerID)
		return false
	}
	ok, err := s.allowWrite(ctx, folderID, peerID)
	if err != nil {
		s.log.Warn("holder: put authorization", "folder", folderID, "peer", peerID, "err", err)
		return false
	}
	return ok
}
