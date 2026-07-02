package control

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
)

// Stable, client-facing error codes. Messages carry the domain failure; there is
// no untrusted caller to hide detail from on a permission-gated socket.
const (
	codeBadRequest = "bad_request"
	codeNotFound   = "not_found"
	codeConflict   = "conflict"
	codeInternal   = "internal_error"
)

// Server serves the control API for one Backend.
type Server struct {
	backend Backend
	log     *slog.Logger
}

// New builds a Server; a nil logger discards.
func New(b Backend, log *slog.Logger) *Server {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Server{backend: b, log: log}
}

// Handler is the routed, middleware-wrapped API surface.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/identity", s.handleIdentity)
	mux.HandleFunc("GET /v1/status", s.handleStatus)
	mux.HandleFunc("GET /v1/peers", s.handlePeers)
	mux.HandleFunc("POST /v1/folders", s.handleFound)
	mux.HandleFunc("POST /v1/folders/join", s.handleJoin)
	mux.HandleFunc("POST /v1/folders/{id}/invite", s.handleInvite)
	mux.HandleFunc("PATCH /v1/folders/{id}", s.handleQuota)
	mux.HandleFunc("DELETE /v1/folders/{id}", s.handleRemove)
	return chain(mux, logRequests(s.log), recoverPanic(s.log))
}

func (s *Server) handleIdentity(w http.ResponseWriter, r *http.Request) {
	id, err := s.backend.Identity(r.Context())
	if err != nil {
		s.writeBackendError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, id)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	st, err := s.backend.Status(r.Context())
	if err != nil {
		s.writeBackendError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func (s *Server) handlePeers(w http.ResponseWriter, r *http.Request) {
	p, err := s.backend.Peers(r.Context())
	if err != nil {
		s.writeBackendError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, p)
}

func (s *Server) handleFound(w http.ResponseWriter, r *http.Request) {
	var req FoundRequest
	if !s.decode(w, r, &req) {
		return
	}
	resp, err := s.backend.Found(r.Context(), req)
	if err != nil {
		s.writeBackendError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, resp)
}

func (s *Server) handleJoin(w http.ResponseWriter, r *http.Request) {
	var req JoinRequest
	if !s.decode(w, r, &req) {
		return
	}
	if err := s.backend.Join(r.Context(), req); err != nil {
		s.writeBackendError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, struct{}{})
}

func (s *Server) handleInvite(w http.ResponseWriter, r *http.Request) {
	var req InviteRequest
	if !s.decode(w, r, &req) {
		return
	}
	if err := s.backend.Invite(r.Context(), r.PathValue("id"), req); err != nil {
		s.writeBackendError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, struct{}{})
}

func (s *Server) handleQuota(w http.ResponseWriter, r *http.Request) {
	var req QuotaRequest
	if !s.decode(w, r, &req) {
		return
	}
	f, err := s.backend.SetQuota(r.Context(), r.PathValue("id"), req.QuotaBytes)
	if err != nil {
		s.writeBackendError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, f)
}

func (s *Server) handleRemove(w http.ResponseWriter, r *http.Request) {
	purge := r.URL.Query().Get("purge") == "true"
	if err := s.backend.Remove(r.Context(), r.PathValue("id"), purge); err != nil {
		s.writeBackendError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) decode(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		writeError(w, s.log, http.StatusBadRequest, codeBadRequest, "malformed request body")
		return false
	}
	return true
}

func (s *Server) writeBackendError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrInvalid):
		writeError(w, s.log, http.StatusBadRequest, codeBadRequest, err.Error())
	case errors.Is(err, ErrNotFound):
		writeError(w, s.log, http.StatusNotFound, codeNotFound, err.Error())
	case errors.Is(err, ErrExists):
		writeError(w, s.log, http.StatusConflict, codeConflict, err.Error())
	default:
		writeError(w, s.log, http.StatusInternalServerError, codeInternal, err.Error())
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, log *slog.Logger, status int, code, msg string) {
	log.Warn("control: request rejected", "code", code, "status", status, "detail", msg)
	writeJSON(w, status, Error{Code: code, Message: msg})
}
