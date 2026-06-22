package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/GhentiLabs/Trove/pkg/discovery"
)

// Stable, client-facing error codes. Messages are intentionally generic; the
// underlying detail is logged, never returned.
const (
	codeBadRequest   = "bad_request"
	codeUnauthorized = "unauthorized"
	codeNotFound     = "not_found"
	codeRateLimited  = "rate_limited"
	codeForbidden    = "forbidden"
	codePayloadLarge = "payload_too_large"
	codeUnavailable  = "unavailable"
	codeInternal     = "internal_error"
)

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError emits the standard error envelope and logs the real reason. The
// public message is generic; detail stays server-side.
func writeError(w http.ResponseWriter, log *slog.Logger, status int, code, publicMsg string, detail error) {
	if log != nil && detail != nil {
		log.Warn("request rejected", "code", code, "status", status, "detail", detail.Error())
	}
	writeJSON(w, status, discovery.Error{Code: code, Message: publicMsg})
}
