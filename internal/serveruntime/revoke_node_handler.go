package serveruntime

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/gritive/GrainFS/internal/cluster"
)

type revokeNodeRequest struct {
	NodeID string `json:"node_id"`
}

// RevokeNodeHandler handles POST /v1/cluster/revoke-node.
type RevokeNodeHandler struct {
	RunRevoke func(context.Context, string) error
}

func (h *RevokeNodeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h == nil || h.RunRevoke == nil {
		writeRevokeNodeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "revoke-node unavailable"})
		return
	}
	var req revokeNodeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeRevokeNodeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON"})
		return
	}
	if req.NodeID == "" {
		writeRevokeNodeJSON(w, http.StatusBadRequest, map[string]string{"error": "node_id is required"})
		return
	}
	if err := h.RunRevoke(r.Context(), req.NodeID); err != nil {
		if errors.Is(err, cluster.ErrRevokeNodeNotFound) {
			writeRevokeNodeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
			return
		}
		writeRevokeNodeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeRevokeNodeJSON(w, http.StatusOK, map[string]string{"status": "ok", "message": "node revoked"})
}

func writeRevokeNodeJSON(w http.ResponseWriter, code int, body map[string]string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}
