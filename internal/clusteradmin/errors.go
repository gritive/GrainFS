package clusteradmin

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/gritive/GrainFS/internal/adminapi"
)

// RemovePeerErrorDetails mirrors the JSON shape of the structured "details"
// object the server emits for /v1/cluster/remove-peer non-2xx responses (or
// the top-level fields on a legacy flat-shape body — json.Unmarshal ignores
// unknown keys like "code"/"error" so the same struct works for both).
type RemovePeerErrorDetails struct {
	LeaderID    string `json:"leader_id,omitempty"`
	VotersAfter int    `json:"voters_after,omitempty"`
	AliveAfter  int    `json:"alive_after,omitempty"`
	NewQuorum   int    `json:"new_quorum,omitempty"`
}

// RemovePeerError carries server-supplied structured fields for non-2xx
// responses to /v1/cluster/remove-peer (leader hint, quorum math). Envelope
// is the transport-level *adminapi.Error; the embedded
// RemovePeerErrorDetails is the typed view of envelope.Details.
type RemovePeerError struct {
	Envelope *adminapi.Error
	Status   int
	Message  string
	RemovePeerErrorDetails
}

// Error renders a cluster-specific message format with leader hint.
func (e *RemovePeerError) Error() string {
	if e.LeaderID != "" {
		return fmt.Sprintf("server: %s (leader=%s)", e.Message, e.LeaderID)
	}
	return "server: " + e.Message
}

// Unwrap exposes the envelope so errors.As(err, &*adminapi.Error) finds it
// from a *RemovePeerError chain.
func (e *RemovePeerError) Unwrap() error { return e.Envelope }

// parseRemovePeerError lifts adminapi.Error.Details into the typed wrapper.
// Returns nil if e is nil.
func parseRemovePeerError(e *adminapi.Error) *RemovePeerError {
	if e == nil {
		return nil
	}
	out := &RemovePeerError{
		Envelope: e,
		Status:   e.Status,
		Message:  e.Message,
	}
	_ = json.Unmarshal(e.Details, &out.RemovePeerErrorDetails)
	return out
}

// parseTransferLeaderError lifts adminapi.Error.Details into TransferLeaderError.
func parseTransferLeaderError(e *adminapi.Error) *TransferLeaderError {
	if e == nil {
		return nil
	}
	out := &TransferLeaderError{
		StatusCode: e.Status,
		Message:    e.Message,
	}
	_ = json.Unmarshal(e.Details, &out.TransferLeaderErrorDetails)
	return out
}

// asAdminError extracts a *adminapi.Error from an error chain.
func asAdminError(err error) (*adminapi.Error, bool) {
	var e *adminapi.Error
	return e, errors.As(err, &e)
}
