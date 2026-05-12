package clusteradmin

import (
	"errors"
	"fmt"

	"github.com/gritive/GrainFS/internal/adminapi"
)

// RemovePeerError carries server-supplied structured fields for non-2xx
// responses to /v1/cluster/remove-peer (leader hint, quorum math). Envelope
// is the transport-level *adminapi.Error; LeaderID/VotersAfter/... are the
// typed view of envelope.Details.
type RemovePeerError struct {
	Envelope    *adminapi.Error
	Status      int
	Message     string
	LeaderID    string
	VotersAfter int
	AliveAfter  int
	NewQuorum   int
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
	out.LeaderID, _ = e.Details["leader_id"].(string)
	out.VotersAfter = intField(e.Details, "voters_after")
	out.AliveAfter = intField(e.Details, "alive_after")
	out.NewQuorum = intField(e.Details, "new_quorum")
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
	out.LeaderID, _ = e.Details["leader_id"].(string)
	if r, ok := e.Details["retry"].(bool); ok {
		out.Retry = r
	}
	return out
}

// asAdminError extracts a *adminapi.Error from an error chain.
func asAdminError(err error) (*adminapi.Error, bool) {
	var e *adminapi.Error
	return e, errors.As(err, &e)
}

func intField(m map[string]any, key string) int {
	v, ok := m[key]
	if !ok {
		return 0
	}
	switch x := v.(type) {
	case float64:
		return int(x)
	case int:
		return x
	}
	return 0
}
