package clusteradmin

import "fmt"

// TransferLeaderErrorDetails mirrors the structured "details" object the
// server emits for /v1/cluster/transfer-leader non-2xx responses.
type TransferLeaderErrorDetails struct {
	LeaderID string `json:"leader_id,omitempty"`
	Retry    bool   `json:"retry,omitempty"`
}

// TransferLeaderError carries structured fields the server returns for
// non-2xx responses to /v1/cluster/transfer-leader so the CLI can render
// contextual messages (leader hint) and orchestration code can branch on
// Retry (true on race semantics — raft.ErrNotLeader during transfer).
type TransferLeaderError struct {
	StatusCode int
	Message    string
	TransferLeaderErrorDetails
}

func (e *TransferLeaderError) Error() string {
	if e.LeaderID != "" {
		return fmt.Sprintf("server: %s (leader=%s)", e.Message, e.LeaderID)
	}
	return "server: " + e.Message
}
