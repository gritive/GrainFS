package clusteradmin

import "fmt"

// TransferLeaderError carries structured fields the server returns for
// non-2xx responses to /v1/cluster/transfer-leader so the CLI can render
// contextual messages (leader hint) and orchestration code can branch on
// Retry (true on race semantics — raft.ErrNotLeader during transfer).
type TransferLeaderError struct {
	StatusCode int
	Message    string
	LeaderID   string
	Retry      bool
}

func (e *TransferLeaderError) Error() string {
	if e.LeaderID != "" {
		return fmt.Sprintf("server: %s (leader=%s)", e.Message, e.LeaderID)
	}
	return "server: " + e.Message
}
