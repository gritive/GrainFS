package server

// apiError builds the uniform {error,hint} JSON body shared by the
// raft-snapshot admin endpoints (and the snapshot endpoints, which carry their
// own copy in internal/server/snapshotsvc). Kept in core because the
// raft-snapshot handlers remain in package server.
func apiError(msg, hint string) map[string]string {
	return map[string]string{"error": msg, "hint": hint}
}
