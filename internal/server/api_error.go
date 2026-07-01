package server

// apiError builds the uniform {error,hint} JSON body used by the raft-snapshot
// admin endpoints in package server.
func apiError(msg, hint string) map[string]string {
	return map[string]string{"error": msg, "hint": hint}
}
