package server

import (
	"errors"

	"github.com/gritive/GrainFS/internal/snapshot"
)

// apiError builds the uniform {error,hint} JSON body shared by the PITR and
// raft-snapshot admin endpoints (and the snapshot endpoints, which carry their
// own copy in internal/server/snapshotsvc). Kept in core because the PITR and
// raft-snapshot handlers remain in package server.
func apiError(msg, hint string) map[string]string {
	return map[string]string{"error": msg, "hint": hint}
}

// errSnapshotUnavailable mirrors the snapshotsvc sentinel; callers surface it via
// err.Error() only (no cross-package identity comparison), so an independent
// sentinel with the same message is behavior-neutral.
var errSnapshotUnavailable = errors.New("backend does not support snapshots")

// snapshotManager returns the configured snapshot Manager, or
// errSnapshotUnavailable when the snapshot feature is unavailable. Used by the
// PITR and raft-snapshot endpoints that remain in package server.
func (s *Server) snapshotManager() (*snapshot.Manager, error) {
	if !s.routeFeatureAvailable(routeFeatureSnapshot) {
		return nil, errSnapshotUnavailable
	}
	return s.snapMgr, nil
}
