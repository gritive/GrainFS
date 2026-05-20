// internal/server/handlers_audit_test.go
package server_test

import (
	"testing"

	"github.com/gritive/GrainFS/internal/audit"
	"github.com/gritive/GrainFS/internal/server"
)

// TestWithAuditEmitterOptionExists confirms WithAuditEmitter server option is exposed.
// Fails to compile until Step 2 adds it to server.go.
func TestWithAuditEmitterOptionExists(t *testing.T) {
	e := audit.NewEmitterWithRingCapacity("test-node", 1)
	_ = server.WithAuditEmitter(e)
}
