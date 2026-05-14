// internal/audit/emitter_test.go
package audit_test

import (
	"testing"

	"github.com/gritive/GrainFS/internal/audit"
	"github.com/stretchr/testify/require"
)

func TestEmitterRecursionGuard_AuditBucket(t *testing.T) {
	e := audit.NewEmitter("node-1")
	e.EmitS3(audit.S3Event{Bucket: audit.BucketName, Method: "PUT", Status: 200})
	buf := make([]audit.S3Event, 10)
	got := e.Ring().DrainInto(buf)
	require.Empty(t, got, "grainfs-audit bucket write must not produce ring entry")
}

func TestEmitterRecursionGuard_SystemSA(t *testing.T) {
	e := audit.NewEmitter("node-1")
	e.EmitS3(audit.S3Event{SAID: audit.SystemSA, Bucket: "data", Method: "PUT", Status: 200})
	buf := make([]audit.S3Event, 10)
	got := e.Ring().DrainInto(buf)
	require.Empty(t, got, "system:audit SA must not produce ring entry")
}

func TestEmitterNormalEvent(t *testing.T) {
	e := audit.NewEmitter("node-1")
	e.EmitS3(audit.S3Event{Bucket: "mybucket", Method: "PUT", Key: "obj", Status: 200})
	buf := make([]audit.S3Event, 10)
	got := e.Ring().DrainInto(buf)
	require.Len(t, got, 1)
	require.Equal(t, "PUT", got[0].Method)
	require.Equal(t, "node-1", got[0].NodeID)
}
