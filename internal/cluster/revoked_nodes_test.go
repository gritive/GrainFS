package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetaFSM_RevokedNodes_RecordAndQuery(t *testing.T) {
	f := NewMetaFSM()
	require.False(t, f.IsRevoked("node-b"))
	require.Empty(t, f.RevokedNodeIDs())

	f.recordRevokedNodeForTest("node-b")

	require.True(t, f.IsRevoked("node-b"))
	got := f.RevokedNodeIDs()
	require.Equal(t, map[string]struct{}{"node-b": {}}, got)

	// Returned map is a defensive copy: mutating it must not affect the FSM.
	delete(got, "node-b")
	require.True(t, f.IsRevoked("node-b"))

	// Re-recording is idempotent.
	f.recordRevokedNodeForTest("node-b")
	require.Len(t, f.RevokedNodeIDs(), 1)
}
