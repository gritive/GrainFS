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

func TestMetaFSM_ApplyRevokePeer_RecordsRevokedNodeAndFiresCallback(t *testing.T) {
	f := NewMetaFSM()
	require.NoError(t, f.peers.registerMember("node-b", spki(0x01), "10.0.0.2:7000", true, 0))

	var fired string
	f.SetOnNodeRevoked(func(nodeID string) { fired = nodeID })

	data, err := encodeRevokePeerCmd("node-b")
	require.NoError(t, err)
	require.NoError(t, f.applyRevokePeer(data))

	require.True(t, f.IsRevoked("node-b"))
	require.Equal(t, "node-b", fired)

	// Idempotent replay: applying again is a no-op that still leaves it revoked
	// and does not panic when the registry entry is already gone.
	require.NoError(t, f.applyRevokePeer(data))
	require.True(t, f.IsRevoked("node-b"))
}
