package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetaFSM_ResolveNodeAddress(t *testing.T) {
	f := NewMetaFSM()
	require.NoError(t, f.applyCmd(makeAddNodeCmd(t, "node-1", "10.0.0.1:7001", 0)))

	addr, ok := f.ResolveNodeAddress("node-1")
	require.True(t, ok)
	require.Equal(t, "10.0.0.1:7001", addr)
}

func TestResolveNodeAddress_AllowsLegacyAddressPeerIDs(t *testing.T) {
	f := NewMetaFSM()
	require.NoError(t, f.applyCmd(makeAddNodeCmd(t, "node-1", "10.0.0.1:7001", 0)))

	addr, ok := ResolveNodeAddress(f, "10.0.0.1:7001")
	require.True(t, ok)
	require.Equal(t, "10.0.0.1:7001", addr)
}

func TestResolveNodeAddresses_MissingNodeIDIsExplicit(t *testing.T) {
	f := NewMetaFSM()
	require.NoError(t, f.applyCmd(makeAddNodeCmd(t, "node-1", "10.0.0.1:7001", 0)))

	_, err := ResolveNodeAddresses(f, []string{"node-1", "node-missing"})
	require.ErrorContains(t, err, `node "node-missing" not found in address book`)
}
