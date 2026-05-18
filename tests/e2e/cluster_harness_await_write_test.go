package e2e

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestAwaitWriteFromNonOwnerProbe verifies that AwaitWriteFromNonOwner
// returns nil once a write against the cluster succeeds from at least
// one non-leader peer. Uses __grainfs_probe internal namespace so the
// probe doesn't leak into user-visible bucket lists.
func TestAwaitWriteFromNonOwnerProbe(t *testing.T) {
	skipIfShort(t, "cluster boot")
	c := startE2ECluster(t, e2eClusterOptions{
		Nodes: 3, Mode: ClusterModeDynamicJoin,
		ClusterKey: "E2E-AWAIT", LogPrefix: "await-probe",
		DisableNFS: true, DisableNBD: true,
	})
	require.NoError(t, c.AwaitWriteFromNonOwner("__grainfs_probe", "leader-check", 15*time.Second))
}
