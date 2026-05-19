package e2e

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestClusterAwaitWriteFromNonOwnerE2E verifies that AwaitWriteFromNonOwner
// returns nil once a write against the cluster succeeds from at least one
// non-leader peer. Cluster-only by nature: requires ≥2 peers. Runs on the
// shared 4-node cluster fixture; no single-node analogue.
func TestClusterAwaitWriteFromNonOwnerE2E(t *testing.T) {
	t.Run("Cluster4Node", func(t *testing.T) {
		runAwaitWriteFromNonOwnerCases(t, newSharedClusterS3Target(t))
	})
}

func runAwaitWriteFromNonOwnerCases(t *testing.T, tgt s3Target) {
	t.Helper()
	require.True(t, tgt.isCluster, "AwaitWriteFromNonOwner requires cluster fixture")
	bucket := tgt.uniqueBucket(t, "awaitprobe")
	require.NoError(t, tgt.cluster.AwaitWriteFromNonOwner(bucket, "leader-check", 15*time.Second))
}
