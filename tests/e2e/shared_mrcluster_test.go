package e2e

import (
	"sync"
	"testing"
)

var (
	sharedMRClusterOnce sync.Once
	sharedMRCluster     *mrCluster
)

// getOrInitSharedMRCluster boots a 3-node static-peers mrCluster shared by
// iceberg + NFS Target factories. NFS enabled (for nfsTarget reuse in later
// tasks), NBD disabled, fast bootstrap.
//
// TODO(test-reorg): newMRCluster registers t.Cleanup(c.Stop) against the
// FIRST caller's *testing.T. When that test exits, the shared cluster is
// stopped — subsequent callers will get a dead cluster via sync.Once. Hook
// teardown through TestMain (stopSharedMRCluster) before relying on this
// across multiple tests. Today this helper is unused by the matrix tests.
func getOrInitSharedMRCluster(t *testing.T) *mrCluster {
	t.Helper()
	sharedMRClusterOnce.Do(func() {
		c := startStaticMRClusterWithOptions(t, 3, mrClusterOptions{
			disableNBD:    true,
			FastBootstrap: true,
			// disableNFS4 = false (NFS stays enabled for nfsTarget reuse)
		})
		sharedMRCluster = c
	})
	return sharedMRCluster
}

// stopSharedMRCluster is intended to be invoked from TestMain teardown once
// the lifecycle TODO above is resolved.
func stopSharedMRCluster() {
	if sharedMRCluster != nil {
		sharedMRCluster.Stop()
	}
}
