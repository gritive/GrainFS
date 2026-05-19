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
// iceberg + NFS Target factories. NFS enabled (for nfsTarget reuse), NBD
// disabled, fast bootstrap.
//
// Lifecycle: newMRCluster (called by startStaticMRClusterWithOptions) registers
// t.Cleanup(c.Stop) against the FIRST caller's *testing.T. If we let that
// fire, subsequent callers via sync.Once get a dead cluster. We disarm by
// pre-setting c.stopped=true so the test-scoped Stop is a no-op; the real
// shutdown happens in stopSharedMRCluster (TestMain teardown).
//
// Also: startStaticMRClusterWithOptions leaves c.nodeCount=0 (see comment at
// multiraft_sharding_test.go:433 — "static clusters set all nodes before
// setting nodeCount"). Matrix Target factories (nfsTarget, icebergTarget)
// use c.nodeCount for index modulo, so we set it explicitly here.
func getOrInitSharedMRCluster(t *testing.T) *mrCluster {
	t.Helper()
	sharedMRClusterOnce.Do(func() {
		c := startStaticMRClusterWithOptions(t, 3, mrClusterOptions{
			disableNBD:    true,
			FastBootstrap: true,
		})
		c.nodeCount = 3
		c.stopped = true // disarm first-caller t.Cleanup; TestMain does real stop
		sharedMRCluster = c
	})
	return sharedMRCluster
}

// stopSharedMRCluster is invoked from TestMain teardown. Re-arms the
// disarmed Stop() set up in getOrInitSharedMRCluster.
func stopSharedMRCluster() {
	if sharedMRCluster != nil {
		sharedMRCluster.stopped = false
		sharedMRCluster.Stop()
	}
}
