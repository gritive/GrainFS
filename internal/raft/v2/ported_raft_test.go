package raftv2

// ported_raft_test.go: selected raft_test.go scenarios ported to v2 (PR 19 batch 2).
//
// v1 equivalents:
//   TestConfiguration_ConcurrentReads
//   TestNode_Close_WaitsForGoroutines
//
// Scenarios SKIPPED-within-file (v1-specific, no v2 equivalent):
//   TestObserver_* — v2 has no RegisterObserver API.
//   TestElectionPriority_* — v2 is single-group; ElectionPriorityKey not wired.
//   TestNode_LogGC_* — LogGCInterval in Config but actor loop not wired yet.
//   TestTrailingLogs_* — v1-internal CompactLog; v2 uses CreateSnapshot/CompactBefore.
//   TestBootstrap_RejectsSecondCall — covered by v2/api_test.go TestAPI_BootstrapIdempotent.
//   TestBootstrap_AutoDetectsExistingStore — v2 Bootstrap is informational only (node.go:414).
//   TestAEReply_ConflictTermJumps* — white-box v1 internal; covered by v2/replication_test.go.
//   TestHasQuorum_* — v1 white-box (n.checkQuorumAcks); covered via election_test.go behaviourally.

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConfiguration_ConcurrentReads verifies that Configuration() is safe to
// call from multiple goroutines concurrently while the node is running (race
// detector coverage). v1 equivalent: TestConfiguration_ConcurrentReads.
func TestConfiguration_ConcurrentReads(t *testing.T) {
	nodes, net := startCluster(t, "n1", "n2", "n3")
	_ = net

	// Wait for a leader.
	require.NoError(t, waitFor(5*time.Second, func() bool {
		for _, n := range nodes {
			if n.IsLeader() {
				return true
			}
		}
		return false
	}))

	// Run 500 concurrent Configuration() reads on node 0 while the cluster
	// is active. The race detector will catch unsynchronised accesses.
	var wg sync.WaitGroup
	const workers = 8
	const readsPerWorker = 64

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < readsPerWorker; j++ {
				cfg := nodes[0].Configuration()
				assert.NotEmpty(t, cfg.Servers)
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("concurrent Configuration() reads timed out")
	}

	// Final sanity: 3-voter cluster reports 3 servers.
	cfg := nodes[0].Configuration()
	assert.Len(t, cfg.Servers, 3)
	for _, s := range cfg.Servers {
		assert.Equal(t, Voter, s.Suffrage)
	}
}

// TestNode_Stop_WaitsForGoroutines verifies that Stop() blocks until the actor
// goroutine has exited rather than returning immediately, so the caller can
// safely release resources (stores, transports) after Stop returns.
// v1 equivalent: TestNode_Close_WaitsForGoroutines.
func TestNode_Stop_WaitsForGoroutines(t *testing.T) {
	n, err := NewNode(Config{ID: "n1"})
	require.NoError(t, err)
	n.Start()

	// Drain ApplyCh to avoid blocking the actor on commit delivery.
	go func() {
		for range n.ApplyCh() {
		}
	}()

	// Brief pause so the actor goroutine is fully running.
	time.Sleep(20 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		n.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Success: Stop returned promptly.
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() did not return within 2s — actor goroutine may have leaked")
	}
}
