package raft

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
	"errors"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Ported raft scenarios", func() {
	ginkgo.It("allows concurrent Configuration reads while the node is running", func(ginkgo.SpecContext) {
		nodes, _, cleanup, err := startRaftIntegrationCluster("n1", "n2", "n3")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(cleanup)

		gomega.Expect(waitFor(5*time.Second, func() bool {
			for _, node := range nodes {
				if node.IsLeader() {
					return true
				}
			}
			return false
		})).To(gomega.Succeed())

		var wg sync.WaitGroup
		const workers = 8
		const readsPerWorker = 64
		errCh := make(chan error, workers*readsPerWorker)

		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < readsPerWorker; j++ {
					cfg := nodes[0].Configuration()
					if len(cfg.Servers) == 0 {
						errCh <- errors.New("Configuration returned no servers")
					}
				}
			}()
		}

		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
			close(errCh)
		}()

		select {
		case <-done:
		case <-time.After(3 * time.Second):
			ginkgo.Fail("concurrent Configuration() reads timed out")
		}
		for err := range errCh {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		cfg := nodes[0].Configuration()
		gomega.Expect(cfg.Servers).To(gomega.HaveLen(3))
		for _, server := range cfg.Servers {
			gomega.Expect(server.Suffrage).To(gomega.Equal(Voter))
		}
	}, ginkgo.NodeTimeout(10*time.Second))

	ginkgo.It("waits for goroutines when stopping a node", func(ginkgo.SpecContext) {
		node, err := NewNode(Config{ID: "n1"})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		node.Start()

		go func() {
			for range node.ApplyCh() {
			}
		}()

		time.Sleep(20 * time.Millisecond)

		done := make(chan struct{})
		go func() {
			node.Stop()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			ginkgo.Fail("Stop() did not return within 2s; actor goroutine may have leaked")
		}
	}, ginkgo.NodeTimeout(5*time.Second))
})
