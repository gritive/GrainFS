package cluster

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// notifyMover is a ShardMover that reads from a fixed in-memory shard slice
// and tracks delete calls.
type notifyMover struct {
	shards   [][]byte
	written  [][]byte
	onDelete func(peer string)
}

func (m *notifyMover) ReadShard(_ context.Context, _ string, _, _ string, idx int) ([]byte, error) {
	return m.shards[idx], nil
}

func (m *notifyMover) WriteShard(_ context.Context, _ string, _, _ string, idx int, data []byte) error {
	if m.written == nil {
		m.written = make([][]byte, len(m.shards))
	}
	m.written[idx] = data
	return nil
}

func (m *notifyMover) DeleteShards(_ context.Context, peer, _, _ string) error {
	if m.onDelete != nil {
		m.onDelete(peer)
	}
	return nil
}

// notifyLoopRaft implements MigrationRaft and calls onPropose after each Propose.
type notifyLoopRaft struct {
	onPropose func()
}

func (r *notifyLoopRaft) Propose(data []byte) error {
	if r.onPropose != nil {
		r.onPropose()
	}
	return nil
}

func (r *notifyLoopRaft) NodeID() string { return "test-node" }

var _ = Describe("Balancer integration", func() {
	It("proposes migration on imbalance", func() {
		store := NewNodeStatsStore(1 * time.Minute)
		store.Set(NodeStats{NodeID: "leader", DiskUsedPct: 80.0, DiskAvailBytes: 10 << 30})
		store.Set(NodeStats{NodeID: "peer-a", DiskUsedPct: 30.0, DiskAvailBytes: 200 << 30})

		node := &mockRaftNode{
			state:   2,
			nodeID:  "leader",
			peerIDs: []string{"peer-a"},
		}

		cfg := defaultFakeBalancerCfg()
		cfg.warmupTimeout = 1 * time.Millisecond
		cfg.migrationRate = 1

		p := NewBalancerProposer("leader", store, node, cfg)
		p.SetObjectPicker(&mockObjectPicker{bucket: "b", key: "k", ok: true})

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		DeferCleanup(cancel)
		go p.Run(ctx)

		Eventually(func() int {
			return node.ProposedLen()
		}, 400*time.Millisecond, 10*time.Millisecond).Should(BeNumerically(">", 0), "timeout: no CmdMigrateShard proposed within 400ms")

		cmd, err := DecodeCommand(node.ProposedAt(0))
		Expect(err).NotTo(HaveOccurred())
		Expect(cmd.Type).To(Equal(CmdMigrateShard), "should propose CmdMigrateShard")

		migrate, err := decodeMigrateShardCmd(cmd.Data)
		Expect(err).NotTo(HaveOccurred())
		Expect(migrate.SrcNode).To(Equal("leader"))
		Expect(migrate.DstNode).To(Equal("peer-a"))
	})

	It("deletes source shards after migration commit", func() {
		var shards [4][]byte
		for i := range shards {
			shards[i] = []byte{byte(i)}
		}

		var deletedSrc string
		mover := &notifyMover{
			shards:   shards[:],
			onDelete: func(peer string) { deletedSrc = peer },
		}

		var exec *MigrationExecutor
		node := &notifyLoopRaft{onPropose: func() {
			exec.NotifyCommit("bucket", "key", "v1")
		}}

		exec = NewMigrationExecutor(mover, node, len(shards))
		DeferCleanup(exec.Stop)

		task := MigrationTask{
			Bucket:    "bucket",
			Key:       "key",
			VersionID: "v1",
			SrcNode:   "src-node",
			DstNode:   "dst-node",
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		DeferCleanup(cancel)

		Expect(exec.Execute(ctx, task)).To(Succeed())
		Expect(deletedSrc).To(Equal("src-node"), "src shards should be deleted after commit")
	})

	It("triggers migration from disk collector stats", func() {
		store := NewNodeStatsStore(1 * time.Minute)

		store.Set(NodeStats{NodeID: "leader", DiskUsedPct: 0.0})
		store.Set(NodeStats{NodeID: "peer-a", DiskUsedPct: 20.0, DiskAvailBytes: 200 << 30})

		node := &mockRaftNode{
			state:   2,
			nodeID:  "leader",
			peerIDs: []string{"peer-a"},
		}

		cfg := defaultFakeBalancerCfg()
		cfg.warmupTimeout = 1 * time.Millisecond
		cfg.migrationRate = 1

		p := NewBalancerProposer("leader", store, node, cfg)
		p.SetObjectPicker(&mockObjectPicker{bucket: "b", key: "k", ok: true})

		collector := NewDiskCollector("leader", "/tmp", store, 10*time.Millisecond, nil)
		collector.SetStatFunc(func(string) (float64, uint64) { return 80.0, 1 << 30 })

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		DeferCleanup(cancel)

		go collector.Run(ctx)
		go p.Run(ctx)

		Eventually(func() int {
			return node.ProposedLen()
		}, 1*time.Second, 10*time.Millisecond).Should(BeNumerically(">", 0), "timeout: no CmdMigrateShard proposed within 1s")

		cmd, err := DecodeCommand(node.ProposedAt(0))
		Expect(err).NotTo(HaveOccurred())
		Expect(cmd.Type).To(Equal(CmdMigrateShard))
	})
})
