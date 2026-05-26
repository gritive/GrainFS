package cluster

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Scrubber wiring integration", func() {
	var b *DistributedBackend

	BeforeEach(func() {
		b = newTestDistributedBackend(GinkgoT())
	})

	It("returns the configured self address as NodeID", func() {
		Expect(b.NodeID()).To(BeEmpty())

		b.SetShardService(nil, []string{"192.168.1.1:9000", "192.168.1.2:9000"})
		Expect(b.NodeID()).To(Equal("192.168.1.1:9000"))
	})

	It("returns the Raft node ID", func() {
		Expect(b.RaftNodeID()).To(Equal("test-node"))
	})

	It("reports owned shards from placement metadata", func() {
		const testVersionID = "any-version"

		cases := []struct {
			name     string
			bucket   string
			key      string
			nodes    []string
			ecData   uint8
			ecParity uint8
			nodeID   string
			want     []int
		}{
			{
				name:   "no placement record yields nil",
				bucket: "b",
				key:    "none",
				nodeID: "test-node",
				want:   nil,
			},
			{
				name:     "node owns zero shards",
				bucket:   "b",
				key:      "zero",
				nodes:    []string{"other-a", "other-b", "other-c"},
				ecData:   2,
				ecParity: 1,
				nodeID:   "test-node",
				want:     nil,
			},
			{
				name:     "node owns exactly one shard",
				bucket:   "b",
				key:      "one",
				nodes:    []string{"other-a", "test-node", "other-b"},
				ecData:   2,
				ecParity: 1,
				nodeID:   "test-node",
				want:     []int{1},
			},
			{
				name:     "node owns multiple non-contiguous shards",
				bucket:   "b",
				key:      "many",
				nodes:    []string{"test-node", "other-a", "test-node", "other-b", "test-node"},
				ecData:   3,
				ecParity: 2,
				nodeID:   "test-node",
				want:     []int{0, 2, 4},
			},
			{
				name:   "unknown node gets nil",
				bucket: "b",
				key:    "one",
				nodeID: "not-in-cluster",
				want:   nil,
			},
		}

		for _, tc := range cases {
			tc := tc
			By(tc.name)
			if tc.nodes != nil {
				seedPlacementMeta(GinkgoT(), b, tc.bucket, tc.key, testVersionID, tc.nodes, tc.ecData, tc.ecParity)
			}
			Expect(b.OwnedShards(tc.bucket, tc.key, testVersionID, tc.nodeID)).To(Equal(tc.want))
		}
	})
})
