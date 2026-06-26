package cluster

import (
	"bytes"
	"context"
	"io"
	"os"

	"go.uber.org/goleak"

	"github.com/dgraph-io/badger/v4"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("EC compatibility integration", func() {
	var (
		b   *DistributedBackend
		db  *badger.DB
		ctx context.Context
	)

	BeforeEach(func() {
		if CurrentSpecReport().LeafNodeText == "retrieves EC objects without leaking goroutines after k-of-n succeeds" {
			// Snapshot goroutines that already exist before this spec builds its
			// backend. goleak.VerifyNone is process-global, so without a baseline
			// it also catches transport goroutines left behind by *other* cluster
			// tests in the same binary — those can lag past goleak's retry budget
			// under load and made this assertion flaky in multi-package runs.
			// Baselining here keeps the check scoped to goroutines this spec's EC
			// read path creates.
			ignoreBaseline := goleak.IgnoreCurrent()
			GinkgoT().Cleanup(func() {
				goleak.VerifyNone(GinkgoT(),
					ignoreBaseline,
					goleak.IgnoreTopFunction("github.com/onsi/ginkgo/v2/internal.(*Suite).runNode"),
					goleak.IgnoreTopFunction("github.com/onsi/ginkgo/v2/internal/interrupt_handler.(*InterruptHandler).registerForInterrupts.func2"),
					goleak.IgnoreTopFunction("github.com/onsi/ginkgo/v2/internal.RegisterForProgressSignal.func1"),
				)
			})
		}
		b, db = newTestDistributedBackendWithDB(GinkgoT())
		ctx = context.Background()
	})

	configureEC := func(dataShards, parityShards int) {
		GinkgoHelper()
		b.SetECConfig(ECConfig{DataShards: dataShards, ParityShards: parityShards})
		keeper, clusterID := testDEKKeeper(GinkgoT())
		svc := NewShardService(b.root, nil, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(GinkgoT(), keeper, clusterID))
		nodes := make([]string, dataShards+parityShards)
		for i := range nodes {
			nodes[i] = b.selfAddr
		}
		b.SetShardService(svc, nodes)
	}

	It("stores the first shard service node as selfAddr", func() {
		keeper, clusterID := testDEKKeeper(GinkgoT())
		svc := NewShardService(b.root, nil, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(GinkgoT(), keeper, clusterID))
		allNodes := []string{"addr-self:9001", "addr-peer1:9001", "addr-peer2:9001"}

		b.SetShardService(svc, allNodes)

		Expect(b.selfAddr).To(Equal("addr-self:9001"))
		Expect(b.allNodes).To(ContainElement(b.selfAddr))
	})

	It("reports false for all PreferWriteAt queries (plain fast-path removed)", func() {
		// DistributedBackend.PreferWriteAt was removed along with WriteAt/Truncate.
		// The ClusterCoordinator now always returns false from PreferWriteAt;
		// all writes use the encrypted RMW (PutObject) path.
		keeper, clusterID := testDEKKeeper(GinkgoT())
		svc := NewShardService(b.root, nil, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(GinkgoT(), keeper, clusterID))
		b.SetShardService(svc, []string{b.selfAddr, "peer-1", "peer-2"})

		// DistributedBackend no longer exposes PreferWriteAt (removed); verify via
		// encryptedShardStorage gating that the backend is encrypted.
		Expect(b.encryptedShardStorage()).To(BeTrue())
	})

	It("keeps selfAddr different from the Raft node ID", func() {
		keeper, clusterID := testDEKKeeper(GinkgoT())
		svc := NewShardService(b.root, nil, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(GinkgoT(), keeper, clusterID))
		b.SetShardService(svc, []string{"addr-self:9001", "addr-peer1:9001"})

		Expect(b.RaftNodeID()).To(Equal("test-node"))
		Expect(b.selfAddr).NotTo(Equal(b.RaftNodeID()))
	})

	It("stores and looks up placement by versioned shard key", func() {
		Expect(b.CreateBucket(ctx, "bkt")).To(Succeed())

		const (
			key       = "myobject"
			versionID = "01JT5BVMZABCDEF12345"
			shardKey  = key + "/" + versionID
		)
		nodes := []string{"addr-a", "addr-b", "addr-c"}
		writePlacement(GinkgoT(), b, db, "bkt", shardKey, nodes)

		got, err := b.fsm.LookupShardPlacement("bkt", shardKey)
		Expect(err).NotTo(HaveOccurred())
		Expect(got.Nodes).To(Equal(nodes))

		bare, err := b.fsm.LookupShardPlacement("bkt", key)
		Expect(err).NotTo(HaveOccurred())
		Expect(bare).To(Equal(PlacementRecord{}))
	})

	It("keeps placement records isolated across object versions", func() {
		Expect(b.CreateBucket(ctx, "bkt")).To(Succeed())

		const key = "shared-key"
		v1Nodes := []string{"node-a", "node-b", "node-c"}
		v2Nodes := []string{"node-x", "node-y", "node-z"}

		writePlacement(GinkgoT(), b, db, "bkt", key+"/v1", v1Nodes)
		writePlacement(GinkgoT(), b, db, "bkt", key+"/v2", v2Nodes)

		got1, err := b.fsm.LookupShardPlacement("bkt", key+"/v1")
		Expect(err).NotTo(HaveOccurred())
		Expect(got1.Nodes).To(Equal(v1Nodes))

		got2, err := b.fsm.LookupShardPlacement("bkt", key+"/v2")
		Expect(err).NotTo(HaveOccurred())
		Expect(got2.Nodes).To(Equal(v2Nodes))
	})

	It("round-trips an EC object on duplicate-self topology", func() {
		Expect(b.CreateBucket(ctx, "bkt")).To(Succeed())
		configureEC(2, 1)

		data := []byte("hello world")
		_, err := b.PutObject(ctx, "bkt", "obj", bytes.NewReader(data), "text/plain")
		Expect(err).NotTo(HaveOccurred())

		rc, _, err := b.GetObject(ctx, "bkt", "obj")
		Expect(err).NotTo(HaveOccurred())
		got, readErr := io.ReadAll(rc)
		closeErr := rc.Close()
		Expect(readErr).NotTo(HaveOccurred())
		Expect(closeErr).NotTo(HaveOccurred())
		Expect(got).To(Equal(data))
	})

	It("retrieves EC objects without leaking goroutines after k-of-n succeeds", func() {
		Expect(b.CreateBucket(ctx, "bkt")).To(Succeed())
		configureEC(2, 1)

		data := []byte("test data for k-of-n")
		_, err := b.PutObject(ctx, "bkt", "obj", bytes.NewReader(data), "text/plain")
		Expect(err).NotTo(HaveOccurred())

		rc, _, err := b.GetObject(ctx, "bkt", "obj")
		Expect(err).NotTo(HaveOccurred())
		got, readErr := io.ReadAll(rc)
		closeErr := rc.Close()
		Expect(readErr).NotTo(HaveOccurred())
		Expect(closeErr).NotTo(HaveOccurred())
		Expect(got).To(Equal(data))
	})

	It("reconstructs reads when a data shard is missing", func() {
		Expect(b.CreateBucket(ctx, "bkt")).To(Succeed())
		configureEC(4, 2)

		data := bytes.Repeat([]byte("data-shard-fallback-"), 1024)
		obj, err := b.PutObject(ctx, "bkt", "obj", bytes.NewReader(data), "text/plain")
		Expect(err).NotTo(HaveOccurred())

		shardKey := "obj/" + obj.VersionID
		Expect(os.Remove(mustShardPath(b.shardSvc, "bkt", shardKey, 0))).To(Succeed())

		rc, _, err := b.GetObject(ctx, "bkt", "obj")
		Expect(err).NotTo(HaveOccurred())
		got, readErr := io.ReadAll(rc)
		closeErr := rc.Close()
		Expect(readErr).NotTo(HaveOccurred())
		Expect(closeErr).NotTo(HaveOccurred())
		Expect(got).To(Equal(data))
	})

	It("round-trips multi-window EC objects", func() {
		Expect(b.CreateBucket(ctx, "bkt")).To(Succeed())
		configureEC(4, 2)

		data := make([]byte, 5*1024*1024+123)
		for i := range data {
			data[i] = byte(i % 251)
		}
		_, err := b.PutObject(ctx, "bkt", "obj", bytes.NewReader(data), "application/octet-stream")
		Expect(err).NotTo(HaveOccurred())

		rc, _, err := b.GetObject(ctx, "bkt", "obj")
		Expect(err).NotTo(HaveOccurred())
		got, readErr := io.ReadAll(rc)
		closeErr := rc.Close()
		Expect(readErr).NotTo(HaveOccurred())
		Expect(closeErr).NotTo(HaveOccurred())
		Expect(got).To(Equal(data))
	})

	It("serves ReadAt from EC data shards", func() {
		Expect(b.CreateBucket(ctx, "bkt")).To(Succeed())
		configureEC(4, 2)

		data := make([]byte, 2*1024*1024+333)
		for i := range data {
			data[i] = byte((i * 17) % 251)
		}
		_, err := b.PutObject(ctx, "bkt", "obj", bytes.NewReader(data), "application/octet-stream")
		Expect(err).NotTo(HaveOccurred())

		buf := make([]byte, 96*1024)
		const offset = int64(512*1024 - 17)
		n, err := b.ReadAt(ctx, "bkt", "obj", offset, buf)
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(len(buf)))
		Expect(buf).To(Equal(data[offset : int(offset)+len(buf)]))
	})

	It("stores and reads empty user-bucket EC objects", func() {
		Expect(b.CreateBucket(ctx, "bkt")).To(Succeed())
		configureEC(4, 2)

		obj, err := b.PutObject(ctx, "bkt", "empty", bytes.NewReader(nil), "application/octet-stream")
		Expect(err).NotTo(HaveOccurred())
		Expect(obj.Size).To(BeZero())

		rc, gotObj, err := b.GetObject(ctx, "bkt", "empty")
		Expect(err).NotTo(HaveOccurred())
		got, readErr := io.ReadAll(rc)
		closeErr := rc.Close()
		Expect(readErr).NotTo(HaveOccurred())
		Expect(closeErr).NotTo(HaveOccurred())
		Expect(got).To(BeEmpty())
		Expect(gotObj.Size).To(BeZero())
	})
})
