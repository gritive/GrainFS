package cluster

import (
	"bytes"
	"context"
	"io"
	"os"

	"github.com/gritive/GrainFS/internal/encrypt"
	"go.uber.org/goleak"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("EC compatibility integration", func() {
	var (
		b   *DistributedBackend
		ctx context.Context
	)

	BeforeEach(func() {
		if CurrentSpecReport().LeafNodeText == "retrieves EC objects without leaking goroutines after k-of-n succeeds" {
			// Snapshot goroutines that already exist before this spec builds its
			// backend. goleak.VerifyNone is process-global, so without a baseline
			// it also catches quic-go transport goroutines (Transport.listen,
			// sendQueue.Run, Conn.run) left behind by *other* cluster tests in the
			// same binary — those lag past goleak's retry budget under load and
			// made this assertion flaky in multi-package runs. Baselining here keeps
			// the check scoped to goroutines this spec's EC read path creates.
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
		b = newTestDistributedBackend(GinkgoT())
		ctx = context.Background()
	})

	configureEC := func(dataShards, parityShards int) {
		GinkgoHelper()
		b.SetECConfig(ECConfig{DataShards: dataShards, ParityShards: parityShards})
		enc := testEncryptor(GinkgoT())
		svc := NewShardService(b.root, nil, WithEncryptor(enc), withTestWALEnc(GinkgoT(), enc))
		nodes := make([]string, dataShards+parityShards)
		for i := range nodes {
			nodes[i] = b.selfAddr
		}
		b.SetShardService(svc, nodes)
	}

	It("stores the first shard service node as selfAddr", func() {
		enc := testEncryptor(GinkgoT())
		svc := NewShardService(b.root, nil, WithEncryptor(enc), withTestWALEnc(GinkgoT(), enc))
		allNodes := []string{"addr-self:9001", "addr-peer1:9001", "addr-peer2:9001"}

		b.SetShardService(svc, allNodes)

		Expect(b.selfAddr).To(Equal("addr-self:9001"))
		Expect(b.allNodes).To(ContainElement(b.selfAddr))
	})

	It("rejects distinct-peer topology for internal writes", func() {
		enc := testEncryptor(GinkgoT())
		svc := NewShardService(b.root, nil, WithEncryptor(enc), withTestWALEnc(GinkgoT(), enc))
		b.SetShardService(svc, []string{b.selfAddr, "peer-1", "peer-2"})

		Expect(b.PreferWriteAt("__grainfs_volumes")).To(BeFalse())
	})

	It("rejects encrypted shard storage for internal writes", func() {
		enc, err := encrypt.NewEncryptor(bytes.Repeat([]byte{0x44}, 32))
		Expect(err).NotTo(HaveOccurred())
		svc := NewShardService(b.root, nil, WithEncryptor(enc), withTestWALEnc(GinkgoT(), enc))
		b.SetShardService(svc, []string{b.selfAddr, b.selfAddr, b.selfAddr})

		Expect(b.PreferWriteAt("__grainfs_volumes")).To(BeFalse())
		_, err = b.WriteAt(ctx, "__grainfs_volumes", "vol/blk", 0, []byte("abcd"))
		Expect(err).To(HaveOccurred())
		Expect(b.Truncate(ctx, "__grainfs_volumes", "vol/blk", 4)).To(HaveOccurred())
	})

	It("keeps selfAddr different from the Raft node ID", func() {
		enc := testEncryptor(GinkgoT())
		svc := NewShardService(b.root, nil, WithEncryptor(enc), withTestWALEnc(GinkgoT(), enc))
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
		writePlacement(GinkgoT(), b, "bkt", shardKey, nodes)

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

		writePlacement(GinkgoT(), b, "bkt", key+"/v1", v1Nodes)
		writePlacement(GinkgoT(), b, "bkt", key+"/v2", v2Nodes)

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
		Expect(os.Remove(b.shardSvc.getShardPath("bkt", shardKey, 0))).To(Succeed())

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
