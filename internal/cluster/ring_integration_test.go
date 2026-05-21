package cluster

// ring_e2e_test.go: 링 기반 EC 쓰기/읽기 경로 통합 테스트.
//
// 단일 노드(항상 리더)에서 CmdSetRing으로 링을 초기화한 뒤
// PutObject/GetObject가 RingVersion 기반 배치를 사용하는지 검증한다.

import (
	"bytes"
	"context"
	"io"

	"github.com/gritive/GrainFS/internal/storage"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func putBytes(t clusterTestTB, b *DistributedBackend, bucket, key string, data []byte) {
	t.Helper()
	_, err := b.PutObject(context.Background(), bucket, key, bytes.NewReader(data), "application/octet-stream")
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}
}

// proposeRing은 테스트 전용 — nodeIDs로 Ring v1을 FSM에 직접 propose한다.
func proposeRing(t clusterTestTB, b *DistributedBackend, nodeIDs []string) {
	t.Helper()
	ring := NewRing(1, nodeIDs, 10)
	err := b.propose(context.Background(), CmdSetRing, SetRingCmd{
		Version:  ring.Version,
		VNodes:   ring.VNodes,
		VPerNode: ring.VPerNode,
	})
	if err != nil {
		t.Fatalf("propose ring failed: %v", err)
	}
}

var _ = Describe("Ring integration", func() {
	var (
		t       clusterTestTB
		ctx     context.Context
		backend *DistributedBackend
	)

	BeforeEach(func() {
		t = GinkgoT()
		ctx = context.Background()
		backend = NewSingletonBackendForTest(t)
	})

	configureSingleNodeEC := func(selfAddr string) {
		svc := NewShardService(t.TempDir(), nil)
		backend.SetShardService(svc, []string{selfAddr})
		backend.SetECConfig(ECConfig{DataShards: 1, ParityShards: 1})
	}

	It("uses ring version for EC PutObject", func() {
		const selfAddr = "self"
		configureSingleNodeEC(selfAddr)
		proposeRing(t, backend, []string{selfAddr})

		Expect(backend.CurrentRingVersion()).To(Equal(RingVersion(1)), "ring version should be 1 after init")

		Expect(backend.CreateBucket(ctx, "bucket")).To(Succeed())
		content := []byte("hello ring ec world")
		putBytes(t, backend, "bucket", "key", content)

		rc, obj, err := backend.GetObject(ctx, "bucket", "key")
		Expect(err).NotTo(HaveOccurred())
		Expect(obj).NotTo(BeNil())
		DeferCleanup(rc.Close)

		got, err := io.ReadAll(rc)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(content), "content must round-trip via ring EC path")
	})

	It("uses ring version for multipart complete", func() {
		const selfAddr = "self"
		configureSingleNodeEC(selfAddr)
		proposeRing(t, backend, []string{selfAddr})

		Expect(backend.CreateBucket(ctx, "bucket")).To(Succeed())
		up, err := backend.CreateMultipartUpload(ctx, "bucket", "mp.bin", "application/octet-stream")
		Expect(err).NotTo(HaveOccurred())
		part, err := backend.UploadPart(ctx, "bucket", "mp.bin", up.UploadID, 1, bytes.NewReader([]byte("multipart ring payload")))
		Expect(err).NotTo(HaveOccurred())
		_, err = backend.CompleteMultipartUpload(ctx, "bucket", "mp.bin", up.UploadID, []storage.Part{*part})
		Expect(err).NotTo(HaveOccurred())

		_, _, err = backend.headObjectMeta(ctx, "bucket", "mp.bin")
		Expect(err).NotTo(HaveOccurred())
	})

	It("falls back when no ring is configured", func() {
		const selfAddr = "self"
		configureSingleNodeEC(selfAddr)

		Expect(backend.CreateBucket(ctx, "bucket")).To(Succeed())
		content := []byte("no ring fallback")
		putBytes(t, backend, "bucket", "key", content)

		rc, obj, err := backend.GetObject(ctx, "bucket", "key")
		Expect(err).NotTo(HaveOccurred())
		Expect(obj).NotTo(BeNil())
		DeferCleanup(rc.Close)

		got, err := io.ReadAll(rc)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(content))
	})

	It("reports zero ring version before init", func() {
		Expect(backend.CurrentRingVersion()).To(Equal(RingVersion(0)))
	})

	It("reports ring version after init", func() {
		proposeRing(t, backend, []string{"node-A", "node-B"})
		Expect(backend.CurrentRingVersion()).To(Equal(RingVersion(1)))
	})

	It("noops when resharding to the current ring version", func() {
		const selfAddr = "self"
		configureSingleNodeEC(selfAddr)

		proposeRing(t, backend, []string{selfAddr})
		Expect(backend.CreateBucket(ctx, "b")).To(Succeed())
		putBytes(t, backend, "b", "k", []byte("data"))

		Expect(backend.ReshardToRing(ctx, "b", "k", RingVersion(1))).To(Succeed())
	})
})
