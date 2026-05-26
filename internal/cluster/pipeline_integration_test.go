package cluster_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"path/filepath"
	"time"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/cluster/putpipeline"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Backend put pipeline integration", func() {
	var (
		b   *cluster.DistributedBackend
		ctx context.Context
	)

	BeforeEach(func() {
		b = cluster.NewTestDistributedBackend(GinkgoT())
		ctx = context.Background()
	})

	It("round-trips objects through the actor pipeline", func() {
		enc, err := encrypt.NewEncryptor(bytes.Repeat([]byte{0xAB}, 32))
		Expect(err).NotTo(HaveOccurred())
		b.SetShardService(cluster.NewShardService(b.Root(), nil, cluster.WithEncryptor(enc)), []string{b.SelfAddr()})

		shardsDir := filepath.Join(b.Root(), "shards")
		pipeline := putpipeline.New(putpipeline.Config{
			DataDirs:  []string{shardsDir},
			Encryptor: enc,
			ECConfig:  cluster.ECConfig{DataShards: 1, ParityShards: 0},
		})
		DeferCleanup(func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = pipeline.Shutdown(shutdownCtx)
		})
		b.SetPutPipeline(pipeline)

		Expect(b.CreateBucket(ctx, "bucket")).To(Succeed())

		payload := []byte("hello from the actor pipeline")
		size := int64(len(payload))
		obj, err := b.PutObjectWithRequest(ctx, storage.PutObjectRequest{
			Bucket:      "bucket",
			Key:         "actor.txt",
			Body:        bytes.NewReader(payload),
			SizeHint:    &size,
			ContentType: "text/plain",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(obj.Size).To(Equal(int64(len(payload))))
		Expect(obj.ContentType).To(Equal("text/plain"))
		Expect(obj.ETag).NotTo(BeEmpty())
		want := md5.Sum(payload)
		Expect(obj.ETag).To(Equal(hex.EncodeToString(want[:])))

		rc, gotObj, err := b.GetObject(ctx, "bucket", "actor.txt")
		Expect(err).NotTo(HaveOccurred())
		got, readErr := io.ReadAll(rc)
		closeErr := rc.Close()
		Expect(readErr).NotTo(HaveOccurred())
		Expect(closeErr).NotTo(HaveOccurred())
		Expect(got).To(Equal(payload))
		Expect(gotObj.ETag).To(Equal(obj.ETag))
		Expect(gotObj.Size).To(Equal(obj.Size))
	})
})
