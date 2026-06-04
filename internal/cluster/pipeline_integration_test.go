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
	"github.com/gritive/GrainFS/internal/storage/datawal"

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
		// The same keeper instance + clusterID must thread through the data WAL
		// adapter, the ShardService sealer, and the put pipeline so that
		// PUT-via-pipeline seals and GET-via-ShardService opens under one DEK.
		clusterID := bytes.Repeat([]byte{0x42}, 16)
		keeper, err := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0x91}, encrypt.KEKSize), clusterID)
		Expect(err).NotTo(HaveOccurred())
		dwal, err := datawal.Open(filepath.Join(b.Root(), "datawal"), storage.NewDEKKeeperAdapter(keeper, clusterID), datawal.NamespaceShard)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = dwal.Close() })
		b.SetShardService(cluster.NewShardService(b.Root(), nil, cluster.WithShardDEKKeeper(keeper, clusterID), cluster.WithDataWAL(dwal)), []string{b.SelfAddr()})

		shardsDir := filepath.Join(b.Root(), "shards")
		pipeline := putpipeline.New(putpipeline.Config{
			DataDirs:  []string{shardsDir},
			DEKKeeper: keeper,
			ClusterID: clusterID,
			ECConfig:  cluster.ECConfig{DataShards: 1, ParityShards: 0},
		})
		DeferCleanup(func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = pipeline.Shutdown(shutdownCtx)
		})
		b.SetPutPipeline(pipeline, true)

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

	// Guards against a latent corruption path: when the all-local pipeline runs
	// with K>=2, CPUPool writes a stripe-INTERLEAVED shard layout (one fragment
	// per stripe per shard), but the production GET reader expects each shard to
	// be a CONTIGUOUS 1/K slice of the object. They agree only for a single
	// stripe; a multi-stripe object (> StripeBytes) round-trips to garbage. The
	// dispatch guard must route K>=2 to the spool writer (contiguous layout)
	// until a de-interleaving reader lands, so this PUT->GET must recover bytes.
	It("round-trips a K>=2 multi-stripe object (dispatch guard avoids interleaved-layout corruption)", func() {
		clusterID := bytes.Repeat([]byte{0x42}, 16)
		keeper, err := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0x91}, encrypt.KEKSize), clusterID)
		Expect(err).NotTo(HaveOccurred())
		dwal, err := datawal.Open(filepath.Join(b.Root(), "datawal"), storage.NewDEKKeeperAdapter(keeper, clusterID), datawal.NamespaceShard)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = dwal.Close() })

		ec := cluster.ECConfig{DataShards: 2, ParityShards: 1}
		b.SetECConfig(ec)
		// One drive per shard, mirroring a single node with NumShards drives (the
		// standard multi-drive single-node EC layout, e.g. 6 drives doing 4+2).
		// A single shared root would round-robin every shard onto one drive and
		// collide; that is a drive-count artifact, not the layout bug under test.
		dataRoots := make([]string, ec.NumShards())
		for i := range dataRoots {
			dataRoots[i] = GinkgoT().TempDir()
		}
		svc := cluster.NewMultiRootShardService(dataRoots, nil, cluster.WithShardDEKKeeper(keeper, clusterID), cluster.WithDataWAL(dwal))
		nodes := make([]string, ec.NumShards())
		for i := range nodes {
			nodes[i] = b.SelfAddr()
		}
		b.SetShardService(svc, nodes)

		// Wire the pipeline exactly like boot does: DataDirs == ShardService's, so
		// the local sink writes where GET reads.
		pipeline := putpipeline.New(putpipeline.Config{
			DataDirs:  svc.DataDirs(),
			DEKKeeper: keeper,
			ClusterID: clusterID,
			ECConfig:  ec,
		})
		DeferCleanup(func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = pipeline.Shutdown(shutdownCtx)
		})
		b.SetPutPipeline(pipeline, true)

		Expect(b.CreateBucket(ctx, "bucket")).To(Succeed())

		// > 1 MiB default StripeBytes forces multiple stripes (the +123 also
		// exercises a non-stripe-aligned tail).
		payload := make([]byte, 3*1024*1024+123)
		for i := range payload {
			payload[i] = byte((i * 7) % 251)
		}
		size := int64(len(payload))
		obj, err := b.PutObjectWithRequest(ctx, storage.PutObjectRequest{
			Bucket:   "bucket",
			Key:      "big.bin",
			Body:     bytes.NewReader(payload),
			SizeHint: &size,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(obj.Size).To(Equal(size))

		rc, _, err := b.GetObject(ctx, "bucket", "big.bin")
		Expect(err).NotTo(HaveOccurred())
		got, readErr := io.ReadAll(rc)
		closeErr := rc.Close()
		Expect(readErr).NotTo(HaveOccurred())
		Expect(closeErr).NotTo(HaveOccurred())
		Expect(got).To(Equal(payload))
	})
})
