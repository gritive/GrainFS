package cluster_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"os"
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

	// All-local K>=2 multi-stripe PUT now goes through the streaming pipeline,
	// which writes a stripe-INTERLEAVED shard layout (one fragment per stripe per
	// shard) and stamps the stripe size into object metadata as StripeBytes > 0.
	// GET de-interleaves per stripe when that marker is set, so a multi-stripe
	// object (> StripeBytes) round-trips byte-for-byte. The degraded sub-case
	// deletes one DATA shard's on-disk file and asserts GET still recovers the
	// payload via per-stripe Reed-Solomon reconstruction through the striped
	// reader.
	It("round-trips a K>=2 multi-stripe object through the streaming pipeline (interleaved PUT, de-interleaving GET) and survives a lost data shard", func() {
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
		// The PUT took the streaming-pipeline path: the returned object carries the
		// pipeline's stripe size as the de-interleave marker (default 1<<20).
		Expect(obj.StripeBytes).To(BeNumerically(">", uint32(0)))

		rc, _, err := b.GetObject(ctx, "bucket", "big.bin")
		Expect(err).NotTo(HaveOccurred())
		got, readErr := io.ReadAll(rc)
		closeErr := rc.Close()
		Expect(readErr).NotTo(HaveOccurred())
		Expect(closeErr).NotTo(HaveOccurred())
		Expect(got).To(Equal(payload))

		// Degraded GET: with 2 data + 1 parity, losing one DATA shard forces the
		// reader to Reed-Solomon reconstruct the missing fragment per stripe (the
		// striped reader's degraded path). Deleting a DATA shard (not parity) is
		// what actually exercises reconstruction: dropping parity would leave all
		// data present and de-interleave directly. shard i lives under
		// dataRoots[i] at {root}/shards/{bucket}/{shardKey}/shard_{i}, where
		// shardKey == "big.bin/<versionID>".
		shard0Glob := filepath.Join(dataRoots[0], "shards", "bucket", "big.bin", "*", "shard_0")
		matches, globErr := filepath.Glob(shard0Glob)
		Expect(globErr).NotTo(HaveOccurred())
		Expect(matches).To(HaveLen(1), "exactly one data-shard-0 file should exist on disk")
		Expect(os.Remove(matches[0])).To(Succeed())

		rc2, _, err := b.GetObject(ctx, "bucket", "big.bin")
		Expect(err).NotTo(HaveOccurred())
		got2, readErr2 := io.ReadAll(rc2)
		closeErr2 := rc2.Close()
		Expect(readErr2).NotTo(HaveOccurred())
		Expect(closeErr2).NotTo(HaveOccurred())
		Expect(got2).To(Equal(payload), "degraded GET must reconstruct the payload after losing a data shard")
	})

	// Characterization (Task 8): upgradeObjectEC re-encodes an object into a new
	// EC config by reading the source via the now-striping-aware ReadObject. The
	// source here is a real striped (interleaved, StripeBytes > 0) K=2 pipeline
	// PUT, so the upgrade's reconstruct-read must de-interleave correctly. The
	// upgrade write path does NOT stripe (contiguous re-split), so the upgraded
	// object's StripeBytes returns to 0 — a behavior we pin here. This guards the
	// upgrade-reads-striped-source dependency end to end (PUT striped -> upgrade
	// -> GET original bytes), which the contiguous-seeded reshard unit tests in
	// reshard_manager_test.go do not cover.
	It("upgrades a striped K=2 pipeline object to a new EC config and reads back the original bytes (contiguous re-split)", func() {
		clusterID := bytes.Repeat([]byte{0x42}, 16)
		keeper, err := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0x91}, encrypt.KEKSize), clusterID)
		Expect(err).NotTo(HaveOccurred())
		dwal, err := datawal.Open(filepath.Join(b.Root(), "datawal"), storage.NewDEKKeeperAdapter(keeper, clusterID), datawal.NamespaceShard)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = dwal.Close() })

		ec := cluster.ECConfig{DataShards: 2, ParityShards: 1}
		b.SetECConfig(ec)
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

		payload := make([]byte, 3*1024*1024+123)
		for i := range payload {
			payload[i] = byte((i*13 + 5) % 251)
		}
		size := int64(len(payload))
		obj, err := b.PutObjectWithRequest(ctx, storage.PutObjectRequest{
			Bucket:   "bucket",
			Key:      "upgrade.bin",
			Body:     bytes.NewReader(payload),
			SizeHint: &size,
		})
		Expect(err).NotTo(HaveOccurred())
		// Source is genuinely striped: upgrade's ReadObject must de-interleave.
		Expect(obj.StripeBytes).To(BeNumerically(">", uint32(0)))

		// Upgrade 2+1 -> 3+2. The reshard read path reconstructs via the
		// striping-aware ReadObject; the re-encode write path is contiguous.
		newCfg := cluster.ECConfig{DataShards: 3, ParityShards: 2}
		Expect(b.UpgradeObjectECForTest(ctx, "bucket", "upgrade.bin", newCfg)).To(Succeed())

		upObj, err := b.HeadObject(ctx, "bucket", "upgrade.bin")
		Expect(err).NotTo(HaveOccurred())
		Expect(upObj.ECData).To(Equal(uint8(newCfg.DataShards)))
		Expect(upObj.ECParity).To(Equal(uint8(newCfg.ParityShards)))
		// Upgrade re-split is contiguous, so the stripe marker is cleared.
		Expect(upObj.StripeBytes).To(Equal(uint32(0)))

		rc, _, err := b.GetObject(ctx, "bucket", "upgrade.bin")
		Expect(err).NotTo(HaveOccurred())
		got, readErr := io.ReadAll(rc)
		closeErr := rc.Close()
		Expect(readErr).NotTo(HaveOccurred())
		Expect(closeErr).NotTo(HaveOccurred())
		Expect(got).To(Equal(payload), "object content must survive EC upgrade from a striped source")
	})
})
