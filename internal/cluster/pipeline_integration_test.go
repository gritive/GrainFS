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
		Skip("Phase 3: upgradeObjectEC writes to BadgerDB, headObjectMeta reads stale quorum meta")

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

	// Versioned GET must take the same striped de-interleave path as latest GET.
	// headObjectMetaV builds its own storage.Object + PlacementMeta; if it drops
	// the StripeBytes marker, ResolvePlacement hands the reader a zero marker and
	// GetObjectVersion reads a K>=2 multi-stripe version as contiguous garbage.
	It("round-trips a K>=2 multi-stripe object through versioned GET (GetObjectVersion de-interleaves)", func() {
		Skip("Phase 3: versioned GET not yet adapted to quorum meta store")

		_, _ = wireK2InterleavedPipeline(b)
		Expect(b.CreateBucket(ctx, "bucket")).To(Succeed())
		Expect(b.SetBucketVersioning("bucket", "Enabled")).To(Succeed())

		payload := make([]byte, 3*1024*1024+123)
		for i := range payload {
			payload[i] = byte((i*7)%251 + 1)
		}
		size := int64(len(payload))
		obj, err := b.PutObjectWithRequest(ctx, storage.PutObjectRequest{
			Bucket:   "bucket",
			Key:      "ver.bin",
			Body:     bytes.NewReader(payload),
			SizeHint: &size,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(obj.StripeBytes).To(BeNumerically(">", uint32(0)))
		Expect(obj.VersionID).NotTo(BeEmpty())

		rc, _, err := b.GetObjectVersion("bucket", "ver.bin", obj.VersionID)
		Expect(err).NotTo(HaveOccurred())
		got, readErr := io.ReadAll(rc)
		closeErr := rc.Close()
		Expect(readErr).NotTo(HaveOccurred())
		Expect(closeErr).NotTo(HaveOccurred())
		Expect(got).To(Equal(payload), "versioned GET of a striped object must de-interleave, not read contiguous garbage")
	})

	// Appending to a plain (pipeline-written, interleaved) object transitions it
	// to appendable and captures its base shards as a coalesced ref via
	// appendBaseCoalescedRef. Pre-flip a K>=2 base was always contiguous (spool),
	// so the base coalesced ref dropping StripeBytes was harmless; the all-local
	// K>=2 flip makes the base interleaved, so the marker MUST carry forward
	// through CoalescedShardRef -> storage.CoalescedRef -> the appendable reader,
	// or the base portion reads back as contiguous garbage.
	It("round-trips a K>=2 multi-stripe base after it transitions to appendable (base coalesced ref carries StripeBytes)", func() {
		Skip("Phase 3: appendable object metadata not yet adapted to quorum meta store")

		_, _ = wireK2InterleavedPipeline(b)
		Expect(b.CreateBucket(ctx, "bucket")).To(Succeed())

		base := make([]byte, 3*1024*1024+123)
		for i := range base {
			base[i] = byte((i*7)%251 + 1)
		}
		bsize := int64(len(base))
		obj, err := b.PutObjectWithRequest(ctx, storage.PutObjectRequest{
			Bucket:   "bucket",
			Key:      "app.bin",
			Body:     bytes.NewReader(base),
			SizeHint: &bsize,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(obj.StripeBytes).To(BeNumerically(">", uint32(0)))

		tail := []byte("appended-tail-bytes")
		apObj, err := b.AppendObject(ctx, "bucket", "app.bin", bsize, bytes.NewReader(tail))
		Expect(err).NotTo(HaveOccurred())
		Expect(apObj.IsAppendable).To(BeTrue())
		Expect(apObj.Size).To(Equal(bsize + int64(len(tail))))

		rc, _, err := b.GetObject(ctx, "bucket", "app.bin")
		Expect(err).NotTo(HaveOccurred())
		got, readErr := io.ReadAll(rc)
		closeErr := rc.Close()
		Expect(readErr).NotTo(HaveOccurred())
		Expect(closeErr).NotTo(HaveOccurred())
		want := append(append([]byte(nil), base...), tail...)
		Expect(got).To(Equal(want), "appended object's interleaved base must de-interleave, not read contiguous garbage")
	})

	// Range GET (ReadAt) over an appendable object's interleaved base goes through
	// readAtChunk -> ec_object_reader.ReadAt, which gates the stripe de-interleave
	// path on rec.StripeBytes > 0. If readAtChunk's PlacementRecord drops the
	// marker, a range spanning the multi-stripe base reads as contiguous garbage.
	It("range-reads (ReadAt) a K>=2 multi-stripe appendable base via readAtChunk (carries StripeBytes)", func() {
		Skip("Phase 3: appendable/range-read metadata not yet adapted to quorum meta store")

		_, _ = wireK2InterleavedPipeline(b)
		Expect(b.CreateBucket(ctx, "bucket")).To(Succeed())

		base := make([]byte, 3*1024*1024+123)
		for i := range base {
			base[i] = byte((i*7)%251 + 1)
		}
		bsize := int64(len(base))
		obj, err := b.PutObjectWithRequest(ctx, storage.PutObjectRequest{
			Bucket:   "bucket",
			Key:      "rng.bin",
			Body:     bytes.NewReader(base),
			SizeHint: &bsize,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(obj.StripeBytes).To(BeNumerically(">", uint32(0)))

		tail := []byte("appended-tail-bytes")
		_, err = b.AppendObject(ctx, "bucket", "rng.bin", bsize, bytes.NewReader(tail))
		Expect(err).NotTo(HaveOccurred())

		// Read a window inside the base that straddles the 1 MiB stripe boundary
		// (offset 1.5 MiB, length 1 MiB) so interleaved != contiguous.
		off := int64(1536 * 1024)
		buf := make([]byte, 1024*1024)
		n, err := b.ReadAt(ctx, "bucket", "rng.bin", off, buf)
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(len(buf)))
		Expect(buf).To(Equal(base[off:off+int64(len(buf))]), "range read over the interleaved base must de-interleave, not read contiguous garbage")
	})

	// Startup data-WAL repair regenerates a missing shard via
	// ResolveShardKeyPlacement -> reconstructShardAtKey, which re-interleaves when
	// StripeBytes > 0. The ObjectVersion resolve goes through LookupObjectPlacement;
	// if it drops the marker, the repaired shard is rebuilt with a contiguous (not
	// interleaved) layout and no longer matches its striped siblings, so a later
	// GET that reads the repaired data shard de-interleaves garbage.
	It("repairs a missing shard of a K>=2 striped object with the correct interleaved layout (LookupObjectPlacement carries StripeBytes)", func() {
		Skip("Phase 3: shard repair reads BadgerDB placement, not quorum meta store")

		dataRoots, _ := wireK2InterleavedPipeline(b)
		Expect(b.CreateBucket(ctx, "bucket")).To(Succeed())

		payload := make([]byte, 3*1024*1024+123)
		for i := range payload {
			payload[i] = byte((i*7)%251 + 1)
		}
		size := int64(len(payload))
		obj, err := b.PutObjectWithRequest(ctx, storage.PutObjectRequest{
			Bucket:   "bucket",
			Key:      "repair.bin",
			Body:     bytes.NewReader(payload),
			SizeHint: &size,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(obj.StripeBytes).To(BeNumerically(">", uint32(0)))

		shardKey := "repair.bin/" + obj.VersionID

		// Control: a healthy GET (no repair) must already round-trip, isolating any
		// failure below to the repair path rather than PUT/GET for this config.
		rcBase, _, baseErr := b.GetObject(ctx, "bucket", "repair.bin")
		Expect(baseErr).NotTo(HaveOccurred())
		gotBase, baseReadErr := io.ReadAll(rcBase)
		Expect(baseReadErr).NotTo(HaveOccurred())
		Expect(rcBase.Close()).To(Succeed())
		Expect(gotBase).To(Equal(payload), "control: healthy GET must round-trip before testing repair")

		// Delete data shard 0 on disk, then drive the startup-repair path that
		// resolves placement via LookupObjectPlacement (ObjectVersion shard key).
		shard0Glob := filepath.Join(dataRoots[0], "shards", "bucket", "repair.bin", "*", "shard_0")
		matches, globErr := filepath.Glob(shard0Glob)
		Expect(globErr).NotTo(HaveOccurred())
		Expect(matches).To(HaveLen(1))
		Expect(os.Remove(matches[0])).To(Succeed())

		rec, _, rerr := b.ResolveShardKeyPlacement(ctx, "bucket", shardKey, nil)
		Expect(rerr).NotTo(HaveOccurred())
		Expect(rec.Nodes).NotTo(BeEmpty())
		Expect(b.RepairShardLocalWithIncident(ctx, cluster.IncidentRepairRequest{
			Bucket:    "bucket",
			Key:       "repair.bin",
			ShardKey:  shardKey,
			ShardIdx:  0,
			Placement: rec,
		})).To(Succeed())

		rc, _, err := b.GetObject(ctx, "bucket", "repair.bin")
		Expect(err).NotTo(HaveOccurred())
		got, readErr := io.ReadAll(rc)
		closeErr := rc.Close()
		Expect(readErr).NotTo(HaveOccurred())
		Expect(closeErr).NotTo(HaveOccurred())
		Expect(got).To(Equal(payload), "repaired shard must be re-interleaved to match its striped siblings")
	})

	// The production scrubber (scrubber.go -> RepairShardLocal) repairs a missing shard via
	// RepairShard -> readPlacementMeta + ResolvePlacement -> reconstructShardAtKey, a
	// DIFFERENT placement-resolve path than the startup-repair test above (which passes a
	// pre-resolved ShardKey/Placement and so bypasses ResolvePlacement). If readPlacementMeta
	// (object_get.go) or ResolvePlacement (placement_resolver.go) drops StripeBytes, the
	// scrubber rebuilds the missing shard with a contiguous layout that no longer matches its
	// striped siblings, and a later GET de-interleaves garbage.
	It("repairs a missing shard of a K>=2 striped object via the scrubber RepairShard path (readPlacementMeta + ResolvePlacement carry StripeBytes)", func() {
		// K=3 (not the K=2 helper): at K=2 the per-stripe interleaved shard length
		// happens to equal ceil(objectSize/2), so the contiguous ECReconstruct+ECSplit
		// repair branch coincidentally reproduces the interleaved layout and a dropped
		// StripeBytes would NOT corrupt — a degenerate geometry. At K>=3 (production
		// default is 4+2) per-stripe fragment padding diverges from a single contiguous
		// split, so a contiguous-branch repair of a striped object yields a wrong-layout
		// shard. K=3 makes this test actually discriminate the StripeBytes-carrying path.
		dataRoots, _ := wireInterleavedPipeline(b, cluster.ECConfig{DataShards: 3, ParityShards: 2}, 0)
		Expect(b.CreateBucket(ctx, "bucket")).To(Succeed())

		payload := make([]byte, 3*1024*1024+123)
		for i := range payload {
			payload[i] = byte((i*11)%251 + 1)
		}
		size := int64(len(payload))
		obj, err := b.PutObjectWithRequest(ctx, storage.PutObjectRequest{
			Bucket:   "bucket",
			Key:      "scrub.bin",
			Body:     bytes.NewReader(payload),
			SizeHint: &size,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(obj.StripeBytes).To(BeNumerically(">", uint32(0)))

		// Control: a healthy GET must round-trip, isolating any failure to the repair path.
		rcBase, _, baseErr := b.GetObject(ctx, "bucket", "scrub.bin")
		Expect(baseErr).NotTo(HaveOccurred())
		gotBase, baseReadErr := io.ReadAll(rcBase)
		Expect(baseReadErr).NotTo(HaveOccurred())
		Expect(rcBase.Close()).To(Succeed())
		Expect(gotBase).To(Equal(payload), "control: healthy GET must round-trip before testing repair")

		// Delete data shard 0 on disk, then drive the production scrubber entrypoint
		// (RepairShardLocal, no ShardKey) which resolves placement via readPlacementMeta +
		// ResolvePlacement rather than a caller-supplied record.
		shard0Glob := filepath.Join(dataRoots[0], "shards", "bucket", "scrub.bin", "*", "shard_0")
		matches, globErr := filepath.Glob(shard0Glob)
		Expect(globErr).NotTo(HaveOccurred())
		Expect(matches).To(HaveLen(1))
		Expect(os.Remove(matches[0])).To(Succeed())

		Expect(b.RepairShardLocal("bucket", "scrub.bin", obj.VersionID, 0)).To(Succeed())

		// Shards are encrypted at rest, so the repaired shard_0 ciphertext is never byte-equal
		// to the original even on a correct repair — comparing on-disk bytes is meaningless. The
		// decrypt-and-de-interleave through GET is the discriminator: a GET reads the K data
		// shards (0,1,2) directly, so the just-repaired shard_0 IS on the read path. If
		// readPlacementMeta/ResolvePlacement drops StripeBytes, reconstructShardAtKey takes the
		// contiguous ECReconstruct+ECSplit branch and writes a wrong-layout shard_0 at K>=3, so
		// this GET de-interleaves garbage.
		rc, _, err := b.GetObject(ctx, "bucket", "scrub.bin")
		Expect(err).NotTo(HaveOccurred())
		got, readErr := io.ReadAll(rc)
		closeErr := rc.Close()
		Expect(readErr).NotTo(HaveOccurred())
		Expect(closeErr).NotTo(HaveOccurred())
		Expect(got).To(Equal(payload), "scrubber-repaired shard_0 must be re-interleaved so a GET that reads it recovers the payload")
	})
})

// wireK2InterleavedPipeline configures b with a K=2/parity=1 multi-root shard
// service and a streaming put pipeline whose DataDirs match the shard service,
// so all-local PUTs take the interleaved streaming path (StripeBytes > 0). It
// mirrors boot wiring. Returns the per-shard data roots for degraded-shard
// manipulation. Must be called from within a spec node (uses Ginkgo cleanup).
func wireK2InterleavedPipeline(b *cluster.DistributedBackend) ([]string, *cluster.ShardService) {
	return wireInterleavedPipeline(b, cluster.ECConfig{DataShards: 2, ParityShards: 1}, 0)
}

// wireInterleavedPipeline is the parametric form: it wires the pipeline at ec
// and lays out `drives` single-node data roots (drives==0 means one root per
// shard, i.e. ec.NumShards()). Use drives > ec.NumShards() to pre-provision
// headroom for an in-place EC upgrade (reshard) on the same node.
func wireInterleavedPipeline(b *cluster.DistributedBackend, ec cluster.ECConfig, drives int) ([]string, *cluster.ShardService) {
	clusterID := bytes.Repeat([]byte{0x42}, 16)
	keeper, err := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0x91}, encrypt.KEKSize), clusterID)
	Expect(err).NotTo(HaveOccurred())
	dwal, err := datawal.Open(filepath.Join(b.Root(), "datawal"), storage.NewDEKKeeperAdapter(keeper, clusterID), datawal.NamespaceShard)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() { _ = dwal.Close() })

	b.SetECConfig(ec)
	if drives == 0 {
		drives = ec.NumShards()
	}
	dataRoots := make([]string, drives)
	for i := range dataRoots {
		dataRoots[i] = GinkgoT().TempDir()
	}
	svc := cluster.NewMultiRootShardService(dataRoots, nil, cluster.WithShardDEKKeeper(keeper, clusterID), cluster.WithDataWAL(dwal))
	nodes := make([]string, drives)
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
	return dataRoots, svc
}
