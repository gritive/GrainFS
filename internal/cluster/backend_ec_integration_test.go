package cluster

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Backend EC object integration", func() {
	var (
		b   *DistributedBackend
		ctx context.Context
	)

	BeforeEach(func() {
		b = newTestDistributedBackend(GinkgoT())
		ctx = context.Background()
		Expect(b.CreateBucket(ctx, "bucket")).To(Succeed())
	})

	configureEC := func(cfg ECConfig) {
		GinkgoHelper()
		nodes := make([]string, cfg.NumShards())
		for i := range nodes {
			nodes[i] = b.selfAddr
		}
		b.SetECConfig(cfg)
		// Reuse the backend's existing keeper so FSM-sealed meta written before
		// this reconfigure (e.g. legacy-convert tests) stays decryptable.
		keeper, clusterID := b.shardSvc.DEKKeeper(), b.shardSvc.ClusterID()
		b.SetShardService(NewShardService(b.root, nil, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(GinkgoT(), keeper, clusterID)), nodes)
	}

	It("rejects an object key that escapes the shard data root", func() {
		configureEC(ECConfig{DataShards: 2, ParityShards: 1})

		// The shared EC write path (single + cluster) maps key→shard path via
		// getShardDir, which now rejects a key whose ".." segments escape
		// {dataDir}/{bucket}. The physical no-escape guarantee is asserted
		// deterministically in TestWriteLocalShard_RejectsKeyEscapingShardRoot;
		// here we assert the user-facing PutObject surfaces the rejection.
		_, err := b.PutObject(ctx, "bucket", "../../../escape", bytes.NewReader([]byte("malicious")), "application/octet-stream")
		Expect(err).To(HaveOccurred())

		escaped := filepath.Join(filepath.Dir(b.root), "escape")
		_, statErr := os.Stat(escaped)
		Expect(os.IsNotExist(statErr)).To(BeTrue(), "no shard dir may escape the data dir: %s", escaped)
	})

	It("spools large parity EC shard encoding to disk", func() {
		configureEC(ECConfig{DataShards: 2, ParityShards: 1})

		payload := bytes.Repeat([]byte("b"), 2<<20)
		body := io.LimitReader(bytes.NewReader(payload), int64(len(payload)))
		obj, err := b.PutObject(ctx, "bucket", "large-spooled.bin", body, "application/octet-stream")
		Expect(err).NotTo(HaveOccurred())
		Expect(obj.Size).To(Equal(int64(len(payload))))

		rc, gotObj, err := b.GetObject(ctx, "bucket", "large-spooled.bin")
		Expect(err).NotTo(HaveOccurred())
		got, readErr := io.ReadAll(rc)
		closeErr := rc.Close()
		Expect(readErr).NotTo(HaveOccurred())
		Expect(closeErr).NotTo(HaveOccurred())
		Expect(got).To(Equal(payload))
		Expect(gotObj.ETag).To(Equal(obj.ETag))

		_, err = os.Stat(b.ecSpoolDir())
		Expect(err).NotTo(HaveOccurred())
	})

	It("preserves user metadata when EC shard encoding is spooled", func() {
		configureEC(ECConfig{DataShards: 2, ParityShards: 1})

		payload := bytes.Repeat([]byte("a"), 64<<10)
		body := io.LimitReader(bytes.NewReader(payload), int64(len(payload)))
		obj, err := b.PutObjectWithUserMetadata(
			ctx,
			"bucket",
			"small-meta.bin",
			body,
			"application/octet-stream",
			map[string]string{"x-amz-meta-owner": "me"},
		)
		Expect(err).NotTo(HaveOccurred())
		Expect(obj.UserMetadata).To(Equal(map[string]string{"x-amz-meta-owner": "me"}))

		gotObj, err := b.HeadObject(ctx, "bucket", "small-meta.bin")
		Expect(err).NotTo(HaveOccurred())
		Expect(gotObj.UserMetadata).To(Equal(map[string]string{"x-amz-meta-owner": "me"}))
	})

	It("rejects stale PutObjectMeta expected ETag updates", func() {
		putMeta := func(etag string, ecData, ecParity uint8, expectedETag string) error {
			GinkgoHelper()
			cmd := PutObjectMetaCmd{
				Bucket:       "bucket",
				Key:          "race.bin",
				Size:         1,
				ContentType:  "application/octet-stream",
				ETag:         etag,
				ModTime:      1,
				ECData:       ecData,
				ECParity:     ecParity,
				ExpectedETag: expectedETag,
			}
			return b.fsm.db.Update(func(txn MetadataTxn) error {
				if err := b.fsm.checkPutObjectExpectedETag(txn, cmd.Bucket, cmd.Key, cmd.ExpectedETag); err != nil {
					return err
				}
				return b.fsm.persistPutObjectMetaUpdate(txn, cmd, buildPutObjectMeta(cmd))
			})
		}

		Expect(putMeta("old", 0, 0, "")).To(Succeed())
		Expect(putMeta("new", 0, 0, "")).To(Succeed())
		Expect(putMeta("old", 2, 1, "old")).To(HaveOccurred())

		obj, placementMeta, err := b.headObjectMeta(ctx, "bucket", "race.bin")
		Expect(err).NotTo(HaveOccurred())
		Expect(obj.ETag).To(Equal("new"))
		Expect(placementMeta.ECData).To(Equal(uint8(0)))
		Expect(placementMeta.ECParity).To(Equal(uint8(0)))
	})

	DescribeTable("cleans written shards when EC commit aborts before metadata",
		func(cfg ECConfig) {
			configureEC(cfg)

			payload := bytes.Repeat([]byte("abort-before-commit-"), 1024)
			sp, err := b.spoolPutObject(ctx, "bucket", bytes.NewReader(payload))
			Expect(err).NotTo(HaveOccurred())
			defer sp.Cleanup()

			errChanged := errors.New("metadata changed")
			_, err = b.putObjectECSpooledWithOptionalModTime(
				ctx,
				"bucket",
				"abort.bin",
				"",
				sp,
				"application/octet-stream",
				nil,
				"",
				0,
				1,
				true,
				"",
				func() error { return errChanged },
				nil,
				nil,
				"",
			)
			Expect(err).To(MatchError(errChanged))
			_, err = b.shardSvc.ReadLocalShard("bucket", "abort.bin", 0)
			Expect(os.IsNotExist(err)).To(BeTrue())
		},
		Entry("parity", ECConfig{DataShards: 2, ParityShards: 1}),
		Entry("single-local", ECConfig{DataShards: 1, ParityShards: 0}),
	)
})
