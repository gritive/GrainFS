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
		b.SetShardService(NewShardService(b.root, nil), nodes)
	}

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

	It("converts legacy objects to EC with spooled shard encoding", func() {
		payload := bytes.Repeat([]byte("convert-spooled-"), 4096)
		key := "legacy.bin"
		versionID := ""
		path := b.objectPath("bucket", key)
		Expect(os.MkdirAll(filepath.Dir(path), 0o755)).To(Succeed())
		Expect(os.WriteFile(path, payload, 0o600)).To(Succeed())

		raw, err := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
			Bucket:      "bucket",
			Key:         key,
			VersionID:   versionID,
			Size:        int64(len(payload)),
			ContentType: "application/octet-stream",
			ETag:        backendMD5Hex(payload),
			ModTime:     1,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(b.fsm.Apply(raw)).To(Succeed())

		configureEC(ECConfig{DataShards: 2, ParityShards: 1})

		Expect(b.ConvertObjectToEC(ctx, "bucket", key)).To(Succeed())

		_, err = os.Stat(b.ecSpoolDir())
		Expect(err).NotTo(HaveOccurred())
		_, err = os.Stat(path)
		Expect(os.IsNotExist(err)).To(BeTrue())

		rc, gotObj, err := b.GetObject(ctx, "bucket", key)
		Expect(err).NotTo(HaveOccurred())
		got, readErr := io.ReadAll(rc)
		closeErr := rc.Close()
		Expect(readErr).NotTo(HaveOccurred())
		Expect(closeErr).NotTo(HaveOccurred())
		Expect(got).To(Equal(payload))
		Expect(gotObj.VersionID).To(Equal(versionID))
		Expect(gotObj.LastModified).To(Equal(int64(1)))

		_, placementMeta, err := b.headObjectMeta(ctx, "bucket", key)
		Expect(err).NotTo(HaveOccurred())
		Expect(placementMeta.ECData).To(Equal(uint8(2)))
		Expect(placementMeta.ECParity).To(Equal(uint8(1)))
	})

	It("rejects stale PutObjectMeta expected ETag updates", func() {
		putMeta := func(etag string, ecData, ecParity uint8, expectedETag string) error {
			GinkgoHelper()
			raw, err := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
				Bucket:       "bucket",
				Key:          "race.bin",
				Size:         1,
				ContentType:  "application/octet-stream",
				ETag:         etag,
				ModTime:      1,
				ECData:       ecData,
				ECParity:     ecParity,
				ExpectedETag: expectedETag,
			})
			Expect(err).NotTo(HaveOccurred())
			return b.fsm.Apply(raw)
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
