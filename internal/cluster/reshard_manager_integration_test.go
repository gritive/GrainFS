package cluster

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"os"
	"path/filepath"

	"github.com/gritive/GrainFS/internal/storage"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Reshard manager integration", func() {
	var (
		b   *DistributedBackend
		ctx context.Context
	)

	BeforeEach(func() {
		b = newTestDistributedBackend(GinkgoT())
		ctx = context.Background()
		Expect(b.CreateBucket(ctx, "bucket")).To(Succeed())
	})

	It("preserves tags when converting an object to EC", func() {
		payload := bytes.Repeat([]byte("convert-tags-"), 4096)
		key := "tagged-legacy.bin"
		path := b.objectPath("bucket", key)
		Expect(os.MkdirAll(filepath.Dir(path), 0o755)).To(Succeed())
		Expect(os.WriteFile(path, payload, 0o600)).To(Succeed())

		seededTags := []storage.Tag{
			{Key: "env", Value: "prod"},
			{Key: "owner", Value: "alice"},
		}
		sum := md5.Sum(payload)
		raw, err := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
			Bucket:      "bucket",
			Key:         key,
			VersionID:   "",
			Size:        int64(len(payload)),
			ContentType: "application/octet-stream",
			ETag:        hex.EncodeToString(sum[:]),
			ModTime:     1,
			Tags:        seededTags,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(b.fsm.Apply(raw)).To(Succeed())

		preTags, err := b.GetObjectTags("bucket", key, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(preTags).To(HaveLen(2))

		b.SetECConfig(ECConfig{DataShards: 2, ParityShards: 1})
		enc := testEncryptor(GinkgoT())
		b.SetShardService(NewShardService(b.root, nil, WithEncryptor(enc), withTestWALEnc(GinkgoT(), enc)), []string{b.selfAddr, b.selfAddr, b.selfAddr})

		Expect(b.ConvertObjectToEC(ctx, "bucket", key)).To(Succeed())

		postTags, err := b.GetObjectTags("bucket", key, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(postTags).To(ConsistOf(seededTags))
	})
})
