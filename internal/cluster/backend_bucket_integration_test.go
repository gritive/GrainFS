package cluster

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"

	"github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Backend bucket integration", func() {
	var (
		b   *DistributedBackend
		ctx context.Context
	)

	BeforeEach(func() {
		b = newTestDistributedBackend(GinkgoT())
		ctx = context.Background()
	})

	It("creates and heads buckets", func() {
		Expect(b.CreateBucket(ctx, "test")).To(Succeed())
		Expect(b.HeadBucket(ctx, "test")).To(Succeed())

		err := b.HeadBucket(ctx, "nope")
		Expect(errors.Is(err, storage.ErrBucketNotFound)).To(BeTrue())
	})

	It("sets, gets, and deletes bucket policies", func() {
		policy := []byte(`{"Version":"2012-10-17","Statement":[]}`)

		Expect(b.CreateBucket(ctx, "policy-bucket")).To(Succeed())
		Expect(b.SetBucketPolicy("policy-bucket", policy)).To(Succeed())

		got, err := b.GetBucketPolicy("policy-bucket")
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(policy))

		Expect(b.DeleteBucketPolicy("policy-bucket")).To(Succeed())
		_, err = b.GetBucketPolicy("policy-bucket")
		Expect(errors.Is(err, storage.ErrBucketNotFound)).To(BeTrue())
	})

	It("decrypts encrypted bucket policy FSM values", func() {
		policy := []byte(`{"Version":"2012-10-17","Statement":[{"Resource":"secret-policy-resource"}]}`)
		enc, err := encrypt.NewEncryptor(bytes.Repeat([]byte{0x48}, 32))
		Expect(err).NotTo(HaveOccurred())
		b.fsm.SetEncryptor(enc)

		Expect(b.CreateBucket(ctx, "policy-bucket")).To(Succeed())
		Expect(b.SetBucketPolicy("policy-bucket", policy)).To(Succeed())

		Expect(b.db.View(func(txn *badger.Txn) error {
			item, err := txn.Get(b.ks().BucketPolicyKey("policy-bucket"))
			if err != nil {
				return err
			}
			raw, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			Expect(encrypt.IsEncryptedValue(raw)).To(BeTrue())
			Expect(string(raw)).NotTo(ContainSubstring("secret-policy-resource"))
			return nil
		})).To(Succeed())

		got, err := b.GetBucketPolicy("policy-bucket")
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(policy))
	})

	It("round-trips internal bucket objects via PutObject/GetObject", func() {
		// The plain-file WriteAt/Truncate fast-path has been removed. Internal bucket
		// objects now go through the encrypted PutObject path on all backends.
		Expect(b.CreateBucket(ctx, "__grainfs_vfs_default")).To(Succeed())
		_, err := b.PutObject(ctx, "__grainfs_vfs_default", "dir/file.bin",
			bytes.NewReader([]byte("0123456789")), "application/octet-stream")
		Expect(err).NotTo(HaveOccurred())

		obj, err := b.HeadObject(ctx, "__grainfs_vfs_default", "dir/file.bin")
		Expect(err).NotTo(HaveOccurred())
		Expect(obj.Size).To(Equal(int64(10)))

		body, _, err := b.GetObject(ctx, "__grainfs_vfs_default", "dir/file.bin")
		Expect(err).NotTo(HaveOccurred())
		got, readErr := io.ReadAll(body)
		closeErr := body.Close()
		Expect(readErr).NotTo(HaveOccurred())
		Expect(closeErr).NotTo(HaveOccurred())
		Expect(string(got)).To(Equal("0123456789"))
	})

	It("round-trips internal bucket object metadata across delete and rewrite", func() {
		// The plain-file WriteAt fast-path has been removed. Internal bucket objects
		// now go through the encrypted PutObject path on all backends.
		Expect(b.CreateBucket(ctx, "__grainfs_vfs_default")).To(Succeed())
		_, err := b.PutObject(ctx, "__grainfs_vfs_default", "dir/file.bin",
			bytes.NewReader([]byte("old")), "application/octet-stream")
		Expect(err).NotTo(HaveOccurred())
		Expect(b.DeleteObject(ctx, "__grainfs_vfs_default", "dir/file.bin")).To(Succeed())
		_, err = b.PutObject(ctx, "__grainfs_vfs_default", "dir/file.bin",
			bytes.NewReader([]byte("new")), "application/octet-stream")
		Expect(err).NotTo(HaveOccurred())

		obj, err := b.HeadObject(ctx, "__grainfs_vfs_default", "dir/file.bin")
		Expect(err).NotTo(HaveOccurred())
		Expect(obj.Size).To(Equal(int64(3)))
	})

	It("rejects duplicate bucket creation", func() {
		Expect(b.CreateBucket(ctx, "dup")).To(Succeed())
		err := b.CreateBucket(ctx, "dup")
		Expect(errors.Is(err, storage.ErrBucketAlreadyExists)).To(BeTrue())
	})

	It("lists buckets", func() {
		Expect(b.CreateBucket(ctx, "alpha")).To(Succeed())
		Expect(b.CreateBucket(ctx, "beta")).To(Succeed())

		buckets, err := b.ListBuckets(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(buckets).To(Equal([]string{"alpha", "beta"}))
	})

	It("deletes buckets", func() {
		Expect(b.CreateBucket(ctx, "del")).To(Succeed())
		Expect(b.DeleteBucket(ctx, "del")).To(Succeed())
		err := b.HeadBucket(ctx, "del")
		Expect(errors.Is(err, storage.ErrBucketNotFound)).To(BeTrue())
	})

	It("returns not found when deleting a missing bucket", func() {
		err := b.DeleteBucket(ctx, "nope")
		Expect(errors.Is(err, storage.ErrBucketNotFound)).To(BeTrue())
	})

	It("rejects deleting non-empty buckets", func() {
		Expect(b.CreateBucket(ctx, "notempty")).To(Succeed())
		_, err := b.PutObject(ctx, "notempty", "file.txt", strings.NewReader("data"), "text/plain")
		Expect(err).NotTo(HaveOccurred())

		err = b.DeleteBucket(ctx, "notempty")
		Expect(errors.Is(err, storage.ErrBucketNotEmpty)).To(BeTrue())
	})
})
