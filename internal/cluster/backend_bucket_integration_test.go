package cluster

import (
	"bytes"
	"context"
	"errors"
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
		db  *badger.DB
		ctx context.Context
	)

	BeforeEach(func() {
		b, db = newTestDistributedBackendWithDB(GinkgoT())
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

		// After delete, GetBucketPolicy returns ErrBucketNotFound — matching the
		// old BadgerDB semantics where the policy key was absent from the store.
		// The CompiledPolicyStore loader maps this to "no enforceable policy"
		// (allow-all) and the S3 handler maps it to 404 "NoSuchBucketPolicy".
		Expect(b.DeleteBucketPolicy("policy-bucket")).To(Succeed())
		got, err = b.GetBucketPolicy("policy-bucket")
		Expect(errors.Is(err, storage.ErrBucketNotFound)).To(BeTrue(), "no-policy bucket must return ErrBucketNotFound")
		Expect(got).To(BeNil())
	})

	It("decrypts encrypted bucket policy FSM values", func() {
		policy := []byte(`{"Version":"2012-10-17","Statement":[{"Resource":"secret-policy-resource"}]}`)
		clusterID := bytes.Repeat([]byte{0x48}, 16)
		keeper, err := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0x48}, encrypt.KEKSize), clusterID)
		Expect(err).NotTo(HaveOccurred())
		b.fsm.SetDEKKeeper(keeper, clusterID)

		Expect(b.CreateBucket(ctx, "policy-bucket")).To(Succeed())
		Expect(b.SetBucketPolicy("policy-bucket", policy)).To(Succeed())

		Expect(db.View(func(txn *badger.Txn) error {
			item, err := txn.Get(b.ks().BucketPolicyKey("policy-bucket"))
			if err != nil {
				return err
			}
			raw, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			_, _, ok, err := decodeFSMValueFrameV2(raw)
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeTrue())
			Expect(string(raw)).NotTo(ContainSubstring("secret-policy-resource"))
			return nil
		})).To(Succeed())

		got, err := b.GetBucketPolicy("policy-bucket")
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(policy))
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
		Skip("Phase 3: bucket empty check reads object index, misses quorum meta objects")

		Expect(b.CreateBucket(ctx, "notempty")).To(Succeed())
		_, err := b.PutObject(ctx, "notempty", "file.txt", strings.NewReader("data"), "text/plain")
		Expect(err).NotTo(HaveOccurred())

		err = b.DeleteBucket(ctx, "notempty")
		Expect(errors.Is(err, storage.ErrBucketNotEmpty)).To(BeTrue())
	})
})
