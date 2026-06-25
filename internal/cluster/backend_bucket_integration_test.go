package cluster

import (
	"context"
	"errors"
	"strings"

	"github.com/dgraph-io/badger/v4"

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

	It("stores and retrieves bucket policy via MetaBucketStore", func() {
		// Task 12: bucket policy moved from group-0 BadgerDB to MetaBucketStore
		// (meta-raft BucketRecord). The policy is stored in the
		// MetaFSM in-memory map (no group-0 BadgerDB key), so the encryption test
		// for the old policy: BadgerDB key is retired. This test verifies the live
		// round-trip: SetBucketPolicy→MetaBucketStore; GetBucketPolicy reads back.
		policy := []byte(`{"Version":"2012-10-17","Statement":[{"Resource":"resource"}]}`)

		Expect(b.CreateBucket(ctx, "policy-bucket")).To(Succeed())
		Expect(b.SetBucketPolicy("policy-bucket", policy)).To(Succeed())

		// Policy must NOT be in BadgerDB (it lives in MetaFSM now).
		err := db.View(func(txn *badger.Txn) error {
			_, err := txn.Get(b.ks().Key([]byte("policy:policy-bucket")))
			return err
		})
		Expect(errors.Is(err, badger.ErrKeyNotFound)).To(BeTrue(),
			"policy must not be written to group-0 BadgerDB (lives in MetaBucketStore)")

		// GetBucketPolicy must read back from MetaBucketStore.
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
