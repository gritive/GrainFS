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

var _ = Describe("Coalesce process integration", func() {
	var (
		b           *DistributedBackend
		ctx         context.Context
		bucket, key string
	)

	BeforeEach(func() {
		b = newTestDistributedBackend(GinkgoT())
		ctx = context.Background()
		bucket, key = "b", "k"
		Expect(b.CreateBucket(ctx, "b")).To(Succeed())
	})

	appendChunks := func(chunks ...string) {
		GinkgoHelper()
		for _, c := range chunks {
			_, err := b.AppendObject(ctx, bucket, key, int64(currentSize(GinkgoT(), b, bucket, key)), bytes.NewReader([]byte(c)))
			Expect(err).NotTo(HaveOccurred())
		}
	}

	It("coalesces B2 owner-local segments end to end", func() {
		appendChunks("aaaa", "bbbb", "cc")

		pre, err := b.HeadObject(ctx, bucket, key)
		Expect(err).NotTo(HaveOccurred())
		Expect(pre.Segments).To(HaveLen(3))

		Expect(b.processCoalesceJobB2(ctx, coalesceJob{Bucket: bucket, Key: key})).To(Succeed())

		obj, err := b.HeadObject(ctx, bucket, key)
		Expect(err).NotTo(HaveOccurred())
		Expect(obj.Segments).To(BeEmpty())
		Expect(obj.Coalesced).To(HaveLen(1))

		rc, _, err := b.GetObject(ctx, bucket, key)
		Expect(err).NotTo(HaveOccurred())
		got, err := io.ReadAll(rc)
		Expect(err).NotTo(HaveOccurred())
		Expect(rc.Close()).To(Succeed())
		Expect(string(got)).To(Equal("aaaabbbbcc"))

		entries, err := os.ReadDir(b.objectPath(bucket, key) + "_segments")
		Expect(err).NotTo(HaveOccurred())
		Expect(entries).To(BeEmpty())

		coalescedPath := filepath.Join(b.objectPath(bucket, key)+"_coalesced", obj.Coalesced[0].CoalescedID)
		_, err = os.Stat(coalescedPath)
		Expect(err).NotTo(HaveOccurred())
	})

	It("rejects B3 coalesce when topology placement is invalid", func() {
		appendChunks("aaaa", "bbbb")

		ctx = ContextWithPlacementGroupEntry(ctx, ShardGroupEntry{ID: "group-1"})
		err := b.processCoalesceJobB3(ctx, coalesceJob{Bucket: bucket, Key: key})
		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, ErrPlacementTargetsUnavailable)).To(BeTrue())

		obj, err := b.HeadObject(context.Background(), bucket, key)
		Expect(err).NotTo(HaveOccurred())
		Expect(obj.Segments).To(HaveLen(2))
		Expect(obj.Coalesced).To(BeEmpty())
	})
})
