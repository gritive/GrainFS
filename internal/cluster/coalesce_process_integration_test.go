package cluster

import (
	"bytes"
	"context"
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Coalesce process integration", func() {
	It("rejects B3 coalesce when topology placement is invalid", func() {
		t := GinkgoT()
		b := newTestDistributedBackend(t)
		ctx := context.Background()
		Expect(b.CreateBucket(ctx, "b")).To(Succeed())

		bucket, key := "b", "k"
		for _, c := range []string{"aaaa", "bbbb"} {
			_, err := b.AppendObject(ctx, bucket, key, int64(currentSize(t, b, bucket, key)), bytes.NewReader([]byte(c)))
			Expect(err).NotTo(HaveOccurred())
		}

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
