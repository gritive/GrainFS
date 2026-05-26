package cluster

import (
	"context"
	"strings"

	"github.com/gritive/GrainFS/internal/storage"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Backend tagging integration", func() {
	var (
		b   *DistributedBackend
		ctx context.Context
	)

	BeforeEach(func() {
		b = newTestDistributedBackend(GinkgoT())
		ctx = context.Background()
	})

	It("includes tags when listing object versions", func() {
		const bucket = "tagbucket"
		Expect(b.CreateBucket(ctx, bucket)).To(Succeed())

		obj, err := b.PutObject(ctx, bucket, "tagged.txt", strings.NewReader("hello"), "text/plain")
		Expect(err).NotTo(HaveOccurred())
		Expect(obj.VersionID).NotTo(BeEmpty())

		want := []storage.Tag{{Key: "env", Value: "prod"}, {Key: "owner", Value: "alice"}}
		Expect(b.SetObjectTags(bucket, "tagged.txt", "", want)).To(Succeed())

		versions, err := b.ListObjectVersions(bucket, "", 100)
		Expect(err).NotTo(HaveOccurred())
		Expect(versions).To(HaveLen(1))
		Expect(versions[0].Key).To(Equal("tagged.txt"))
		Expect(versions[0].Tags).To(Equal(want))
	})

	It("preserves tags across list object paths", func() {
		const bucket = "listtagbucket"
		Expect(b.CreateBucket(ctx, bucket)).To(Succeed())

		_, err := b.PutObject(ctx, bucket, "a.txt", strings.NewReader("hi"), "text/plain")
		Expect(err).NotTo(HaveOccurred())
		_, err = b.PutObject(ctx, bucket, "b.txt", strings.NewReader("hi"), "text/plain")
		Expect(err).NotTo(HaveOccurred())

		tagsA := []storage.Tag{{Key: "env", Value: "prod"}, {Key: "owner", Value: "alice"}}
		Expect(b.SetObjectTags(bucket, "a.txt", "", tagsA)).To(Succeed())

		objs, err := b.ListObjects(ctx, bucket, "", 100)
		Expect(err).NotTo(HaveOccurred())
		got := tagsByKey(objs)
		Expect(got["a.txt"]).To(Equal(tagsA))
		Expect(got["b.txt"]).To(BeNil())

		objs, _, err = b.ListObjectsPage(ctx, bucket, "", "", 100)
		Expect(err).NotTo(HaveOccurred())
		got = tagsByKey(objs)
		Expect(got["a.txt"]).To(Equal(tagsA))
		Expect(got["b.txt"]).To(BeNil())

		var walked []*storage.Object
		Expect(b.WalkObjects(ctx, bucket, "", func(o *storage.Object) error {
			walked = append(walked, o)
			return nil
		})).To(Succeed())
		got = tagsByKey(walked)
		Expect(got["a.txt"]).To(Equal(tagsA))
		Expect(got["b.txt"]).To(BeNil())
	})
})

// tagsByKey collapses []*storage.Object into key->Tags so tests can assert
// per-key regardless of iterator order.
func tagsByKey(objs []*storage.Object) map[string][]storage.Tag {
	out := make(map[string][]storage.Tag, len(objs))
	for _, o := range objs {
		out[o.Key] = o.Tags
	}
	return out
}
