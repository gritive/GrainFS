package cluster

import (
	"context"
	"io"
	"strings"

	"github.com/gritive/GrainFS/internal/storage"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Object versioning integration", func() {
	var (
		b      *DistributedBackend
		ctx    context.Context
		bucket string
	)

	BeforeEach(func() {
		b = newTestDistributedBackend(GinkgoT())
		ctx = context.Background()
		bucket = "vbucket"
		Expect(b.CreateBucket(ctx, bucket)).To(Succeed())
	})

	readAll := func(rc io.ReadCloser) string {
		GinkgoHelper()
		data, err := io.ReadAll(rc)
		Expect(err).NotTo(HaveOccurred())
		Expect(rc.Close()).To(Succeed())
		return string(data)
	}

	It("reads a PUT object and returns its VersionID", func() {
		obj, err := b.PutObject(ctx, bucket, "k", strings.NewReader("v1"), "text/plain")
		Expect(err).NotTo(HaveOccurred())
		Expect(obj.VersionID).NotTo(BeEmpty())

		rc, got, err := b.GetObject(ctx, bucket, "k")
		Expect(err).NotTo(HaveOccurred())
		Expect(readAll(rc)).To(Equal("v1"))
		Expect(got.VersionID).To(Equal(obj.VersionID))
	})

	It("lists multiple versions newest first", func() {
		o1, err := b.PutObject(ctx, bucket, "k", strings.NewReader("v1"), "text/plain")
		Expect(err).NotTo(HaveOccurred())
		o2, err := b.PutObject(ctx, bucket, "k", strings.NewReader("v2-longer"), "text/plain")
		Expect(err).NotTo(HaveOccurred())

		versions, err := b.ListObjectVersions(bucket, "k", 0)
		Expect(err).NotTo(HaveOccurred())
		Expect(versions).To(HaveLen(2))
		Expect(versions[0].VersionID).To(Equal(o2.VersionID))
		Expect(versions[0].IsLatest).To(BeTrue())
		Expect(versions[1].VersionID).To(Equal(o1.VersionID))
		Expect(versions[1].IsLatest).To(BeFalse())
		Expect(versions[0].IsDeleteMarker).To(BeFalse())

		rc, _, err := b.GetObject(ctx, bucket, "k")
		Expect(err).NotTo(HaveOccurred())
		Expect(readAll(rc)).To(Equal("v2-longer"))
	})

	It("creates delete markers while keeping prior versions addressable", func() {
		_, err := b.PutObject(ctx, bucket, "k", strings.NewReader("v1"), "text/plain")
		Expect(err).NotTo(HaveOccurred())
		Expect(b.DeleteObject(ctx, bucket, "k")).To(Succeed())

		_, err = b.HeadObject(ctx, bucket, "k")
		Expect(err).To(MatchError(storage.ErrObjectNotFound))

		versions, err := b.ListObjectVersions(bucket, "k", 0)
		Expect(err).NotTo(HaveOccurred())
		Expect(versions).To(HaveLen(2))
		Expect(versions[0].IsDeleteMarker).To(BeTrue())
		Expect(versions[0].IsLatest).To(BeTrue())
		Expect(versions[1].IsDeleteMarker).To(BeFalse())

		rc, got, err := b.GetObjectVersion(bucket, "k", versions[1].VersionID)
		Expect(err).NotTo(HaveOccurred())
		Expect(readAll(rc)).To(Equal("v1"))
		Expect(got.VersionID).To(Equal(versions[1].VersionID))
	})

	It("returns not found for an unknown version", func() {
		_, err := b.PutObject(ctx, bucket, "k", strings.NewReader("v1"), "text/plain")
		Expect(err).NotTo(HaveOccurred())

		_, _, err = b.GetObjectVersion(bucket, "k", "01ABCDEFGHIJKLMNOPQRSTUVWX")
		Expect(err).To(MatchError(storage.ErrObjectNotFound))
	})

	It("hard-deletes a specific object version and updates latest", func() {
		o1, err := b.PutObject(ctx, bucket, "k", strings.NewReader("v1"), "text/plain")
		Expect(err).NotTo(HaveOccurred())
		o2, err := b.PutObject(ctx, bucket, "k", strings.NewReader("v2"), "text/plain")
		Expect(err).NotTo(HaveOccurred())

		Expect(b.DeleteObjectVersion(bucket, "k", o2.VersionID)).To(Succeed())

		versions, err := b.ListObjectVersions(bucket, "k", 0)
		Expect(err).NotTo(HaveOccurred())
		Expect(versions).To(HaveLen(1))
		Expect(versions[0].VersionID).To(Equal(o1.VersionID))
		Expect(versions[0].IsLatest).To(BeTrue())

		_, err = b.HeadObjectVersion(bucket, "k", o2.VersionID)
		Expect(err).To(MatchError(storage.ErrObjectNotFound))
	})

	It("deduplicates keys when listing latest objects", func() {
		_, err := b.PutObject(ctx, bucket, "k", strings.NewReader("v1"), "text/plain")
		Expect(err).NotTo(HaveOccurred())
		_, err = b.PutObject(ctx, bucket, "k", strings.NewReader("v2"), "text/plain")
		Expect(err).NotTo(HaveOccurred())
		_, err = b.PutObject(ctx, bucket, "other", strings.NewReader("o"), "text/plain")
		Expect(err).NotTo(HaveOccurred())

		objs, err := b.ListObjects(ctx, bucket, "", 100)
		Expect(err).NotTo(HaveOccurred())
		Expect(objs).To(HaveLen(2))
	})

	It("excludes tombstoned keys from latest object listing", func() {
		_, err := b.PutObject(ctx, bucket, "k", strings.NewReader("v1"), "text/plain")
		Expect(err).NotTo(HaveOccurred())
		_, err = b.PutObject(ctx, bucket, "kept", strings.NewReader("k"), "text/plain")
		Expect(err).NotTo(HaveOccurred())
		Expect(b.DeleteObject(ctx, bucket, "k")).To(Succeed())

		objs, err := b.ListObjects(ctx, bucket, "", 100)
		Expect(err).NotTo(HaveOccurred())
		Expect(objs).To(HaveLen(1))
		Expect(objs[0].Key).To(Equal("kept"))
	})

	It("returns tags from the versioned HEAD path", func() {
		tagBucket := "vtagbkt"
		Expect(b.CreateBucket(ctx, tagBucket)).To(Succeed())

		obj, err := b.PutObject(ctx, tagBucket, "k", strings.NewReader("v1"), "text/plain")
		Expect(err).NotTo(HaveOccurred())
		Expect(obj.VersionID).NotTo(BeEmpty())

		tags := []storage.Tag{
			{Key: "env", Value: "staging"},
			{Key: "owner", Value: "bob"},
		}
		Expect(b.SetObjectTags(tagBucket, "k", obj.VersionID, tags)).To(Succeed())

		got, err := b.HeadObjectVersion(tagBucket, "k", obj.VersionID)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).NotTo(BeNil())
		Expect(got.Tags).To(Equal(tags))
	})
})
