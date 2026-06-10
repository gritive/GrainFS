package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/storage"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Backend object API integration", func() {
	var (
		b   *DistributedBackend
		ctx context.Context
	)

	BeforeEach(func() {
		b = newTestDistributedBackend(GinkgoT())
		ctx = context.Background()
		Expect(b.CreateBucket(ctx, "bucket")).To(Succeed())
	})

	It("reads from prepared EC placement when metadata is no longer stored", func() {
		up, err := b.CreateMultipartUpload(ctx, "bucket", "mp.bin", "application/octet-stream")
		Expect(err).NotTo(HaveOccurred())
		payload := bytes.Repeat([]byte("x"), 64<<10)
		part, err := b.UploadPart(ctx, "bucket", "mp.bin", up.UploadID, 1, bytes.NewReader(payload))
		Expect(err).NotTo(HaveOccurred())
		_, err = b.CompleteMultipartUpload(ctx, "bucket", "mp.bin", up.UploadID, []storage.Part{*part})
		Expect(err).NotTo(HaveOccurred())

		obj, err := b.HeadObject(ctx, "bucket", "mp.bin")
		Expect(err).NotTo(HaveOccurred())
		Expect(obj.ECData).To(Equal(uint8(1)))
		Expect(obj.ECParity).To(Equal(uint8(0)))
		Expect(obj.NodeIDs).To(Equal([]string{b.selfAddr}))

		Expect(b.db.Update(func(txn *badger.Txn) error {
			if err := txn.Delete(b.ks().ObjectMetaKey("bucket", "mp.bin")); err != nil && err != badger.ErrKeyNotFound {
				return err
			}
			if err := txn.Delete(b.ks().ObjectMetaKeyV("bucket", "mp.bin", obj.VersionID)); err != nil && err != badger.ErrKeyNotFound {
				return err
			}
			if err := txn.Delete(b.ks().LatestKey("bucket", "mp.bin")); err != nil && err != badger.ErrKeyNotFound {
				return err
			}
			return nil
		})).To(Succeed())
		_, err = b.HeadObject(ctx, "bucket", "mp.bin")
		Expect(err).To(MatchError(storage.ErrObjectNotFound))

		buf := make([]byte, len(payload))
		n, err := b.ReadAtObject(ctx, "bucket", "mp.bin", obj, 0, buf)
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(len(payload)))
		Expect(buf).To(Equal(payload))
	})

	It("returns bucket-not-found when putting to missing buckets", func() {
		_, err := b.PutObject(ctx, "nope", "file.txt", strings.NewReader("data"), "text/plain")
		Expect(err).To(MatchError(storage.ErrBucketNotFound))
	})

	It("heads objects", func() {
		_, err := b.PutObject(ctx, "bucket", "meta.txt", strings.NewReader("metadata"), "text/plain")
		Expect(err).NotTo(HaveOccurred())

		obj, err := b.HeadObject(ctx, "bucket", "meta.txt")
		Expect(err).NotTo(HaveOccurred())
		Expect(obj.Size).To(Equal(int64(8)))
		Expect(obj.Key).To(Equal("meta.txt"))
	})

	It("returns object-not-found when heading missing objects", func() {
		_, err := b.HeadObject(ctx, "bucket", "nope.txt")
		Expect(err).To(MatchError(storage.ErrObjectNotFound))
	})

	It("deletes objects", func() {
		_, err := b.PutObject(ctx, "bucket", "del.txt", strings.NewReader("data"), "text/plain")
		Expect(err).NotTo(HaveOccurred())

		Expect(b.DeleteObject(ctx, "bucket", "del.txt")).To(Succeed())

		_, err = b.HeadObject(ctx, "bucket", "del.txt")
		Expect(err).To(MatchError(storage.ErrObjectNotFound))
	})

	It("returns bucket-not-found when deleting from missing buckets", func() {
		Expect(b.DeleteObject(ctx, "nope", "file.txt")).To(MatchError(storage.ErrBucketNotFound))
	})

	It("lists objects by prefix", func() {
		putObjectFixtures(b, ctx, []struct{ key, val string }{
			{"docs/a.txt", "a"},
			{"docs/b.txt", "b"},
			{"images/c.png", "c"},
		})

		objects, err := b.ListObjects(ctx, "bucket", "", 100)
		Expect(err).NotTo(HaveOccurred())
		Expect(objects).To(HaveLen(3))

		objects, err = b.ListObjects(ctx, "bucket", "docs/", 100)
		Expect(err).NotTo(HaveOccurred())
		Expect(objects).To(HaveLen(2))
	})

	It("limits listed objects", func() {
		for i := range 5 {
			_, err := b.PutObject(ctx, "bucket", fmt.Sprintf("file%d.txt", i), strings.NewReader("x"), "text/plain")
			Expect(err).NotTo(HaveOccurred())
		}

		objects, err := b.ListObjects(ctx, "bucket", "", 3)
		Expect(err).NotTo(HaveOccurred())
		Expect(objects).To(HaveLen(3))
	})

	It("returns bucket-not-found when listing missing buckets", func() {
		_, err := b.ListObjects(ctx, "nope", "", 100)
		Expect(err).To(MatchError(storage.ErrBucketNotFound))
	})

	It("walks objects by prefix", func() {
		putObjectFixtures(b, ctx, []struct{ key, val string }{
			{"docs/a.txt", "a"},
			{"docs/b.txt", "b"},
			{"images/c.png", "c"},
		})

		var all []*storage.Object
		err := b.WalkObjects(ctx, "bucket", "", func(obj *storage.Object) error {
			all = append(all, obj)
			return nil
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(all).To(HaveLen(3))

		var docs []*storage.Object
		err = b.WalkObjects(ctx, "bucket", "docs/", func(obj *storage.Object) error {
			docs = append(docs, obj)
			return nil
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(docs).To(HaveLen(2))
	})

	It("returns bucket-not-found when walking missing buckets", func() {
		err := b.WalkObjects(ctx, "nope", "", func(*storage.Object) error { return nil })
		Expect(err).To(MatchError(storage.ErrBucketNotFound))
	})

	It("skips deleted objects while walking", func() {
		_, err := b.PutObject(ctx, "bucket", "keep.txt", strings.NewReader("keep"), "text/plain")
		Expect(err).NotTo(HaveOccurred())
		_, err = b.PutObject(ctx, "bucket", "gone.txt", strings.NewReader("gone"), "text/plain")
		Expect(err).NotTo(HaveOccurred())
		Expect(b.DeleteObject(ctx, "bucket", "gone.txt")).To(Succeed())

		var keys []string
		err = b.WalkObjects(ctx, "bucket", "", func(obj *storage.Object) error {
			keys = append(keys, obj.Key)
			return nil
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(keys).To(Equal([]string{"keep.txt"}))
	})

	It("emits only latest versions while walking", func() {
		_, err := b.PutObject(ctx, "bucket", "f.txt", strings.NewReader("v1"), "text/plain")
		Expect(err).NotTo(HaveOccurred())
		_, err = b.PutObject(ctx, "bucket", "f.txt", strings.NewReader("v2-longer"), "text/plain")
		Expect(err).NotTo(HaveOccurred())

		var objs []*storage.Object
		err = b.WalkObjects(ctx, "bucket", "", func(obj *storage.Object) error {
			objs = append(objs, obj)
			return nil
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(objs).To(HaveLen(1))
		Expect(objs[0].Size).To(Equal(int64(9)))
	})

	It("stops object walks early when callbacks return errors", func() {
		for i := range 5 {
			_, err := b.PutObject(ctx, "bucket", fmt.Sprintf("f%d.txt", i), strings.NewReader("x"), "text/plain")
			Expect(err).NotTo(HaveOccurred())
		}

		sentinel := fmt.Errorf("stop")
		count := 0
		err := b.WalkObjects(ctx, "bucket", "", func(*storage.Object) error {
			count++
			if count == 2 {
				return sentinel
			}
			return nil
		})
		Expect(err).To(MatchError(sentinel))
		Expect(count).To(Equal(2))
	})

	It("overwrites objects", func() {
		_, err := b.PutObject(ctx, "bucket", "file.txt", strings.NewReader("v1"), "text/plain")
		Expect(err).NotTo(HaveOccurred())

		_, err = b.PutObject(ctx, "bucket", "file.txt", strings.NewReader("version2"), "text/plain")
		Expect(err).NotTo(HaveOccurred())

		rc, obj, err := b.GetObject(ctx, "bucket", "file.txt")
		Expect(err).NotTo(HaveOccurred())
		data, readErr := io.ReadAll(rc)
		closeErr := rc.Close()
		Expect(readErr).NotTo(HaveOccurred())
		Expect(closeErr).NotTo(HaveOccurred())
		Expect(string(data)).To(Equal("version2"))
		Expect(obj.Size).To(Equal(int64(8)))
	})

	It("reads nested keys", func() {
		_, err := b.PutObject(ctx, "bucket", "a/b/c/deep.txt", strings.NewReader("deep"), "text/plain")
		Expect(err).NotTo(HaveOccurred())

		rc, _, err := b.GetObject(ctx, "bucket", "a/b/c/deep.txt")
		Expect(err).NotTo(HaveOccurred())
		data, readErr := io.ReadAll(rc)
		closeErr := rc.Close()
		Expect(readErr).NotTo(HaveOccurred())
		Expect(closeErr).NotTo(HaveOccurred())
		Expect(string(data)).To(Equal("deep"))
	})
})

func putObjectFixtures(b *DistributedBackend, ctx context.Context, fixtures []struct{ key, val string }) {
	GinkgoHelper()
	for _, kv := range fixtures {
		_, err := b.PutObject(ctx, "bucket", kv.key, strings.NewReader(kv.val), "text/plain")
		Expect(err).NotTo(HaveOccurred())
	}
}
