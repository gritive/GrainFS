package cluster

import (
	"context"
	"time"

	"github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/storage"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// Compile-time assertion: DistributedBackend satisfies AppendableScannable.
var _ scrubber.AppendableScannable = (*DistributedBackend)(nil)

var _ = Describe("Appendable object scan integration", func() {
	var (
		b   *DistributedBackend
		ctx context.Context
	)

	BeforeEach(func() {
		b = newTestDistributedBackend(GinkgoT())
		ctx = context.Background()
	})

	writeAppendableMeta := func(bucket, key, versionID string, segs []storage.SegmentRef) {
		GinkgoHelper()
		meta, err := marshalObjectMeta(objectMeta{
			Key:          key,
			ContentType:  "application/octet-stream",
			ETag:         "appendable-etag",
			LastModified: time.Now().Unix(),
			IsAppendable: true,
			Segments:     segs,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(b.db.Update(func(txn *badger.Txn) error {
			if err := txn.Set(objectMetaKey(bucket, key), meta); err != nil {
				return err
			}
			if err := txn.Set(objectMetaKeyV(bucket, key, versionID), meta); err != nil {
				return err
			}
			return txn.Set(latestKey(bucket, key), []byte(versionID))
		})).To(Succeed())
	}

	writePlainMeta := func(bucket, key, versionID, etag string, size int64) {
		GinkgoHelper()
		meta, err := marshalObjectMeta(objectMeta{
			Key:          key,
			Size:         size,
			ContentType:  "application/octet-stream",
			ETag:         etag,
			LastModified: time.Now().Unix(),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(b.db.Update(func(txn *badger.Txn) error {
			if err := txn.Set(objectMetaKey(bucket, key), meta); err != nil {
				return err
			}
			if err := txn.Set(objectMetaKeyV(bucket, key, versionID), meta); err != nil {
				return err
			}
			return txn.Set(latestKey(bucket, key), []byte(versionID))
		})).To(Succeed())
	}

	drainAppendable := func(ch <-chan scrubber.AppendableRecord) []scrubber.AppendableRecord {
		GinkgoHelper()
		var out []scrubber.AppendableRecord
		for rec := range ch {
			out = append(out, rec)
		}
		return out
	}

	It("yields segment IDs for live appendable objects only", func() {
		Expect(b.CreateBucket(ctx, "test-bucket")).To(Succeed())

		writeAppendableMeta("test-bucket", "key", "01VID-APP", []storage.SegmentRef{
			{BlobID: "blob1"},
			{BlobID: "blob2"},
			{BlobID: "blob3"},
		})
		writePlainMeta("test-bucket", "plain-key", "01VID-PLAIN", "etag-plain", 42)

		ch, err := b.ScanAppendableObjects("test-bucket")
		Expect(err).NotTo(HaveOccurred())

		collected := drainAppendable(ch)
		Expect(collected).To(HaveLen(1))
		Expect(collected[0].Bucket).To(Equal("test-bucket"))
		Expect(collected[0].Key).To(Equal("key"))
		Expect(collected[0].SegmentBlobIDs).To(ConsistOf("blob1", "blob2", "blob3"))
	})

	It("returns an error for a missing bucket", func() {
		_, err := b.ScanAppendableObjects("no-such-bucket")
		Expect(err).To(HaveOccurred())
	})

	It("returns no records for an empty bucket", func() {
		Expect(b.CreateBucket(ctx, "empty")).To(Succeed())

		ch, err := b.ScanAppendableObjects("empty")
		Expect(err).NotTo(HaveOccurred())
		Expect(drainAppendable(ch)).To(BeEmpty())
	})

	It("skips appendable tombstones", func() {
		Expect(b.CreateBucket(ctx, "bkt")).To(Succeed())

		writeAppendableMeta("bkt", "live", "01A", []storage.SegmentRef{{BlobID: "blobX"}})
		writePlainMeta("bkt", "dead", "01D", deleteMarkerETag, 0)

		ch, err := b.ScanAppendableObjects("bkt")
		Expect(err).NotTo(HaveOccurred())

		recs := drainAppendable(ch)
		Expect(recs).To(HaveLen(1))
		Expect(recs[0].Key).To(Equal("live"))
	})
})
