package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/storage"
)

// writeAppendableObjectMeta seeds the FSM with an appendable object that has
// the given segments.  Mirrors writeVersionedObjectMeta in scrubbable_test.go
// but sets IsAppendable=true and populates Segments.
func writeAppendableObjectMeta(t *testing.T, b *DistributedBackend, bucket, key, versionID string, segs []storage.SegmentRef) {
	t.Helper()
	meta, err := marshalObjectMeta(objectMeta{
		Key:          key,
		ContentType:  "application/octet-stream",
		ETag:         "appendable-etag",
		LastModified: time.Now().Unix(),
		IsAppendable: true,
		Segments:     segs,
	})
	require.NoError(t, err)
	require.NoError(t, b.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(objectMetaKey(bucket, key), meta); err != nil {
			return err
		}
		if err := txn.Set(objectMetaKeyV(bucket, key, versionID), meta); err != nil {
			return err
		}
		return txn.Set(latestKey(bucket, key), []byte(versionID))
	}))
}

func TestScanAppendableObjects_YieldsSegmentIDs(t *testing.T) {
	// Fixture: newTestDistributedBackend (from backend_test.go)
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "test-bucket"))

	// Object "key" with 3 segments, IsAppendable=true
	segs := []storage.SegmentRef{
		{BlobID: "blob1"},
		{BlobID: "blob2"},
		{BlobID: "blob3"},
	}
	writeAppendableObjectMeta(t, b, "test-bucket", "key", "01VID-APP", segs)

	// Non-appendable object — must NOT appear in results.
	writeVersionedObjectMeta(t, b, "test-bucket", "plain-key", "01VID-PLAIN", "etag-plain", 42)

	ch, err := b.ScanAppendableObjects("test-bucket")
	require.NoError(t, err)

	var collected []scrubber.AppendableRecord
	for rec := range ch {
		collected = append(collected, rec)
	}

	require.Len(t, collected, 1, "only appendable objects must appear")
	rec := collected[0]
	assert.Equal(t, "test-bucket", rec.Bucket)
	assert.Equal(t, "key", rec.Key)
	assert.ElementsMatch(t, []string{"blob1", "blob2", "blob3"}, rec.SegmentBlobIDs)
}

func TestScanAppendableObjects_MissingBucket(t *testing.T) {
	b := newTestDistributedBackend(t)

	_, err := b.ScanAppendableObjects("no-such-bucket")
	assert.Error(t, err)
}

func TestScanAppendableObjects_EmptyBucket(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "empty"))

	ch, err := b.ScanAppendableObjects("empty")
	require.NoError(t, err)

	var recs []scrubber.AppendableRecord
	for rec := range ch {
		recs = append(recs, rec)
	}
	assert.Empty(t, recs)
}

func TestScanAppendableObjects_SkipsTombstones(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "bkt"))

	// appendable live object
	segs := []storage.SegmentRef{{BlobID: "blobX"}}
	writeAppendableObjectMeta(t, b, "bkt", "live", "01A", segs)
	// tombstone (deleteMarkerETag)
	writeVersionedObjectMeta(t, b, "bkt", "dead", "01D", deleteMarkerETag, 0)

	ch, err := b.ScanAppendableObjects("bkt")
	require.NoError(t, err)

	var recs []scrubber.AppendableRecord
	for rec := range ch {
		recs = append(recs, rec)
	}
	require.Len(t, recs, 1)
	assert.Equal(t, "live", recs[0].Key)
}

// Compile-time assertion: DistributedBackend satisfies AppendableScannable.
var _ scrubber.AppendableScannable = (*DistributedBackend)(nil)
