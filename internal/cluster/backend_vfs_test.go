package cluster

import (
	"bytes"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

func TestSetVFSFixedVersionEnabled_default(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.True(t, b.VFSFixedVersionEnabled(),
		"default must be true so disk-amplification fix is active out of the box")
}

func TestSetVFSFixedVersionEnabled_toggle(t *testing.T) {
	b := newTestDistributedBackend(t)
	b.SetVFSFixedVersionEnabled(false)
	require.False(t, b.VFSFixedVersionEnabled())
	b.SetVFSFixedVersionEnabled(true)
	require.True(t, b.VFSFixedVersionEnabled())
}

func TestPutObject_VFSBucket_FixedVersionID(t *testing.T) {
	b := newTestDistributedBackend(t)
	bucket := storage.VFSBucketPrefix + "vol1"
	require.NoError(t, b.CreateBucket(bucket))

	// First PUT.
	o1, err := b.PutObject(bucket, "data.bin", strings.NewReader("aaa"), "application/octet-stream")
	require.NoError(t, err)
	require.Equal(t, "current", o1.VersionID, "VFS bucket PUT must use fixed versionID 'current'")

	// Second PUT to same key.
	o2, err := b.PutObject(bucket, "data.bin", strings.NewReader("bbbbbb"), "application/octet-stream")
	require.NoError(t, err)
	require.Equal(t, "current", o2.VersionID)

	// GetObject returns latest data.
	rc, _, err := b.GetObject(bucket, "data.bin")
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	rc.Close()
	require.NoError(t, err)
	require.Equal(t, "bbbbbb", string(got))

	// ListObjectVersions returns exactly one entry.
	versions, err := b.ListObjectVersions(bucket, "", 100)
	require.NoError(t, err)
	require.Len(t, versions, 1, "VFS bucket should never accumulate multiple versions")
	require.Equal(t, "current", versions[0].VersionID)
}

func TestHeadObject_InternalBucketIgnoresLatestPointer(t *testing.T) {
	b := newTestDistributedBackend(t)
	bucket := storage.NFS4BucketName
	key := "hot.bin"
	require.NoError(t, b.CreateBucket(bucket))

	currentMeta, err := marshalObjectMeta(objectMeta{
		Key:          key,
		Size:         1,
		ContentType:  "application/octet-stream",
		LastModified: 10,
	})
	require.NoError(t, err)
	staleMeta, err := marshalObjectMeta(objectMeta{
		Key:          key,
		Size:         99,
		ContentType:  "application/octet-stream",
		LastModified: 20,
	})
	require.NoError(t, err)

	require.NoError(t, b.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(objectMetaKey(bucket, key), currentMeta); err != nil {
			return err
		}
		if err := txn.Set(objectMetaKeyV(bucket, key, "stale"), staleMeta); err != nil {
			return err
		}
		return txn.Set(latestKey(bucket, key), []byte("stale"))
	}))

	obj, err := b.HeadObject(bucket, key)
	require.NoError(t, err)
	require.Equal(t, int64(1), obj.Size)
	require.Equal(t, "current", obj.VersionID)
}

func TestWriteAt_InternalBucketDoesNotWriteLatestPointer(t *testing.T) {
	b := newTestDistributedBackend(t)
	bucket := storage.NFS4BucketName
	require.NoError(t, b.CreateBucket(bucket))

	_, err := b.WriteAt(bucket, "direct.bin", 0, []byte("data"))
	require.NoError(t, err)

	require.NoError(t, b.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(latestKey(bucket, "direct.bin"))
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
		return nil
	}))
}

func TestWriteAt_InternalBucketCachesPathAndSize(t *testing.T) {
	b := newTestDistributedBackend(t)
	bucket := storage.NFS4BucketName
	key := "direct.bin"
	cacheKey := internalObjectCacheKey{bucket: bucket, key: key}
	require.NoError(t, b.CreateBucket(bucket))

	obj, err := b.WriteAt(bucket, key, 0, []byte("abcdef"))
	require.NoError(t, err)
	require.Equal(t, int64(6), obj.Size)

	if _, ok := b.internalPathCache.Load(cacheKey); !ok {
		t.Fatalf("WriteAt should cache the internal object path")
	}
	size, ok := b.internalSizeCache.Load(cacheKey)
	require.True(t, ok, "WriteAt should cache the internal object size")
	require.Equal(t, int64(6), size)

	obj, err = b.WriteAt(bucket, key, 2, []byte("XY"))
	require.NoError(t, err)
	require.Equal(t, int64(6), obj.Size)
	size, ok = b.internalSizeCache.Load(cacheKey)
	require.True(t, ok)
	require.Equal(t, int64(6), size)

	obj, err = b.WriteAt(bucket, key, 10, []byte("z"))
	require.NoError(t, err)
	require.Equal(t, int64(11), obj.Size)
	size, ok = b.internalSizeCache.Load(cacheKey)
	require.True(t, ok)
	require.Equal(t, int64(11), size)

	require.NoError(t, b.Truncate(bucket, key, 3))
	size, ok = b.internalSizeCache.Load(cacheKey)
	require.True(t, ok)
	require.Equal(t, int64(3), size)
}

func TestPutObject_VFSBucket_DisabledTogglesLegacy(t *testing.T) {
	b := newTestDistributedBackend(t)
	b.SetVFSFixedVersionEnabled(false)
	bucket := storage.VFSBucketPrefix + "legacy"
	require.NoError(t, b.CreateBucket(bucket))

	o, err := b.PutObject(bucket, "k", strings.NewReader("x"), "application/octet-stream")
	require.NoError(t, err)
	require.NotEqual(t, "current", o.VersionID, "with toggle off, legacy ULID must be used")
}

func TestPutObject_NormalBucket_StillUsesULID(t *testing.T) {
	b := newTestDistributedBackend(t)
	bucket := "normal-bucket"
	require.NoError(t, b.CreateBucket(bucket))

	o1, err := b.PutObject(bucket, "k", strings.NewReader("a"), "application/octet-stream")
	require.NoError(t, err)
	o2, err := b.PutObject(bucket, "k", strings.NewReader("b"), "application/octet-stream")
	require.NoError(t, err)
	require.NotEqual(t, o1.VersionID, o2.VersionID, "non-VFS buckets must keep multi-version behavior")
}

func TestPutObject_VFSBucket_ConcurrentSameKeyAtomicLastWriterWins(t *testing.T) {
	b := newTestDistributedBackend(t)
	bucket := storage.VFSBucketPrefix + "concurrent"
	require.NoError(t, b.CreateBucket(bucket))

	const writers = 32
	const payloadKB = 64
	var wg sync.WaitGroup
	wg.Add(writers)
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		i := i
		go func() {
			defer wg.Done()
			payload := bytes.Repeat([]byte{byte('A' + (i % 26))}, payloadKB*1024)
			_, err := b.PutObject(bucket, "shared.bin", bytes.NewReader(payload), "application/octet-stream")
			if err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatalf("concurrent PutObject error: %v", e)
	}

	// Result must be one of the 32 payloads, no torn write.
	rc, _, err := b.GetObject(bucket, "shared.bin")
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	rc.Close()
	require.NoError(t, err)

	require.Equal(t, payloadKB*1024, len(got),
		"object size must match a complete payload (no torn write)")
	first := got[0]
	for j, by := range got {
		require.Equalf(t, first, by,
			"byte %d differs (%c vs %c) — torn write detected", j, by, first)
	}
}
