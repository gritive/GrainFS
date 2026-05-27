package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage/datawal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestBackend(t *testing.T) *LocalBackend {
	t.Helper()
	dir := t.TempDir()
	b, err := NewLocalBackend(dir)
	require.NoError(t, err, "NewLocalBackend")
	t.Cleanup(func() { b.Close() })
	return b
}

func TestNewEncryptedLocalBackendRejectsNilEncryptor(t *testing.T) {
	_, err := NewEncryptedLocalBackend(t.TempDir(), nil)
	require.Error(t, err)
}

func TestCreateBucket(t *testing.T) {
	b := setupTestBackend(t)

	require.NoError(t, b.CreateBucket(context.Background(), "test-bucket"), "CreateBucket")

	// duplicate should fail
	require.ErrorIs(t, b.CreateBucket(context.Background(), "test-bucket"), ErrBucketAlreadyExists)
}

func TestHeadBucket(t *testing.T) {
	b := setupTestBackend(t)

	require.ErrorIs(t, b.HeadBucket(context.Background(), "nonexistent"), ErrBucketNotFound)

	b.CreateBucket(context.Background(), "test-bucket")
	require.NoError(t, b.HeadBucket(context.Background(), "test-bucket"), "HeadBucket")
}

func TestDeleteBucket(t *testing.T) {
	b := setupTestBackend(t)

	require.ErrorIs(t, b.DeleteBucket(context.Background(), "nonexistent"), ErrBucketNotFound)

	b.CreateBucket(context.Background(), "test-bucket")
	b.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader([]byte("data")), "text/plain")

	require.ErrorIs(t, b.DeleteBucket(context.Background(), "test-bucket"), ErrBucketNotEmpty)

	b.DeleteObject(context.Background(), "test-bucket", "file.txt")
	require.NoError(t, b.DeleteBucket(context.Background(), "test-bucket"), "DeleteBucket")
}

func TestListBuckets(t *testing.T) {
	b := setupTestBackend(t)

	buckets, err := b.ListBuckets(context.Background())
	require.NoError(t, err, "ListBuckets")
	require.Empty(t, buckets)

	b.CreateBucket(context.Background(), "alpha")
	b.CreateBucket(context.Background(), "bravo")

	buckets, err = b.ListBuckets(context.Background())
	require.NoError(t, err, "ListBuckets")
	require.Len(t, buckets, 2)
}

func TestLocalBackend_DEKKeeperSegEnc_RoundTrip(t *testing.T) {
	keeper, err := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0x88}, encrypt.KEKSize), bytes.Repeat([]byte{0x99}, 16))
	if err != nil {
		t.Fatalf("NewDEKKeeper: %v", err)
	}
	b, err := NewLocalBackendWithDEKKeeper(t.TempDir(), keeper, bytes.Repeat([]byte{0x99}, 16))
	if err != nil {
		t.Fatalf("NewLocalBackendWithDEKKeeper: %v", err)
	}
	defer b.Close()

	require.NoError(t, b.CreateBucket(context.Background(), "test-bucket"), "CreateBucket")

	data := []byte("hello dek")
	_, err = b.PutObject(context.Background(), "test-bucket", "greeting.txt", bytes.NewReader(data), "text/plain")
	require.NoError(t, err, "PutObject")

	rc, _, err := b.GetObject(context.Background(), "test-bucket", "greeting.txt")
	require.NoError(t, err, "GetObject")
	defer rc.Close()

	got, err := io.ReadAll(rc)
	require.NoError(t, err, "ReadAll")
	require.Equal(t, data, got)
}

func TestPutAndGetObject(t *testing.T) {
	b := setupTestBackend(t)
	b.CreateBucket(context.Background(), "test-bucket")

	data := []byte("hello grainfs")
	obj, err := b.PutObject(context.Background(), "test-bucket", "greeting.txt", bytes.NewReader(data), "text/plain")
	require.NoError(t, err, "PutObject")
	assert.Equal(t, int64(len(data)), obj.Size)
	assert.Equal(t, "text/plain", obj.ContentType)
	assert.NotEmpty(t, obj.ETag)

	rc, meta, err := b.GetObject(context.Background(), "test-bucket", "greeting.txt")
	require.NoError(t, err, "GetObject")
	defer rc.Close()

	got, _ := io.ReadAll(rc)
	assert.Equal(t, data, got)
	assert.Equal(t, int64(len(data)), meta.Size)
}

func TestPutObject_AlwaysProducesSegments(t *testing.T) {
	cases := []struct {
		name string
		size int
	}{
		{"empty", 0},
		{"small", 1024},
		{"under_chunk", 15 << 20},
		{"two_chunks", (16 << 20) + 1},
		{"large", (32 << 20) + 123},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			b := newTestLocalBackend(t)
			obj, err := b.PutObject(context.Background(), "test", "key-"+tc.name, newPatternReader(tc.size), "application/octet-stream")
			if err != nil {
				t.Fatalf("put: %v", err)
			}
			if len(obj.Segments) < 1 {
				t.Fatalf("segments must be >=1, got %d", len(obj.Segments))
			}
			if obj.Size != int64(tc.size) {
				t.Fatalf("size: want %d, got %d", tc.size, obj.Size)
			}
			rc, _, err := b.GetObject(context.Background(), "test", "key-"+tc.name)
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			if err := requireReaderEqualPattern(rc, tc.size); err != nil {
				rc.Close()
				t.Fatal(err)
			}
			rc.Close()
		})
	}
}

func TestLocalBackend_DataWALRestoresMissingSegment(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	dwal, err := datawal.Open(filepath.Join(root, "datawal"), nil)
	require.NoError(t, err)

	b, err := NewLocalBackendWithDataWAL(root, dwal)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })

	require.NoError(t, b.CreateBucket(ctx, "b"))
	payload := []byte(strings.Repeat("wal segment payload", 1024))
	obj, err := b.PutObject(ctx, "b", "k", bytes.NewReader(payload), "application/octet-stream")
	require.NoError(t, err)
	require.NotEmpty(t, obj.Segments)
	require.NoError(t, dwal.Flush())

	require.NoError(t, os.Remove(b.segmentPath("b", "k", obj.Segments[0].BlobID)))
	require.NoError(t, b.RecoverDataWAL(ctx))

	rc, _, err := b.GetObject(ctx, "b", "k")
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

func TestEncryptedLocalBackend_DataWALRestoresMissingSegment(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	enc := testEncryptor(t)
	dwal, err := datawal.Open(filepath.Join(root, "datawal"), enc)
	require.NoError(t, err)

	b, err := NewEncryptedLocalBackendWithDataWAL(root, enc, dwal)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })

	require.NoError(t, b.CreateBucket(ctx, "b"))
	payload := []byte(strings.Repeat("encrypted wal segment payload", 1024))
	obj, err := b.PutObject(ctx, "b", "k", bytes.NewReader(payload), "application/octet-stream")
	require.NoError(t, err)
	require.NotEmpty(t, obj.Segments)
	require.NoError(t, dwal.Flush())

	require.NoError(t, os.Remove(b.segmentPath("b", "k", obj.Segments[0].BlobID)))
	require.NoError(t, b.RecoverDataWAL(ctx))

	rc, _, err := b.GetObject(ctx, "b", "k")
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

func TestLocalBackend_SyncFlushesDataWAL(t *testing.T) {
	dwal := &countingDataWAL{dir: filepath.Join(t.TempDir(), "datawal")}
	b, err := NewLocalBackendWithDataWAL(t.TempDir(), dwal)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })

	require.NoError(t, b.CreateBucket(context.Background(), "b"))
	_, err = b.PutObject(context.Background(), "b", "k", strings.NewReader("payload"), "text/plain")
	require.NoError(t, err)

	before := dwal.flushes
	require.NoError(t, b.Sync("b", "k"))
	require.Equal(t, before+1, dwal.flushes)
}

type countingDataWAL struct {
	dir     string
	flushes int
}

func (w *countingDataWAL) Append(context.Context, datawal.Record) (uint64, error) {
	return 1, nil
}

func (w *countingDataWAL) AppendReader(context.Context, datawal.Record, io.Reader) (uint64, error) {
	return 1, nil
}

func (w *countingDataWAL) Flush() error {
	w.flushes++
	return nil
}

func (w *countingDataWAL) Dir() string {
	return w.dir
}

func requireReaderEqualBytes(r io.Reader, want []byte) error {
	buf := make([]byte, 32*1024)
	off := 0
	for {
		n, err := r.Read(buf)
		if n > 0 {
			if off+n > len(want) {
				return fmt.Errorf("round-trip produced too many bytes: got at least %d, want %d", off+n, len(want))
			}
			if !bytes.Equal(buf[:n], want[off:off+n]) {
				return fmt.Errorf("round-trip differs at offset %d", off+firstDiff(buf[:n], want[off:off+n]))
			}
			off += n
		}
		if err == io.EOF {
			if off != len(want) {
				return fmt.Errorf("round-trip ended early: got %d bytes, want %d", off, len(want))
			}
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func requireReaderEqualPattern(r io.Reader, size int) error {
	buf := make([]byte, 32*1024)
	off := 0
	for {
		n, err := r.Read(buf)
		if n > 0 {
			if off+n > size {
				return fmt.Errorf("round-trip produced too many bytes: got at least %d, want %d", off+n, size)
			}
			for i, got := range buf[:n] {
				if want := patternByte(off + i); got != want {
					return fmt.Errorf("round-trip differs at offset %d", off+i)
				}
			}
			off += n
		}
		if err == io.EOF {
			if off != size {
				return fmt.Errorf("round-trip ended early: got %d bytes, want %d", off, size)
			}
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func TestEncryptedLocalBackendDoesNotStorePlaintextObject(t *testing.T) {
	enc := testEncryptor(t)
	b, err := NewEncryptedLocalBackend(t.TempDir(), enc)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })

	require.NoError(t, b.CreateBucket(context.Background(), "bkt"))
	plaintext := []byte("sensitive object payload")
	key := "sensitive-meta-key"
	obj, err := b.PutObject(context.Background(), "bkt", key, bytes.NewReader(plaintext), "text/plain")
	require.NoError(t, err)

	// Plaintext now lives in segment blobs, not at objectPath. Read the
	// first segment off disk and assert plaintext is absent.
	require.NotEmpty(t, obj.Segments, "encrypted PUT must produce segments")
	raw, err := os.ReadFile(b.segmentPath("bkt", key, obj.Segments[0].BlobID))
	require.NoError(t, err)
	require.NotContains(t, string(raw), string(plaintext))

	require.NoError(t, b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(b.objectMetaKey("bkt", key))
		require.NoError(t, err)
		return item.Value(func(val []byte) error {
			require.True(t, encrypt.IsEncryptedValue(val))
			require.NotContains(t, string(val), key)
			return nil
		})
	}))

	rc, _, err := b.GetObject(context.Background(), "bkt", key)
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)
}

func TestEncryptedLocalBackendListsEncryptedObjectMetadata(t *testing.T) {
	enc := testEncryptor(t)
	b, err := NewEncryptedLocalBackend(t.TempDir(), enc)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })

	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "bkt"))
	_, err = b.PutObject(ctx, "bkt", "dir/a.txt", bytes.NewReader([]byte("alpha")), "text/plain")
	require.NoError(t, err)
	_, err = b.PutObject(ctx, "bkt", "dir/b.txt", bytes.NewReader([]byte("beta")), "text/plain")
	require.NoError(t, err)

	listed, err := b.ListObjects(ctx, "bkt", "dir/", 100)
	require.NoError(t, err)
	require.Len(t, listed, 2)
	listedSizes := make(map[string]int64)
	for _, obj := range listed {
		listedSizes[obj.Key] = obj.Size
	}
	require.Equal(t, map[string]int64{
		"dir/a.txt": 5,
		"dir/b.txt": 4,
	}, listedSizes)

	walked := make(map[string]int64)
	err = b.WalkObjects(ctx, "bkt", "dir/", func(obj *Object) error {
		walked[obj.Key] = obj.Size
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, listedSizes, walked)

	snapshots, err := b.ListAllObjects()
	require.NoError(t, err)
	snapshotSizes := make(map[string]int64)
	for _, obj := range snapshots {
		if obj.Bucket == "bkt" {
			snapshotSizes[obj.Key] = obj.Size
		}
	}
	require.Equal(t, listedSizes, snapshotSizes)
}

func TestEncryptedLocalBackendObjectSurvivesDataRootMove(t *testing.T) {
	enc := testEncryptor(t)
	root := filepath.Join(t.TempDir(), "root")
	b, err := NewEncryptedLocalBackend(root, enc)
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "bkt"))
	plaintext := []byte("movable encrypted object")
	_, err = b.PutObject(ctx, "bkt", "dir/object.txt", bytes.NewReader(plaintext), "text/plain")
	require.NoError(t, err)
	require.NoError(t, b.Close())

	moved := filepath.Join(t.TempDir(), "moved")
	require.NoError(t, os.Rename(root, moved))
	reopened, err := NewEncryptedLocalBackend(moved, enc)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	rc, _, err := reopened.GetObject(ctx, "bkt", "dir/object.txt")
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)
}

func TestEncryptedLocalBackendDoesNotAdvertiseWriteAt(t *testing.T) {
	enc := testEncryptor(t)
	b, err := NewEncryptedLocalBackend(t.TempDir(), enc)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })

	require.False(t, b.PreferWriteAt("__grainfs_volumes"))
}

func TestGetObjectNotFound(t *testing.T) {
	b := setupTestBackend(t)
	b.CreateBucket(context.Background(), "test-bucket")

	_, _, err := b.GetObject(context.Background(), "test-bucket", "nope.txt")
	require.ErrorIs(t, err, ErrObjectNotFound)
}

func TestGetObjectBucketNotFound(t *testing.T) {
	b := setupTestBackend(t)

	_, _, err := b.GetObject(context.Background(), "nope", "file.txt")
	require.ErrorIs(t, err, ErrBucketNotFound)
}

func TestHeadObject(t *testing.T) {
	b := setupTestBackend(t)
	b.CreateBucket(context.Background(), "test-bucket")

	_, err := b.HeadObject(context.Background(), "test-bucket", "nope.txt")
	require.ErrorIs(t, err, ErrObjectNotFound)

	data := []byte("head test")
	b.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader(data), "application/octet-stream")

	obj, err := b.HeadObject(context.Background(), "test-bucket", "file.txt")
	require.NoError(t, err, "HeadObject")
	assert.Equal(t, int64(len(data)), obj.Size)
}

func TestDeleteObject(t *testing.T) {
	b := setupTestBackend(t)
	b.CreateBucket(context.Background(), "test-bucket")

	b.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader([]byte("data")), "text/plain")

	require.NoError(t, b.DeleteObject(context.Background(), "test-bucket", "file.txt"), "DeleteObject")

	_, err := b.HeadObject(context.Background(), "test-bucket", "file.txt")
	require.ErrorIs(t, err, ErrObjectNotFound)

	// deleting nonexistent is not an error (S3 behavior)
	require.NoError(t, b.DeleteObject(context.Background(), "test-bucket", "nonexistent"), "DeleteObject nonexistent should not error")
}

func TestListObjects(t *testing.T) {
	b := setupTestBackend(t)
	b.CreateBucket(context.Background(), "test-bucket")

	b.PutObject(context.Background(), "test-bucket", "docs/a.txt", bytes.NewReader([]byte("a")), "text/plain")
	b.PutObject(context.Background(), "test-bucket", "docs/b.txt", bytes.NewReader([]byte("b")), "text/plain")
	b.PutObject(context.Background(), "test-bucket", "images/c.png", bytes.NewReader([]byte("c")), "image/png")

	// list all
	objs, err := b.ListObjects(context.Background(), "test-bucket", "", 1000)
	require.NoError(t, err, "ListObjects")
	require.Len(t, objs, 3)

	// list with prefix
	objs, err = b.ListObjects(context.Background(), "test-bucket", "docs/", 1000)
	require.NoError(t, err, "ListObjects with prefix")
	require.Len(t, objs, 2)

	// list with maxKeys
	objs, err = b.ListObjects(context.Background(), "test-bucket", "", 1)
	require.NoError(t, err, "ListObjects with maxKeys")
	require.Len(t, objs, 1)
}

func TestPutObjectOverwrite(t *testing.T) {
	b := setupTestBackend(t)
	b.CreateBucket(context.Background(), "test-bucket")

	b.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader([]byte("v1")), "text/plain")
	b.PutObject(context.Background(), "test-bucket", "file.txt", bytes.NewReader([]byte("version2")), "text/plain")

	rc, meta, err := b.GetObject(context.Background(), "test-bucket", "file.txt")
	require.NoError(t, err, "GetObject")
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	assert.Equal(t, "version2", string(got))
	assert.Equal(t, int64(8), meta.Size)
}

func TestPutObjectToBucketNotFound(t *testing.T) {
	b := setupTestBackend(t)

	_, err := b.PutObject(context.Background(), "nope", "file.txt", bytes.NewReader([]byte("data")), "text/plain")
	require.ErrorIs(t, err, ErrBucketNotFound)
}

func TestLargeObject(t *testing.T) {
	b := setupTestBackend(t)
	b.CreateBucket(context.Background(), "test-bucket")

	// 10MB object
	size := 10 * 1024 * 1024
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}

	obj, err := b.PutObject(context.Background(), "test-bucket", "large.bin", bytes.NewReader(data), "application/octet-stream")
	require.NoError(t, err, "PutObject large")
	assert.Equal(t, int64(size), obj.Size)

	rc, _, err := b.GetObject(context.Background(), "test-bucket", "large.bin")
	require.NoError(t, err, "GetObject large")
	defer rc.Close()

	got, _ := io.ReadAll(rc)
	assert.Equal(t, data, got)

	// verify segment blobs on disk (objects now flow through SegmentWriter,
	// so the legacy single-file objectPath no longer exists).
	_, err = os.Stat(b.objectPath("test-bucket", "large.bin") + "_segments")
	require.NoError(t, err, "expected segments dir on disk")
}

func TestLocalBackend_BucketPolicy(t *testing.T) {
	b := setupTestBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "policy-bucket"))

	// No policy initially
	_, err := b.GetBucketPolicy("policy-bucket")
	assert.ErrorIs(t, err, ErrBucketNotFound)

	// Set policy
	policy := []byte(`{"Version":"2012-10-17","Statement":[]}`)
	require.NoError(t, b.SetBucketPolicy("policy-bucket", policy))

	// Get policy
	got, err := b.GetBucketPolicy("policy-bucket")
	require.NoError(t, err)
	assert.Equal(t, policy, got)

	// Delete policy
	require.NoError(t, b.DeleteBucketPolicy("policy-bucket"))

	// Verify deleted
	_, err = b.GetBucketPolicy("policy-bucket")
	assert.ErrorIs(t, err, ErrBucketNotFound)

	// Delete non-existent policy (should not error)
	require.NoError(t, b.DeleteBucketPolicy("policy-bucket"))
}

func TestLocalBackend_Close(t *testing.T) {
	b := setupTestBackend(t)
	require.NoError(t, b.Close())
}

func TestLocalBackend_WriteAt(t *testing.T) {
	b := setupTestBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bkt"))

	full := []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ") // 26 bytes

	// Seed the legacy single-file path via WriteAt. PutObject now flows
	// through SegmentWriter, which the WriteAt/ReadAt fast paths do not
	// (yet) read; cross-path tests must seed via WriteAt directly.
	_, err := b.WriteAt(context.Background(), "bkt", "key", 0, full)
	require.NoError(t, err)

	cases := []struct {
		name      string
		offset    uint64
		data      []byte
		wantBytes []byte
		wantSize  int64
	}{
		{
			name:      "overwrite middle",
			offset:    5,
			data:      []byte("XYZ"),
			wantBytes: []byte("ABCDEXYZIJKLMNOPQRSTUVWXYZ"),
			wantSize:  26,
		},
		{
			name:      "overwrite and extend",
			offset:    24,
			data:      []byte("0123"),
			wantBytes: []byte("ABCDEXYZIJKLMNOPQRSTUVWX0123"),
			wantSize:  28,
		},
		{
			name:      "overwrite from zero",
			offset:    0,
			data:      []byte("aa"),
			wantBytes: []byte("aaCDEXYZIJKLMNOPQRSTUVWX0123"),
			wantSize:  28,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obj, err := b.WriteAt(context.Background(), "bkt", "key", tc.offset, tc.data)
			require.NoError(t, err)
			assert.Equal(t, tc.wantSize, obj.Size)

			rc, got, err := b.GetObject(context.Background(), "bkt", "key")
			require.NoError(t, err)
			defer rc.Close()
			gotBytes, _ := io.ReadAll(rc)
			assert.Equal(t, tc.wantBytes, gotBytes)
			assert.Equal(t, tc.wantSize, got.Size)
		})
	}
}

func TestLocalBackend_WriteAt_NewFile(t *testing.T) {
	b := setupTestBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bkt"))

	// Write at offset > 0 to a non-existent file → should create with sparse prefix.
	obj, err := b.WriteAt(context.Background(), "bkt", "sparse", 4, []byte("DATA"))
	require.NoError(t, err)
	assert.Equal(t, int64(8), obj.Size)

	rc, _, err := b.GetObject(context.Background(), "bkt", "sparse")
	require.NoError(t, err)
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	assert.Equal(t, 8, len(got))
	assert.Equal(t, []byte{0, 0, 0, 0}, got[:4]) // sparse hole = zeros
	assert.Equal(t, []byte("DATA"), got[4:])
}

func TestCachedBackend_WriteAt(t *testing.T) {
	b := setupTestBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bkt"))
	cached := NewCachedBackend(b)

	// Seed via WriteAt to stay on the legacy single-file path that
	// CachedBackend.WriteAt patches in place.
	_, err := cached.WriteAt(context.Background(), "bkt", "key", 0, []byte("hello world"))
	require.NoError(t, err)

	// Warm the cache.
	rc, _, err := cached.GetObject(context.Background(), "bkt", "key")
	require.NoError(t, err)
	rc.Close()

	// WriteAt should invalidate cache and update content.
	obj, err := cached.WriteAt(context.Background(), "bkt", "key", 6, []byte("Go!"))
	require.NoError(t, err)
	assert.Equal(t, int64(11), obj.Size)

	rc, _, err = cached.GetObject(context.Background(), "bkt", "key")
	require.NoError(t, err)
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	assert.Equal(t, []byte("hello Go!ld"), got)
}

func TestCachedBackend_CloseAndUnwrap(t *testing.T) {
	b := setupTestBackend(t)
	cached := NewCachedBackend(b)

	assert.Equal(t, b, cached.Unwrap())
	require.NoError(t, cached.Close())
}

func TestLocalBackend_ReadAt(t *testing.T) {
	b := setupTestBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bkt"))

	data := []byte("hello world")
	// Seed via WriteAt (legacy single-file path) so ReadAt's pread can find
	// the bytes at objectPath. Task 1.8 will make ReadAt segment-aware.
	_, err := b.WriteAt(context.Background(), "bkt", "obj", 0, data)
	require.NoError(t, err)

	buf := make([]byte, 5)
	n, err := b.ReadAt(context.Background(), "bkt", "obj", 6, buf)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("world"), buf[:n])
}

func TestLocalBackend_ReadAt_EOF(t *testing.T) {
	b := setupTestBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bkt"))

	// Seed via WriteAt (legacy single-file path). See TestLocalBackend_ReadAt.
	_, err := b.WriteAt(context.Background(), "bkt", "obj", 0, []byte("abc"))
	require.NoError(t, err)

	buf := make([]byte, 10)
	n, err := b.ReadAt(context.Background(), "bkt", "obj", 0, buf)
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, 3, n)
	assert.Equal(t, []byte("abc"), buf[:n])
}

func TestLocalBackend_ReadAt_NotExist(t *testing.T) {
	b := setupTestBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bkt"))

	buf := make([]byte, 4)
	_, err := b.ReadAt(context.Background(), "bkt", "missing", 0, buf)
	assert.True(t, os.IsNotExist(err))
}

func TestCachedBackend_ReadAt_CacheHit(t *testing.T) {
	cb, _ := newTestCachedBackend(t, WithMaxObjectCacheBytes(1024*1024))
	require.NoError(t, cb.CreateBucket(context.Background(), "bkt"))

	data := []byte("cached content")
	_, err := cb.PutObject(context.Background(), "bkt", "obj", bytes.NewReader(data), "application/octet-stream")
	require.NoError(t, err)

	// Warm the cache.
	rc, _, err := cb.GetObject(context.Background(), "bkt", "obj")
	require.NoError(t, err)
	rc.Close()

	buf := make([]byte, 6)
	n, err := cb.ReadAt(context.Background(), "bkt", "obj", 7, buf)
	require.NoError(t, err)
	assert.Equal(t, 6, n)
	assert.Equal(t, []byte("conten"), buf[:n])
}

func TestCachedBackend_ReadAt_CacheMiss(t *testing.T) {
	cb, _ := newTestCachedBackend(t)
	require.NoError(t, cb.CreateBucket(context.Background(), "bkt"))

	data := []byte("uncached data")
	// Seed via WriteAt so ReadAt's pread hits the legacy single-file path.
	_, err := cb.WriteAt(context.Background(), "bkt", "obj", 0, data)
	require.NoError(t, err)

	buf := make([]byte, 8)
	n, err := cb.ReadAt(context.Background(), "bkt", "obj", 0, buf)
	require.NoError(t, err)
	assert.Equal(t, 8, n)
	assert.Equal(t, []byte("uncached"), buf[:n])
}

type recordingPreparedReadAtBackend struct {
	Backend
	calls int
	obj   *Object
}

func (b *recordingPreparedReadAtBackend) ReadAtObject(ctx context.Context, bucket, key string, obj *Object, offset int64, buf []byte) (int, error) {
	b.calls++
	b.obj = obj
	return copy(buf, "prepared"), nil
}

func TestCachedBackend_ReadAtObject_DelegatesPreparedObject(t *testing.T) {
	inner, err := NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { inner.Close() })

	rec := &recordingPreparedReadAtBackend{Backend: inner}
	cb := NewCachedBackend(rec)

	reader, ok := any(cb).(interface {
		ReadAtObject(context.Context, string, string, *Object, int64, []byte) (int, error)
	})
	require.True(t, ok, "CachedBackend must expose prepared ReadAtObject through the wrapper")

	obj := &Object{Key: "k", Size: 8, ETag: "etag"}
	buf := make([]byte, 8)
	n, err := reader.ReadAtObject(context.Background(), "b", "k", obj, 0, buf)
	require.NoError(t, err)
	require.Equal(t, 8, n)
	require.Equal(t, []byte("prepared"), buf)
	require.Equal(t, 1, rec.calls)
	require.Same(t, obj, rec.obj)
}

func TestForceDeleteBucket(t *testing.T) {
	ctx := context.Background()
	b := setupTestBackend(t)

	require.NoError(t, b.CreateBucket(ctx, "force-test"))
	_, err := b.PutObject(ctx, "force-test", "a.txt", bytes.NewReader([]byte("x")), "text/plain")
	require.NoError(t, err)
	_, err = b.PutObject(ctx, "force-test", "b.txt", bytes.NewReader([]byte("y")), "text/plain")
	require.NoError(t, err)

	// regular delete should fail (bucket non-empty)
	require.ErrorIs(t, b.DeleteBucket(ctx, "force-test"), ErrBucketNotEmpty)

	// force delete should succeed
	require.NoError(t, b.ForceDeleteBucket(ctx, "force-test"))

	// bucket should be gone
	require.ErrorIs(t, b.HeadBucket(ctx, "force-test"), ErrBucketNotFound)
}

func TestForceDeleteBucket_NonExistent(t *testing.T) {
	ctx := context.Background()
	b := setupTestBackend(t)

	require.ErrorIs(t, b.ForceDeleteBucket(ctx, "no-such-bucket"), ErrBucketNotFound)
}

func TestOperations_ForceDeleteBucket(t *testing.T) {
	ctx := context.Background()
	b := setupTestBackend(t)
	ops := NewOperations(b)

	require.NoError(t, ops.CreateBucket(ctx, "force-test"))
	_, err := ops.PutObject(ctx, "force-test", "a.txt", bytes.NewReader([]byte("x")), "text/plain")
	require.NoError(t, err)
	_, err = ops.PutObject(ctx, "force-test", "b.txt", bytes.NewReader([]byte("y")), "text/plain")
	require.NoError(t, err)

	// regular delete should fail (bucket non-empty)
	require.Error(t, ops.DeleteBucket(ctx, "force-test"))

	// force delete should succeed
	require.NoError(t, ops.ForceDeleteBucket(ctx, "force-test"))

	// bucket should be gone
	require.Error(t, ops.HeadBucket(ctx, "force-test"))
}

func TestDeleteBucket_ClearsPolicy(t *testing.T) {
	b := setupTestBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "reuse-bucket"))
	require.NoError(t, b.SetBucketPolicy("reuse-bucket", []byte(`{"Version":"2012-10-17","Statement":[]}`)))

	require.NoError(t, b.DeleteBucket(ctx, "reuse-bucket"))

	// Recreate bucket — must NOT inherit the previous policy.
	require.NoError(t, b.CreateBucket(ctx, "reuse-bucket"))
	_, err := b.GetBucketPolicy("reuse-bucket")
	assert.ErrorIs(t, err, ErrBucketNotFound, "ghost policy must be gone after bucket delete+recreate")
}

// TestSnapshotRestore_ChunkedObjectRoundTrip exercises the Phase 1.6
// segment-aware PITR path: ListAllObjects must capture per-object segment
// refs, and RestoreObjects must (a) verify each segment blob exists on disk
// at <key>_segments/<blob_id>, and (b) rebuild Object.Segments so a follow-up
// GetObject yields byte-identical bytes.
//
// Two sizes:
//   - DefaultChunkSize+1 → 2 segments (multi-chunk case).
//   - 100 KiB → 1 segment (single-chunk case).
func TestSnapshotRestore_ChunkedObjectRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		size int
	}{
		{name: "multi_segment_chunk_plus_one", size: int(DefaultChunkSize) + 1},
		{name: "single_segment_100KiB", size: 100 << 10},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := setupTestBackend(t)
			ctx := context.Background()
			const bucket = "bkt"
			const key = "blob"
			require.NoError(t, b.CreateBucket(ctx, bucket))

			payload := make([]byte, tc.size)
			for i := range payload {
				payload[i] = byte(i * 31)
			}

			putObj, err := b.PutObject(ctx, bucket, key, bytes.NewReader(payload), "application/octet-stream")
			require.NoError(t, err)
			require.NotEmpty(t, putObj.Segments, "PutObject must produce segments")
			if tc.size > int(DefaultChunkSize) {
				require.GreaterOrEqual(t, len(putObj.Segments), 2, "chunk+1 object must split into ≥ 2 segments")
			}

			// Snapshot: ListAllObjects must include Segments.
			snaps, err := b.ListAllObjects()
			require.NoError(t, err)
			var snap SnapshotObject
			for _, s := range snaps {
				if s.Bucket == bucket && s.Key == key {
					snap = s
					break
				}
			}
			require.Equal(t, key, snap.Key)
			require.Equal(t, len(putObj.Segments), len(snap.Segments), "snapshot must carry segment refs")
			for i, seg := range snap.Segments {
				require.Equal(t, putObj.Segments[i].BlobID, seg.BlobID)
				require.Equal(t, putObj.Segments[i].Size, seg.Size)
			}

			// Wipe the badger object record but leave the segment blobs on
			// disk. RestoreObjects must rebuild the metadata.
			require.NoError(t, b.db.Update(func(txn *badger.Txn) error {
				return txn.Delete(b.objectMetaKey(bucket, key))
			}))
			_, _, err = b.GetObject(ctx, bucket, key)
			require.ErrorIs(t, err, ErrObjectNotFound, "meta wipe must precede restore")

			// Verify each segment blob is still on disk after the wipe.
			for _, seg := range snap.Segments {
				_, statErr := os.Stat(b.segmentPath(bucket, key, seg.BlobID))
				require.NoError(t, statErr, "segment blob %s must survive meta wipe", seg.BlobID)
			}

			restored, stale, err := b.RestoreObjects([]SnapshotObject{snap})
			require.NoError(t, err)
			require.Equal(t, 1, restored)
			require.Empty(t, stale, "all segment blobs are present — no stale entries")

			// Round-trip: GetObject must reproduce the original bytes.
			rc, _, err := b.GetObject(ctx, bucket, key)
			require.NoError(t, err)
			got, err := io.ReadAll(rc)
			require.NoError(t, err)
			rc.Close()
			require.Equal(t, payload, got, "restored object must round-trip byte-identical")
		})
	}
}

// TestSnapshotRestore_ChunkedObject_StaleWhenSegmentMissing locks in the
// segment-aware stale-blob detection: if any segment file is gone from disk,
// RestoreObjects must report the object as stale rather than restoring
// unreadable metadata.
func TestSnapshotRestore_ChunkedObject_StaleWhenSegmentMissing(t *testing.T) {
	b := setupTestBackend(t)
	ctx := context.Background()
	const bucket = "bkt"
	const key = "missing-seg"
	require.NoError(t, b.CreateBucket(ctx, bucket))

	payload := make([]byte, 32<<20) // 2 segments
	for i := range payload {
		payload[i] = byte(i)
	}
	putObj, err := b.PutObject(ctx, bucket, key, bytes.NewReader(payload), "application/octet-stream")
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(putObj.Segments), 2)

	snaps, err := b.ListAllObjects()
	require.NoError(t, err)
	var snap SnapshotObject
	for _, s := range snaps {
		if s.Key == key {
			snap = s
		}
	}
	require.NotEmpty(t, snap.Segments)

	// Wipe meta + remove ONE segment blob.
	require.NoError(t, b.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(b.objectMetaKey(bucket, key))
	}))
	require.NoError(t, os.Remove(b.segmentPath(bucket, key, snap.Segments[1].BlobID)))

	restored, stale, err := b.RestoreObjects([]SnapshotObject{snap})
	require.NoError(t, err)
	require.Equal(t, 0, restored)
	require.Len(t, stale, 1)
	require.Equal(t, key, stale[0].Key)
	require.Equal(t, snap.ETag, stale[0].ExpectedETag)
}

func TestMultiRootLocalBackend(t *testing.T) {
	metaDir := t.TempDir()
	dataRoot1 := t.TempDir()
	dataRoot2 := t.TempDir()
	dataRoots := []string{dataRoot1, dataRoot2}

	b, err := NewMultiRootLocalBackend(metaDir, dataRoots, nil)
	require.NoError(t, err)
	t.Cleanup(func() { b.Close() })

	bucket := "multi-root-bucket"
	require.NoError(t, b.CreateBucket(context.Background(), bucket))

	// Verify bucket directory is created in all dataRoots
	for _, root := range dataRoots {
		dir := filepath.Join(root, "data", bucket)
		info, err := os.Stat(dir)
		require.NoError(t, err)
		assert.True(t, info.IsDir())
	}

	// Put multiple objects to check round-robin / hashing distribution
	key1 := "key-one" // fnv hash will decide root
	data1 := []byte("data-for-key-one")
	obj1, err := b.PutObject(context.Background(), bucket, key1, bytes.NewReader(data1), "text/plain")
	require.NoError(t, err)
	require.NotEmpty(t, obj1.Segments)

	key2 := "key-two"
	data2 := []byte("data-for-key-two-different")
	obj2, err := b.PutObject(context.Background(), bucket, key2, bytes.NewReader(data2), "text/plain")
	require.NoError(t, err)
	require.NotEmpty(t, obj2.Segments)

	// Verify segment paths are written to correct hashed data roots
	path1 := b.segmentPath(bucket, key1, obj1.Segments[0].BlobID)
	path2 := b.segmentPath(bucket, key2, obj2.Segments[0].BlobID)

	assert.Contains(t, []string{
		filepath.Join(dataRoot1, "data", bucket, key1+"_segments", obj1.Segments[0].BlobID),
		filepath.Join(dataRoot2, "data", bucket, key1+"_segments", obj1.Segments[0].BlobID),
	}, path1)
	assert.Contains(t, []string{
		filepath.Join(dataRoot1, "data", bucket, key2+"_segments", obj2.Segments[0].BlobID),
		filepath.Join(dataRoot2, "data", bucket, key2+"_segments", obj2.Segments[0].BlobID),
	}, path2)

	// File existence checks on segment files
	_, err = os.Stat(path1)
	require.NoError(t, err)
	_, err = os.Stat(path2)
	require.NoError(t, err)

	// Get objects and verify content
	rc1, meta1, err := b.GetObject(context.Background(), bucket, key1)
	require.NoError(t, err)
	defer rc1.Close()
	got1, err := io.ReadAll(rc1)
	require.NoError(t, err)
	assert.Equal(t, data1, got1)
	assert.Equal(t, int64(len(data1)), meta1.Size)

	rc2, meta2, err := b.GetObject(context.Background(), bucket, key2)
	require.NoError(t, err)
	defer rc2.Close()
	got2, err := io.ReadAll(rc2)
	require.NoError(t, err)
	assert.Equal(t, data2, got2)
	assert.Equal(t, int64(len(data2)), meta2.Size)

	// Delete one object and verify physical file is gone
	require.NoError(t, b.DeleteObject(context.Background(), bucket, key1))
	_, err = os.Stat(path1)
	assert.True(t, os.IsNotExist(err))

	// Verify key2 is still intact
	_, err = os.Stat(path2)
	require.NoError(t, err)

	// Delete bucket and verify directories are clean
	require.NoError(t, b.DeleteObject(context.Background(), bucket, key2))
	require.NoError(t, b.DeleteBucket(context.Background(), bucket))

	for _, root := range dataRoots {
		dir := filepath.Join(root, "data", bucket)
		_, err := os.Stat(dir)
		assert.True(t, os.IsNotExist(err))
	}
}

func TestLocalBackend_DataWALRestoresWriteAtAndTruncate(t *testing.T) {
	dir := t.TempDir()
	dwal, err := datawal.Open(filepath.Join(dir, "datawal"), nil)
	require.NoError(t, err)
	b, err := NewLocalBackendWithDataWAL(filepath.Join(dir, "objects"), dwal)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	require.NoError(t, b.CreateBucket(context.Background(), "__grainfs_vfs_default"))
	_, err = b.WriteAt(context.Background(), "__grainfs_vfs_default", "file", 0, []byte("abcdef"))
	require.NoError(t, err)
	_, err = b.WriteAt(context.Background(), "__grainfs_vfs_default", "file", 2, []byte("ZZ"))
	require.NoError(t, err)
	require.NoError(t, b.Truncate(context.Background(), "__grainfs_vfs_default", "file", 5))
	require.NoError(t, dwal.Flush())

	require.NoError(t, os.Remove(b.objectPath("__grainfs_vfs_default", "file")))
	require.NoError(t, b.RecoverDataWAL(context.Background()))
	buf := make([]byte, 5)
	n, err := b.ReadAt(context.Background(), "__grainfs_vfs_default", "file", 0, buf)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, []byte("abZZe"), buf)
}
