package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
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

// dekTestClusterID is the fixed 16-byte clusterID shared by the storage DEK
// test fixtures.
func dekTestClusterID() []byte { return bytes.Repeat([]byte{0x99}, 16) }

// newDEKKeeper builds a deterministic-KEK DEKKeeper for tests. The DEK itself is
// randomized by NewDEKKeeper, so callers that need a sealer and a backend to
// agree MUST share the SAME returned keeper instance.
func newDEKKeeper(t *testing.T) *encrypt.DEKKeeper {
	t.Helper()
	keeper, err := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0x88}, encrypt.KEKSize), dekTestClusterID())
	require.NoError(t, err, "NewDEKKeeper")
	return keeper
}

// newDEKLocalBackend builds a DEKKeeper-backed LocalBackend rooted at a temp dir.
func newDEKLocalBackend(t *testing.T) *LocalBackend {
	t.Helper()
	b, err := NewLocalBackendWithDEKKeeper(t.TempDir(), newDEKKeeper(t), dekTestClusterID())
	require.NoError(t, err, "NewLocalBackendWithDEKKeeper")
	t.Cleanup(func() { b.Close() })
	return b
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
	require.NoError(t, err, "NewDEKKeeper")
	b, err := NewLocalBackendWithDEKKeeper(t.TempDir(), keeper, bytes.Repeat([]byte{0x99}, 16))
	require.NoError(t, err, "NewLocalBackendWithDEKKeeper")
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
			require.NoError(t, err, "put")
			require.NotEmpty(t, obj.Segments, "segments")
			require.Equal(t, int64(tc.size), obj.Size)
			rc, _, err := b.GetObject(context.Background(), "test", "key-"+tc.name)
			require.NoError(t, err, "get")
			if err := requireReaderEqualPattern(rc, tc.size); err != nil {
				rc.Close()
				require.NoError(t, err)
			}
			rc.Close()
		})
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
	b := newDEKLocalBackend(t)

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

	rc, _, err := b.GetObject(context.Background(), "bkt", key)
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)
}

func TestEncryptedLocalBackendListsEncryptedObjectMetadata(t *testing.T) {
	b := newDEKLocalBackend(t)

	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "bkt"))
	_, err := b.PutObject(ctx, "bkt", "dir/a.txt", bytes.NewReader([]byte("alpha")), "text/plain")
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
}

func TestEncryptedLocalBackendObjectSurvivesDataRootMove(t *testing.T) {
	// Reopen must decrypt what the first open sealed, so the SAME keeper
	// instance is threaded through both opens (NewDEKKeeper randomizes the DEK).
	keeper := newDEKKeeper(t)
	cid := dekTestClusterID()
	root := filepath.Join(t.TempDir(), "root")
	b, err := NewLocalBackendWithDEKKeeper(root, keeper, cid)
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "bkt"))
	plaintext := []byte("movable encrypted object")
	_, err = b.PutObject(ctx, "bkt", "dir/object.txt", bytes.NewReader(plaintext), "text/plain")
	require.NoError(t, err)
	require.NoError(t, b.Close())

	moved := filepath.Join(t.TempDir(), "moved")
	require.NoError(t, os.Rename(root, moved))
	reopened, err := NewLocalBackendWithDEKKeeper(moved, keeper, cid)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	rc, _, err := reopened.GetObject(ctx, "bkt", "dir/object.txt")
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)
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

func TestMultiRootLocalBackend(t *testing.T) {
	metaDir := t.TempDir()
	dataRoot1 := t.TempDir()
	dataRoot2 := t.TempDir()
	dataRoots := []string{dataRoot1, dataRoot2}

	b, err := NewMultiRootLocalBackend(metaDir, dataRoots)
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

func TestLocalBackend_PutObjectWithRequest_ContentMD5Mismatch(t *testing.T) {
	b, err := NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, b.CreateBucket(context.Background(), "bucket"))
	_, err = b.PutObjectWithRequest(context.Background(), PutObjectRequest{
		Bucket: "bucket", Key: "k", Body: bytes.NewReader([]byte("hello")),
		ContentMD5Hex: "deadbeefdeadbeefdeadbeefdeadbeef", // wrong
	})
	require.ErrorIs(t, err, ErrContentMD5Mismatch)
}

func TestLocalBackend_PutObjectWithRequest_ContentMD5Match(t *testing.T) {
	b, err := NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, b.CreateBucket(context.Background(), "bucket"))
	obj, err := b.PutObjectWithRequest(context.Background(), PutObjectRequest{
		Bucket: "bucket", Key: "k2", Body: bytes.NewReader([]byte("hello")),
		ContentMD5Hex: "5d41402abc4b2a76b9719d911017c592", // md5("hello")
	})
	require.NoError(t, err)
	require.Equal(t, "5d41402abc4b2a76b9719d911017c592", obj.ETag)
}
