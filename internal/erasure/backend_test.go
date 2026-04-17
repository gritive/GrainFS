package erasure

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// zeroReader produces n bytes of zeros without heap allocation.
type zeroReader struct{ remaining int }

func (z *zeroReader) Read(p []byte) (int, error) {
	if z.remaining <= 0 {
		return 0, io.EOF
	}
	n := len(p)
	if n > z.remaining {
		n = z.remaining
	}
	for i := range n {
		p[i] = 0
	}
	z.remaining -= n
	return n, nil
}

// peakHeapAlloc samples runtime.MemStats.HeapAlloc every interval until the
// returned stop channel is closed, then reports the maximum observed value.
// HeapAlloc tracks live objects only (decreases after GC), giving a cleaner
// peak than HeapInuse which is sticky until the OS reclaims spans.
func peakHeapAlloc(interval time.Duration) (stop chan<- struct{}, peak func() uint64) {
	ch := make(chan struct{})
	var maxVal atomic.Uint64
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				var ms runtime.MemStats
				runtime.ReadMemStats(&ms)
				for {
					cur := maxVal.Load()
					if ms.HeapAlloc <= cur {
						break
					}
					if maxVal.CompareAndSwap(cur, ms.HeapAlloc) {
						break
					}
				}
			case <-ch:
				return
			}
		}
	}()
	return ch, maxVal.Load
}

func newTestBackend(t *testing.T) *ECBackend {
	t.Helper()
	dir := t.TempDir()
	b, err := NewECBackend(dir, DefaultDataShards, DefaultParityShards)
	require.NoError(t, err)
	t.Cleanup(func() { b.Close() })
	return b
}

func TestECBackend_BucketOps(t *testing.T) {
	b := newTestBackend(t)

	require.NoError(t, b.CreateBucket("test"))
	require.NoError(t, b.HeadBucket("test"))

	buckets, err := b.ListBuckets()
	require.NoError(t, err)
	assert.Equal(t, []string{"test"}, buckets)

	assert.ErrorIs(t, b.CreateBucket("test"), storage.ErrBucketAlreadyExists)
	assert.ErrorIs(t, b.HeadBucket("nope"), storage.ErrBucketNotFound)

	require.NoError(t, b.DeleteBucket("test"))
	assert.ErrorIs(t, b.HeadBucket("test"), storage.ErrBucketNotFound)
}

func TestECBackend_PutGetRoundtrip(t *testing.T) {
	b := newTestBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))

	tests := []struct {
		name    string
		key     string
		content string
	}{
		{"small", "small.txt", "hello"},
		{"medium", "medium.txt", strings.Repeat("x", 10000)},
		{"nested_key", "path/to/file.txt", "nested content"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			obj, err := b.PutObject("bucket", tt.key, strings.NewReader(tt.content), "text/plain")
			require.NoError(t, err)
			assert.Equal(t, int64(len(tt.content)), obj.Size)
			assert.NotEmpty(t, obj.ETag)

			rc, gotObj, err := b.GetObject("bucket", tt.key)
			require.NoError(t, err)
			defer rc.Close()

			data, _ := io.ReadAll(rc)
			assert.Equal(t, tt.content, string(data))
			assert.Equal(t, obj.ETag, gotObj.ETag)
			assert.Equal(t, obj.Size, gotObj.Size)
		})
	}
}

func TestECBackend_ShardRecovery(t *testing.T) {
	b := newTestBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))

	content := bytes.Repeat([]byte("recovery test data "), 500)
	_, err := b.PutObject("bucket", "recover.bin", bytes.NewReader(content), "application/octet-stream")
	require.NoError(t, err)

	shardDir := b.ShardDir("bucket", "recover.bin")

	// Delete shard 0 and shard 5 (one data, one parity)
	os.Remove(shardDir + "/00")
	os.Remove(shardDir + "/05")

	rc, _, err := b.GetObject("bucket", "recover.bin")
	require.NoError(t, err)
	defer rc.Close()

	data, _ := io.ReadAll(rc)
	assert.Equal(t, content, data)
}

func TestECBackend_TooManyShardsLost(t *testing.T) {
	b := newTestBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))

	content := []byte("this will be unrecoverable")
	_, err := b.PutObject("bucket", "lost.bin", bytes.NewReader(content), "application/octet-stream")
	require.NoError(t, err)

	shardDir := b.ShardDir("bucket", "lost.bin")
	os.Remove(shardDir + "/00")
	os.Remove(shardDir + "/01")
	os.Remove(shardDir + "/02")

	_, _, err = b.GetObject("bucket", "lost.bin")
	assert.Error(t, err)
}

func TestECBackend_DeleteObject(t *testing.T) {
	b := newTestBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))

	_, err := b.PutObject("bucket", "del.txt", strings.NewReader("data"), "text/plain")
	require.NoError(t, err)

	require.NoError(t, b.DeleteObject("bucket", "del.txt"))

	_, _, err = b.GetObject("bucket", "del.txt")
	assert.Error(t, err)

	_, err = os.Stat(b.ShardDir("bucket", "del.txt"))
	assert.True(t, os.IsNotExist(err))
}

func TestECBackend_ListObjects(t *testing.T) {
	b := newTestBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))

	for _, key := range []string{"a.txt", "b.txt", "dir/c.txt"} {
		_, err := b.PutObject("bucket", key, strings.NewReader("x"), "text/plain")
		require.NoError(t, err)
	}

	objects, err := b.ListObjects("bucket", "", 100)
	require.NoError(t, err)
	assert.Len(t, objects, 3)

	objects, err = b.ListObjects("bucket", "dir/", 100)
	require.NoError(t, err)
	assert.Len(t, objects, 1)
	assert.Equal(t, "dir/c.txt", objects[0].Key)
}

func TestECBackend_Overwrite(t *testing.T) {
	b := newTestBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))

	_, err := b.PutObject("bucket", "file.txt", strings.NewReader("v1"), "text/plain")
	require.NoError(t, err)

	_, err = b.PutObject("bucket", "file.txt", strings.NewReader("version2"), "text/plain")
	require.NoError(t, err)

	rc, obj, err := b.GetObject("bucket", "file.txt")
	require.NoError(t, err)
	defer rc.Close()

	data, _ := io.ReadAll(rc)
	assert.Equal(t, "version2", string(data))
	assert.Equal(t, int64(8), obj.Size)
}

func TestECBackend_Multipart(t *testing.T) {
	b := newTestBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))

	part1Data := bytes.Repeat([]byte("A"), 1024)
	part2Data := bytes.Repeat([]byte("B"), 512)

	upload, err := b.CreateMultipartUpload("bucket", "mp.bin", "application/octet-stream")
	require.NoError(t, err)

	p1, err := b.UploadPart("bucket", "mp.bin", upload.UploadID, 1, bytes.NewReader(part1Data))
	require.NoError(t, err)

	p2, err := b.UploadPart("bucket", "mp.bin", upload.UploadID, 2, bytes.NewReader(part2Data))
	require.NoError(t, err)

	obj, err := b.CompleteMultipartUpload("bucket", "mp.bin", upload.UploadID, []storage.Part{
		{PartNumber: p1.PartNumber, ETag: p1.ETag, Size: p1.Size},
		{PartNumber: p2.PartNumber, ETag: p2.ETag, Size: p2.Size},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(len(part1Data)+len(part2Data)), obj.Size)

	// Verify the assembled and EC-encoded object
	rc, _, err := b.GetObject("bucket", "mp.bin")
	require.NoError(t, err)
	defer rc.Close()

	data, _ := io.ReadAll(rc)
	expected := append(part1Data, part2Data...)
	assert.Equal(t, expected, data)
}

func TestECBackend_AbortMultipart(t *testing.T) {
	b := newTestBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))

	upload, err := b.CreateMultipartUpload("bucket", "abort.bin", "application/octet-stream")
	require.NoError(t, err)

	_, err = b.UploadPart("bucket", "abort.bin", upload.UploadID, 1, strings.NewReader("data"))
	require.NoError(t, err)

	require.NoError(t, b.AbortMultipartUpload("bucket", "abort.bin", upload.UploadID))

	_, _, err = b.GetObject("bucket", "abort.bin")
	assert.Error(t, err)
}

func TestECBackend_DeleteNonexistentObject(t *testing.T) {
	b := newTestBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))

	// S3 semantics: delete nonexistent is not an error
	err := b.DeleteObject("bucket", "nope.txt")
	assert.NoError(t, err)
}

func TestECBackend_DeleteBucketNotEmpty(t *testing.T) {
	b := newTestBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))

	_, err := b.PutObject("bucket", "file.txt", strings.NewReader("data"), "text/plain")
	require.NoError(t, err)

	err = b.DeleteBucket("bucket")
	assert.ErrorIs(t, err, storage.ErrBucketNotEmpty)
}

func TestECBackend_DeleteBucketNotFound(t *testing.T) {
	b := newTestBackend(t)
	err := b.DeleteBucket("nonexistent")
	assert.ErrorIs(t, err, storage.ErrBucketNotFound)
}

func TestECBackend_SetBucketECPolicy(t *testing.T) {
	b := newTestBackend(t)
	require.NoError(t, b.CreateBucket("ec-toggle"))

	// Default is EC enabled
	enabled, err := b.GetBucketECPolicy("ec-toggle")
	require.NoError(t, err)
	assert.True(t, enabled)

	// Disable EC
	require.NoError(t, b.SetBucketECPolicy("ec-toggle", false))
	enabled, err = b.GetBucketECPolicy("ec-toggle")
	require.NoError(t, err)
	assert.False(t, enabled)

	// Re-enable EC
	require.NoError(t, b.SetBucketECPolicy("ec-toggle", true))
	enabled, err = b.GetBucketECPolicy("ec-toggle")
	require.NoError(t, err)
	assert.True(t, enabled)
}

func TestECBackend_SetBucketECPolicy_NotFound(t *testing.T) {
	b := newTestBackend(t)
	err := b.SetBucketECPolicy("nope", false)
	assert.ErrorIs(t, err, storage.ErrBucketNotFound)
}

func TestECBackend_GetBucketECPolicy_NotFound(t *testing.T) {
	b := newTestBackend(t)
	_, err := b.GetBucketECPolicy("nope")
	assert.ErrorIs(t, err, storage.ErrBucketNotFound)
}

// TestECBackend_PutLargeObject_Roundtrip verifies byte-identical round-trip for a 50 MB object.
// This passes both before and after the streaming fix — confirms correctness is preserved.
func TestECBackend_PutLargeObject_Roundtrip(t *testing.T) {
	b := newTestBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))

	const size = 50 * 1024 * 1024
	payload := bytes.Repeat([]byte{0xAB, 0xCD, 0xEF, 0x12}, size/4)

	obj, err := b.PutObject("bucket", "roundtrip.bin", bytes.NewReader(payload), "application/octet-stream")
	require.NoError(t, err)
	assert.Equal(t, int64(size), obj.Size)
	assert.NotEmpty(t, obj.ETag)

	rc, _, err := b.GetObject("bucket", "roundtrip.bin")
	require.NoError(t, err)
	defer rc.Close()

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, payload, got)
}

// TestECBackend_PutLargeObject_NoOOM verifies that PutObject does not hold the
// entire 100 MB body in heap simultaneously (streaming spool-to-disk fix).
// RED: current io.ReadAll implementation spikes HeapInuse by ~200 MB+; threshold is 60 MB.
func TestECBackend_PutLargeObject_NoOOM(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large-object memory test in short mode")
	}
	b := newTestBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))

	const size = 100 * 1024 * 1024

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	stop, peakFn := peakHeapAlloc(5 * time.Millisecond)

	_, err := b.PutObject("bucket", "large.bin", &zeroReader{remaining: size}, "application/octet-stream")
	require.NoError(t, err)

	close(stop)
	runtime.GC()

	growth := int64(peakFn()) - int64(before.HeapInuse)
	if growth < 0 {
		growth = 0
	}

	const threshold = 60 * 1024 * 1024
	assert.Less(t, growth, int64(threshold),
		"PutObject(100MB) peak heap growth %d MB exceeds %d MB — io.ReadAll is buffering the full body",
		growth/(1024*1024), threshold/(1024*1024))
}

// TestECBackend_Multipart_LargeParts_NoOOM verifies CompleteMultipartUpload
// does not buffer all assembled parts (3 × 20 MB = 60 MB) in memory.
// RED: current assembled bytes.Buffer pattern buffers all 60 MB before EC encode.
func TestECBackend_Multipart_LargeParts_NoOOM(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large multipart memory test in short mode")
	}
	b := newTestBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))

	upload, err := b.CreateMultipartUpload("bucket", "mp-large.bin", "application/octet-stream")
	require.NoError(t, err)

	const partSize = 20 * 1024 * 1024
	var parts []storage.Part
	for i := 1; i <= 3; i++ {
		p, err := b.UploadPart("bucket", "mp-large.bin", upload.UploadID, i, &zeroReader{remaining: partSize})
		require.NoError(t, err)
		parts = append(parts, storage.Part{PartNumber: p.PartNumber, ETag: p.ETag, Size: p.Size})
	}

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	stop, peakFn := peakHeapAlloc(5 * time.Millisecond)

	obj, err := b.CompleteMultipartUpload("bucket", "mp-large.bin", upload.UploadID, parts)
	require.NoError(t, err)
	assert.Equal(t, int64(3*partSize), obj.Size)

	close(stop)
	runtime.GC()

	growth := int64(peakFn()) - int64(before.HeapInuse)
	if growth < 0 {
		growth = 0
	}

	const threshold = 60 * 1024 * 1024
	assert.Less(t, growth, int64(threshold),
		"CompleteMultipartUpload(3×20MB) peak heap growth %d MB exceeds %d MB — bytes.Buffer is buffering all parts",
		growth/(1024*1024), threshold/(1024*1024))
}

// TestStripVerifyCRC_MissingVsMismatch verifies that stripVerifyCRC returns
// ErrCRCMissing for too-short input and ErrCRCMismatch for corrupted CRC.
func TestStripVerifyCRC_MissingVsMismatch(t *testing.T) {
	t.Run("too short returns ErrCRCMissing", func(t *testing.T) {
		_, err := stripVerifyCRC([]byte{0x01, 0x02}) // 2 bytes < 4
		require.Error(t, err)
		require.ErrorIs(t, err, ErrCRCMissing, "short shard should return ErrCRCMissing, got: %v", err)
	})

	t.Run("zero-length returns ErrCRCMissing", func(t *testing.T) {
		_, err := stripVerifyCRC([]byte{})
		require.ErrorIs(t, err, ErrCRCMissing)
	})

	t.Run("corrupt CRC returns ErrCRCMismatch", func(t *testing.T) {
		payload := []byte("hello shard")
		valid := shardWithCRC(payload)
		// flip last byte to corrupt CRC
		corrupt := make([]byte, len(valid))
		copy(corrupt, valid)
		corrupt[len(corrupt)-1] ^= 0xFF
		_, err := stripVerifyCRC(corrupt)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrCRCMismatch, "corrupt CRC should return ErrCRCMismatch, got: %v", err)
	})

	t.Run("valid CRC returns payload", func(t *testing.T) {
		payload := []byte("hello shard")
		valid := shardWithCRC(payload)
		got, err := stripVerifyCRC(valid)
		require.NoError(t, err)
		assert.Equal(t, payload, got)
	})
}

func TestECBackend_PutGetPlain_ECDisabled(t *testing.T) {
	b := newTestBackend(t)
	require.NoError(t, b.CreateBucket("plain"))
	require.NoError(t, b.SetBucketECPolicy("plain", false))

	content := "plain storage without erasure coding"

	obj, err := b.PutObject("plain", "file.txt", strings.NewReader(content), "text/plain")
	require.NoError(t, err)
	assert.Equal(t, int64(len(content)), obj.Size)
	assert.NotEmpty(t, obj.ETag)

	// GetObject should read from plain storage
	rc, gotObj, err := b.GetObject("plain", "file.txt")
	require.NoError(t, err)
	defer rc.Close()

	data, _ := io.ReadAll(rc)
	assert.Equal(t, content, string(data))
	assert.Equal(t, obj.ETag, gotObj.ETag)
	assert.Equal(t, obj.Size, gotObj.Size)
}

func TestECBackend_HeadObject(t *testing.T) {
	b := newTestBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))

	content := strings.Repeat("x", 1000)
	putObj, err := b.PutObject("bucket", "head-test.txt", strings.NewReader(content), "text/plain")
	require.NoError(t, err)

	headObj, err := b.HeadObject("bucket", "head-test.txt")
	require.NoError(t, err)
	assert.Equal(t, "head-test.txt", headObj.Key)
	assert.Equal(t, int64(len(content)), headObj.Size)
	assert.Equal(t, "text/plain", headObj.ContentType)
	assert.Equal(t, putObj.ETag, headObj.ETag)
}

func TestECBackend_HeadObject_NotFound(t *testing.T) {
	b := newTestBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))

	_, err := b.HeadObject("bucket", "nope.txt")
	assert.ErrorIs(t, err, storage.ErrObjectNotFound)
}

func TestECBackend_HeadObject_BucketNotFound(t *testing.T) {
	b := newTestBackend(t)
	_, err := b.HeadObject("nope", "file.txt")
	assert.ErrorIs(t, err, storage.ErrBucketNotFound)
}

func TestECBackend_HeadObject_ECDisabled(t *testing.T) {
	b := newTestBackend(t)
	require.NoError(t, b.CreateBucket("plain"))
	require.NoError(t, b.SetBucketECPolicy("plain", false))

	_, err := b.PutObject("plain", "plain.txt", strings.NewReader("hello"), "text/plain")
	require.NoError(t, err)

	obj, err := b.HeadObject("plain", "plain.txt")
	require.NoError(t, err)
	assert.Equal(t, "plain.txt", obj.Key)
	assert.Equal(t, int64(5), obj.Size)
}

// testEncryptor is a marker-based encryptor for testing.
// Encrypt prepends a magic byte; Decrypt checks it and strips it.
// This lets us detect corrupted ciphertext.
type testEncryptor struct {
	magic byte
}

func (e *testEncryptor) Encrypt(data []byte) ([]byte, error) {
	out := make([]byte, len(data)+1)
	out[0] = e.magic
	copy(out[1:], data)
	return out, nil
}

func (e *testEncryptor) Decrypt(data []byte) ([]byte, error) {
	if len(data) == 0 || data[0] != e.magic {
		return nil, fmt.Errorf("decrypt: invalid magic byte")
	}
	return data[1:], nil
}

func TestECBackend_WithEncryption(t *testing.T) {
	dir := t.TempDir()
	enc := &testEncryptor{magic: 0xAA}
	b, err := NewECBackend(dir, DefaultDataShards, DefaultParityShards, WithEncryption(enc))
	require.NoError(t, err)
	t.Cleanup(func() { b.Close() })

	require.NoError(t, b.CreateBucket("enc"))

	content := bytes.Repeat([]byte("encrypted data test "), 200)
	obj, err := b.PutObject("enc", "secret.bin", bytes.NewReader(content), "application/octet-stream")
	require.NoError(t, err)
	assert.Equal(t, int64(len(content)), obj.Size)

	// Read back and verify round-trip
	rc, gotObj, err := b.GetObject("enc", "secret.bin")
	require.NoError(t, err)
	defer rc.Close()

	data, _ := io.ReadAll(rc)
	assert.Equal(t, content, data)
	assert.Equal(t, obj.ETag, gotObj.ETag)
}

func TestECBackend_WithEncryption_CorruptShard(t *testing.T) {
	dir := t.TempDir()
	enc := &testEncryptor{magic: 0xBB}
	b, err := NewECBackend(dir, DefaultDataShards, DefaultParityShards, WithEncryption(enc))
	require.NoError(t, err)
	t.Cleanup(func() { b.Close() })

	require.NoError(t, b.CreateBucket("bucket"))

	content := bytes.Repeat([]byte("shard corruption test "), 200)
	_, err = b.PutObject("bucket", "corrupt.bin", bytes.NewReader(content), "application/octet-stream")
	require.NoError(t, err)

	// Corrupt shard 0 on disk (write random garbage that will fail XOR decrypt)
	shardDir := b.ShardDir("bucket", "corrupt.bin")
	err = os.WriteFile(shardDir+"/00", []byte("totally corrupted"), 0o644)
	require.NoError(t, err)

	// Should still reconstruct from remaining shards (1 corrupted = within parity tolerance)
	rc, _, err := b.GetObject("bucket", "corrupt.bin")
	require.NoError(t, err)
	defer rc.Close()

	data, _ := io.ReadAll(rc)
	assert.Equal(t, content, data)
}

func TestECBackend_PutObject_BucketNotFound(t *testing.T) {
	b := newTestBackend(t)
	_, err := b.PutObject("nope", "file.txt", strings.NewReader("data"), "text/plain")
	assert.ErrorIs(t, err, storage.ErrBucketNotFound)
}

func TestECBackend_DeleteObject_BucketNotFound(t *testing.T) {
	b := newTestBackend(t)
	err := b.DeleteObject("nope", "file.txt")
	assert.ErrorIs(t, err, storage.ErrBucketNotFound)
}

func TestECBackend_ListObjects_BucketNotFound(t *testing.T) {
	b := newTestBackend(t)
	_, err := b.ListObjects("nope", "", 100)
	assert.ErrorIs(t, err, storage.ErrBucketNotFound)
}

func TestECBackend_MultipartBadBucket(t *testing.T) {
	b := newTestBackend(t)
	_, err := b.CreateMultipartUpload("nope", "file.bin", "binary")
	assert.ErrorIs(t, err, storage.ErrBucketNotFound)
}

func TestECBackend_UploadPartBadUploadID(t *testing.T) {
	b := newTestBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))

	_, err := b.UploadPart("bucket", "file.bin", "bad-id", 1, strings.NewReader("data"))
	assert.ErrorIs(t, err, storage.ErrUploadNotFound)
}

func TestECBackend_CompleteMultipartBadUploadID(t *testing.T) {
	b := newTestBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))

	_, err := b.CompleteMultipartUpload("bucket", "file.bin", "bad-id", nil)
	assert.ErrorIs(t, err, storage.ErrUploadNotFound)
}

func TestECBackend_AbortMultipartBadUploadID(t *testing.T) {
	b := newTestBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))

	err := b.AbortMultipartUpload("bucket", "file.bin", "bad-id")
	assert.ErrorIs(t, err, storage.ErrUploadNotFound)
}

func TestECBackend_Multipart_ECDisabled(t *testing.T) {
	b := newTestBackend(t)
	require.NoError(t, b.CreateBucket("plain"))
	require.NoError(t, b.SetBucketECPolicy("plain", false))

	part1Data := bytes.Repeat([]byte("A"), 512)
	part2Data := bytes.Repeat([]byte("B"), 256)

	upload, err := b.CreateMultipartUpload("plain", "mp-plain.bin", "application/octet-stream")
	require.NoError(t, err)

	p1, err := b.UploadPart("plain", "mp-plain.bin", upload.UploadID, 1, bytes.NewReader(part1Data))
	require.NoError(t, err)

	p2, err := b.UploadPart("plain", "mp-plain.bin", upload.UploadID, 2, bytes.NewReader(part2Data))
	require.NoError(t, err)

	obj, err := b.CompleteMultipartUpload("plain", "mp-plain.bin", upload.UploadID, []storage.Part{
		{PartNumber: p1.PartNumber, ETag: p1.ETag, Size: p1.Size},
		{PartNumber: p2.PartNumber, ETag: p2.ETag, Size: p2.Size},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(len(part1Data)+len(part2Data)), obj.Size)

	// Verify the assembled plain object
	rc, _, err := b.GetObject("plain", "mp-plain.bin")
	require.NoError(t, err)
	defer rc.Close()

	data, _ := io.ReadAll(rc)
	expected := append(part1Data, part2Data...)
	assert.Equal(t, expected, data)
}

func TestECBackend_ListObjectsMaxKeys(t *testing.T) {
	b := newTestBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))

	for _, key := range []string{"a.txt", "b.txt", "c.txt", "d.txt", "e.txt"} {
		_, err := b.PutObject("bucket", key, strings.NewReader("x"), "text/plain")
		require.NoError(t, err)
	}

	objects, err := b.ListObjects("bucket", "", 3)
	require.NoError(t, err)
	assert.Len(t, objects, 3)
}

// TestECBackend_ReadShardBlocksDuringWrite verifies that ReadShard (scrubber Verify)
// is blocked while a write lock is held (simulating an in-progress PutObject / WriteShard).
// This is the regression test for the per-key RWMutex fix (Eng Review #5 / A2).
// Without acquireReadLock in ReadShard, the goroutine would read the shard immediately
// without waiting — and the select-default case would fire, failing this test.
func TestECBackend_ReadShardBlocksDuringWrite(t *testing.T) {
	b := newTestBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))

	content := bytes.Repeat([]byte("concurrent lock test "), 300)
	_, err := b.PutObject("bucket", "key", bytes.NewReader(content), "application/octet-stream")
	require.NoError(t, err)

	total := DefaultDataShards + DefaultParityShards
	paths := b.ShardPaths("bucket", "key", total)
	require.NotEmpty(t, paths)

	// Hold the write lock to simulate a long-running PutObject or scrubber repair.
	lockHeld := make(chan struct{})
	release := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		unlock := b.acquireWriteLock("bucket", "key")
		close(lockHeld)
		<-release
		unlock()
	}()

	// Wait until the write lock is held before starting the reader.
	<-lockHeld

	// Start a ReadShard (scrubber Verify) for the same key — it must block.
	readDone := make(chan struct{})
	go func() {
		b.ReadShard("bucket", "key", paths[1]) //nolint:errcheck
		close(readDone)
	}()

	// Give the goroutine time to call acquireReadLock (which should block).
	time.Sleep(20 * time.Millisecond)

	// ReadShard must still be blocked (readDone channel should not be closed yet).
	select {
	case <-readDone:
		t.Fatal("ReadShard completed while write lock was held — per-key RWMutex is not working")
	default:
		// Correct: ReadShard is blocked by the write lock.
	}

	// Release the write lock; ReadShard must proceed and complete.
	close(release)
	wg.Wait()

	select {
	case <-readDone:
		// Correct: ReadShard unblocked after write lock was released.
	case <-time.After(2 * time.Second):
		t.Fatal("ReadShard did not complete after write lock was released")
	}
}

func TestECBackend_VersionedPut_GeneratesVersionID(t *testing.T) {
	b := newTestBackend(t)
	require.NoError(t, b.CreateBucket("ver-bucket"))
	require.NoError(t, b.SetBucketVersioning("ver-bucket", "Enabled"))

	obj, err := b.PutObject("ver-bucket", "file.txt", strings.NewReader("hello"), "text/plain")
	require.NoError(t, err)
	assert.NotEmpty(t, obj.VersionID, "versioned PutObject must return a VersionID")

	// Second PUT must generate a different VersionID, both versions must be retrievable
	obj2, err := b.PutObject("ver-bucket", "file.txt", strings.NewReader("world"), "text/plain")
	require.NoError(t, err)
	assert.NotEmpty(t, obj2.VersionID)
	assert.NotEqual(t, obj.VersionID, obj2.VersionID)
}

func TestECBackend_UnversionedPut_NoVersionID(t *testing.T) {
	b := newTestBackend(t)
	require.NoError(t, b.CreateBucket("plain-bucket"))

	obj, err := b.PutObject("plain-bucket", "file.txt", strings.NewReader("hello"), "text/plain")
	require.NoError(t, err)
	assert.Empty(t, obj.VersionID, "unversioned PutObject must not set VersionID")
}

func TestECBackend_BucketVersioning_SetGet(t *testing.T) {
	b := newTestBackend(t)
	require.NoError(t, b.CreateBucket("test-bucket"))

	// Initially Unversioned
	state, err := b.GetBucketVersioning("test-bucket")
	require.NoError(t, err)
	assert.Equal(t, "Unversioned", state)

	// Enable versioning
	require.NoError(t, b.SetBucketVersioning("test-bucket", "Enabled"))
	state, err = b.GetBucketVersioning("test-bucket")
	require.NoError(t, err)
	assert.Equal(t, "Enabled", state)
}

func TestECBackend_BucketVersioning_BucketNotFound(t *testing.T) {
	b := newTestBackend(t)
	err := b.SetBucketVersioning("no-such-bucket", "Enabled")
	assert.ErrorIs(t, err, storage.ErrBucketNotFound)
}
