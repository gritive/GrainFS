package storage

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateAndCompleteMultipartUpload(t *testing.T) {
	b := setupTestBackend(t)
	b.CreateBucket(context.Background(), "test-bucket")

	upload, err := b.CreateMultipartUpload(context.Background(), "test-bucket", "big-file.bin", "application/octet-stream")
	require.NoError(t, err, "CreateMultipartUpload")
	assert.NotEmpty(t, upload.UploadID)

	part1Data := bytes.Repeat([]byte("A"), 5*1024*1024) // 5MB
	part2Data := bytes.Repeat([]byte("B"), 3*1024*1024) // 3MB

	p1, err := b.UploadPart(context.Background(), "test-bucket", "big-file.bin", upload.UploadID, 1, bytes.NewReader(part1Data))
	require.NoError(t, err, "UploadPart 1")
	assert.Equal(t, 1, p1.PartNumber)
	assert.NotEmpty(t, p1.ETag)

	p2, err := b.UploadPart(context.Background(), "test-bucket", "big-file.bin", upload.UploadID, 2, bytes.NewReader(part2Data))
	require.NoError(t, err, "UploadPart 2")

	obj, err := b.CompleteMultipartUpload(context.Background(), "test-bucket", "big-file.bin", upload.UploadID, []Part{*p1, *p2})
	require.NoError(t, err, "CompleteMultipartUpload")
	assert.Equal(t, int64(len(part1Data)+len(part2Data)), obj.Size)

	// verify the completed object is readable
	rc, meta, err := b.GetObject(context.Background(), "test-bucket", "big-file.bin")
	require.NoError(t, err, "GetObject")
	defer rc.Close()
	data, _ := io.ReadAll(rc)
	assert.Len(t, data, len(part1Data)+len(part2Data))
	assert.Equal(t, "application/octet-stream", meta.ContentType)
}

func TestAbortMultipartUpload(t *testing.T) {
	b := setupTestBackend(t)
	b.CreateBucket(context.Background(), "test-bucket")

	upload, _ := b.CreateMultipartUpload(context.Background(), "test-bucket", "aborted.bin", "application/octet-stream")
	b.UploadPart(context.Background(), "test-bucket", "aborted.bin", upload.UploadID, 1, bytes.NewReader([]byte("data")))

	require.NoError(t, b.AbortMultipartUpload(context.Background(), "test-bucket", "aborted.bin", upload.UploadID), "AbortMultipartUpload")

	// object should not exist
	_, err := b.HeadObject(context.Background(), "test-bucket", "aborted.bin")
	require.ErrorIs(t, err, ErrObjectNotFound)

	// abort again should fail
	require.ErrorIs(t, b.AbortMultipartUpload(context.Background(), "test-bucket", "aborted.bin", upload.UploadID), ErrUploadNotFound)
}

func TestEncryptedMultipartPartFilesHidePlaintext(t *testing.T) {
	enc := testEncryptor(t)
	b, err := NewEncryptedLocalBackend(t.TempDir(), enc)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })

	require.NoError(t, b.CreateBucket(context.Background(), "bkt"))
	up, err := b.CreateMultipartUpload(context.Background(), "bkt", "obj", "text/plain")
	require.NoError(t, err)

	partBytes := []byte("multipart-sensitive-payload")
	part, err := b.UploadPart(context.Background(), "bkt", "obj", up.UploadID, 1, bytes.NewReader(partBytes))
	require.NoError(t, err)
	require.Equal(t, int64(len(partBytes)), part.Size)

	raw, err := os.ReadFile(b.partPath(up.UploadID, 1))
	require.NoError(t, err)
	require.NotContains(t, string(raw), string(partBytes))

	parts, err := b.ListParts(context.Background(), "bkt", "obj", up.UploadID, 100)
	require.NoError(t, err)
	require.Len(t, parts, 1)
	require.Equal(t, part.ETag, parts[0].ETag)

	obj, err := b.CompleteMultipartUpload(context.Background(), "bkt", "obj", up.UploadID, []Part{*part})
	require.NoError(t, err)
	require.Equal(t, int64(len(partBytes)), obj.Size)

	rc, _, err := b.GetObject(context.Background(), "bkt", "obj")
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, partBytes, got)
}

func TestEncryptedMultipartUploadsListDecryptsMetadata(t *testing.T) {
	enc := testEncryptor(t)
	b, err := NewEncryptedLocalBackend(t.TempDir(), enc)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })

	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "bkt"))
	up, err := b.CreateMultipartUpload(ctx, "bkt", "prefix/obj", "text/plain")
	require.NoError(t, err)

	uploads, err := b.ListMultipartUploads(ctx, "bkt", "prefix/", 100)
	require.NoError(t, err)
	require.Len(t, uploads, 1)
	require.Equal(t, up.UploadID, uploads[0].UploadID)
	require.Equal(t, "bkt", uploads[0].Bucket)
	require.Equal(t, "prefix/obj", uploads[0].Key)
	require.Equal(t, "text/plain", uploads[0].ContentType)
}

func TestUploadPartInvalidUploadID(t *testing.T) {
	b := setupTestBackend(t)
	b.CreateBucket(context.Background(), "test-bucket")

	_, err := b.UploadPart(context.Background(), "test-bucket", "file.bin", "invalid-id", 1, bytes.NewReader([]byte("data")))
	require.ErrorIs(t, err, ErrUploadNotFound)
}

func TestCompleteMultipartBucketNotFound(t *testing.T) {
	b := setupTestBackend(t)

	_, err := b.CreateMultipartUpload(context.Background(), "nope", "file.bin", "application/octet-stream")
	require.ErrorIs(t, err, ErrBucketNotFound)
}

func TestLocalBackend_SweepOrphanMultiparts(t *testing.T) {
	b := setupTestBackend(t)
	partsRoot := filepath.Join(b.root, "parts")
	require.NoError(t, os.MkdirAll(partsRoot, 0o755))

	// old1, old2 — older than cutoff, should be removed.
	// fresh1 — newer than cutoff, should be retained.
	now := time.Now()
	old := now.Add(-2 * time.Hour)
	fresh := now.Add(-1 * time.Minute)
	for name, mtime := range map[string]time.Time{
		"old1":   old,
		"old2":   old,
		"fresh1": fresh,
	} {
		dir := filepath.Join(partsRoot, name)
		require.NoError(t, os.MkdirAll(dir, 0o755))
		require.NoError(t, os.Chtimes(dir, mtime, mtime))
	}
	// stray regular file under parts/ — must be ignored (only directories sweepable).
	straySrc := filepath.Join(partsRoot, "stray.txt")
	require.NoError(t, os.WriteFile(straySrc, []byte("x"), 0o644))
	require.NoError(t, os.Chtimes(straySrc, old, old))

	cutoff := now.Add(-1 * time.Hour)
	res, err := b.SweepOrphanMultiparts(context.Background(), cutoff)
	require.NoError(t, err)
	assert.Equal(t, 2, res.Removed)
	assert.Empty(t, res.Errors)

	// Verify which entries remain.
	for _, name := range []string{"fresh1", "stray.txt"} {
		_, err := os.Stat(filepath.Join(partsRoot, name))
		assert.NoError(t, err, "%q must remain", name)
	}
	for _, name := range []string{"old1", "old2"} {
		_, err := os.Stat(filepath.Join(partsRoot, name))
		assert.True(t, os.IsNotExist(err), "%q must be removed", name)
	}
}

func TestLocalBackend_SweepOrphanMultiparts_NoPartsDir(t *testing.T) {
	b := setupTestBackend(t)
	res, err := b.SweepOrphanMultiparts(context.Background(), time.Now())
	require.NoError(t, err)
	assert.Equal(t, 0, res.Removed)
	assert.Empty(t, res.Errors)
}

func TestLocalBackend_SweepOrphanMultiparts_AllFresh(t *testing.T) {
	b := setupTestBackend(t)
	partsRoot := filepath.Join(b.root, "parts")
	require.NoError(t, os.MkdirAll(partsRoot, 0o755))
	dir := filepath.Join(partsRoot, "active")
	require.NoError(t, os.MkdirAll(dir, 0o755))

	res, err := b.SweepOrphanMultiparts(context.Background(), time.Now().Add(-1*time.Hour))
	require.NoError(t, err)
	assert.Equal(t, 0, res.Removed)
}

func TestOperations_SweepOrphanMultiparts_DiscoversCapability(t *testing.T) {
	b := setupTestBackend(t)
	partsRoot := filepath.Join(b.root, "parts")
	require.NoError(t, os.MkdirAll(filepath.Join(partsRoot, "old"), 0o755))
	require.NoError(t, os.Chtimes(filepath.Join(partsRoot, "old"), time.Now().Add(-2*time.Hour), time.Now().Add(-2*time.Hour)))

	ops := NewOperations(b)
	res, err := ops.SweepOrphanMultiparts(context.Background(), time.Now().Add(-1*time.Hour))
	require.NoError(t, err)
	assert.Equal(t, 1, res.Removed)
}

type noSweepBackend struct{ Backend }

func TestOperations_SweepOrphanMultiparts_NoCapability(t *testing.T) {
	b := setupTestBackend(t)
	ops := NewOperations(noSweepBackend{Backend: b})
	res, err := ops.SweepOrphanMultiparts(context.Background(), time.Now())
	require.NoError(t, err)
	assert.Equal(t, 0, res.Removed)
}

func TestOperations_SweepOrphanMultiparts_BlockedByRecoveryGate(t *testing.T) {
	b := setupTestBackend(t)
	partsRoot := filepath.Join(b.root, "parts")
	require.NoError(t, os.MkdirAll(filepath.Join(partsRoot, "old"), 0o755))
	require.NoError(t, os.Chtimes(filepath.Join(partsRoot, "old"), time.Now().Add(-2*time.Hour), time.Now().Add(-2*time.Hour)))

	gate := NewRecoveryWriteGate(b, ErrRecoveryWriteDisabled)
	ops := NewOperations(gate)
	res, err := ops.SweepOrphanMultiparts(context.Background(), time.Now().Add(-1*time.Hour))
	require.ErrorIs(t, err, ErrRecoveryWriteDisabled)
	assert.Equal(t, 0, res.Removed)

	// Confirm the orphan dir was NOT removed (gate blocked the sweep).
	_, statErr := os.Stat(filepath.Join(partsRoot, "old"))
	assert.NoError(t, statErr, "gate should have prevented removal")
}

func TestLocalBackend_ListMultipartUploads(t *testing.T) {
	b := setupTestBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "alpha"))
	require.NoError(t, b.CreateBucket(ctx, "beta"))

	mk := func(bucket, key string) string {
		up, err := b.CreateMultipartUpload(ctx, bucket, key, "application/octet-stream")
		require.NoError(t, err)
		return up.UploadID
	}
	idA1 := mk("alpha", "logs/2026-05-08")
	idA2 := mk("alpha", "logs/2026-05-09")
	idA3 := mk("alpha", "snapshots/2026-05-08")
	idB := mk("beta", "logs/2026-05-08")

	uploadsA, err := b.ListMultipartUploads(ctx, "alpha", "", 0)
	require.NoError(t, err)
	gotIDsA := make(map[string]bool)
	for _, u := range uploadsA {
		gotIDsA[u.UploadID] = true
		assert.Equal(t, "alpha", u.Bucket)
	}
	assert.True(t, gotIDsA[idA1])
	assert.True(t, gotIDsA[idA2])
	assert.True(t, gotIDsA[idA3])
	assert.False(t, gotIDsA[idB], "beta upload must not appear in alpha listing")

	uploadsB, err := b.ListMultipartUploads(ctx, "beta", "", 0)
	require.NoError(t, err)
	require.Len(t, uploadsB, 1)
	assert.Equal(t, idB, uploadsB[0].UploadID)

	uploadsLogs, err := b.ListMultipartUploads(ctx, "alpha", "logs/", 0)
	require.NoError(t, err)
	gotPrefix := make(map[string]bool)
	for _, u := range uploadsLogs {
		gotPrefix[u.UploadID] = true
	}
	assert.True(t, gotPrefix[idA1])
	assert.True(t, gotPrefix[idA2])
	assert.False(t, gotPrefix[idA3], "snapshots/ upload must not match logs/ prefix")

	uploadsCap, err := b.ListMultipartUploads(ctx, "alpha", "", 2)
	require.NoError(t, err)
	assert.Len(t, uploadsCap, 2, "maxUploads cap must apply")
}

func TestLocalBackend_ListMultipartUploads_BucketNotFound(t *testing.T) {
	b := setupTestBackend(t)
	_, err := b.ListMultipartUploads(context.Background(), "ghost", "", 0)
	require.ErrorIs(t, err, ErrBucketNotFound)
}

func TestLocalBackend_ListParts(t *testing.T) {
	b := setupTestBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "mybucket"))
	upload, err := b.CreateMultipartUpload(ctx, "mybucket", "obj.bin", "application/octet-stream")
	require.NoError(t, err)
	uploadID := upload.UploadID

	empty, err := b.ListParts(ctx, "mybucket", "obj.bin", uploadID, 0)
	require.NoError(t, err)
	assert.Empty(t, empty, "no parts yet")

	wrote := map[int]string{}
	for _, n := range []int{2, 1, 3} {
		data := bytes.Repeat([]byte{byte('a' + n)}, 1024)
		p, err := b.UploadPart(ctx, "mybucket", "obj.bin", uploadID, n, bytes.NewReader(data))
		require.NoError(t, err)
		wrote[n] = p.ETag
	}

	parts, err := b.ListParts(ctx, "mybucket", "obj.bin", uploadID, 0)
	require.NoError(t, err)
	require.Len(t, parts, 3)
	for i, p := range parts {
		assert.Equal(t, i+1, p.PartNumber, "parts must be sorted ascending")
		assert.Equal(t, wrote[p.PartNumber], p.ETag, "ETag must match upload-time hash")
		assert.Equal(t, int64(1024), p.Size)
	}

	capped, err := b.ListParts(ctx, "mybucket", "obj.bin", uploadID, 2)
	require.NoError(t, err)
	assert.Len(t, capped, 2)
	assert.Equal(t, 1, capped[0].PartNumber)
	assert.Equal(t, 2, capped[1].PartNumber)
}

func TestLocalBackend_ListParts_NotFound(t *testing.T) {
	b := setupTestBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "mybucket"))
	_, err := b.ListParts(ctx, "mybucket", "ghost.bin", "ghost-upload-id", 0)
	require.ErrorIs(t, err, ErrUploadNotFound)
}
