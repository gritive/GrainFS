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
