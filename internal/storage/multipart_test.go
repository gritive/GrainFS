package storage

import (
	"bytes"
	"io"
	"testing"
)

func TestCreateAndCompleteMultipartUpload(t *testing.T) {
	b := setupTestBackend(t)
	b.CreateBucket("test-bucket")

	upload, err := b.CreateMultipartUpload("test-bucket", "big-file.bin", "application/octet-stream")
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	if upload.UploadID == "" {
		t.Fatal("expected non-empty UploadID")
	}

	part1Data := bytes.Repeat([]byte("A"), 5*1024*1024) // 5MB
	part2Data := bytes.Repeat([]byte("B"), 3*1024*1024) // 3MB

	p1, err := b.UploadPart("test-bucket", "big-file.bin", upload.UploadID, 1, bytes.NewReader(part1Data))
	if err != nil {
		t.Fatalf("UploadPart 1: %v", err)
	}
	if p1.PartNumber != 1 || p1.ETag == "" {
		t.Fatalf("unexpected part1: %+v", p1)
	}

	p2, err := b.UploadPart("test-bucket", "big-file.bin", upload.UploadID, 2, bytes.NewReader(part2Data))
	if err != nil {
		t.Fatalf("UploadPart 2: %v", err)
	}

	obj, err := b.CompleteMultipartUpload("test-bucket", "big-file.bin", upload.UploadID, []Part{*p1, *p2})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}
	if obj.Size != int64(len(part1Data)+len(part2Data)) {
		t.Fatalf("expected size %d, got %d", len(part1Data)+len(part2Data), obj.Size)
	}

	// verify the completed object is readable
	rc, meta, err := b.GetObject("test-bucket", "big-file.bin")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer rc.Close()
	data, _ := io.ReadAll(rc)
	if len(data) != len(part1Data)+len(part2Data) {
		t.Fatalf("data length mismatch: got %d", len(data))
	}
	if meta.ContentType != "application/octet-stream" {
		t.Fatalf("expected content-type application/octet-stream, got %s", meta.ContentType)
	}
}

func TestAbortMultipartUpload(t *testing.T) {
	b := setupTestBackend(t)
	b.CreateBucket("test-bucket")

	upload, _ := b.CreateMultipartUpload("test-bucket", "aborted.bin", "application/octet-stream")
	b.UploadPart("test-bucket", "aborted.bin", upload.UploadID, 1, bytes.NewReader([]byte("data")))

	if err := b.AbortMultipartUpload("test-bucket", "aborted.bin", upload.UploadID); err != nil {
		t.Fatalf("AbortMultipartUpload: %v", err)
	}

	// object should not exist
	_, err := b.HeadObject("test-bucket", "aborted.bin")
	if err != ErrObjectNotFound {
		t.Fatalf("expected ErrObjectNotFound, got %v", err)
	}

	// abort again should fail
	if err := b.AbortMultipartUpload("test-bucket", "aborted.bin", upload.UploadID); err != ErrUploadNotFound {
		t.Fatalf("expected ErrUploadNotFound, got %v", err)
	}
}

func TestUploadPartInvalidUploadID(t *testing.T) {
	b := setupTestBackend(t)
	b.CreateBucket("test-bucket")

	_, err := b.UploadPart("test-bucket", "file.bin", "invalid-id", 1, bytes.NewReader([]byte("data")))
	if err != ErrUploadNotFound {
		t.Fatalf("expected ErrUploadNotFound, got %v", err)
	}
}

func TestCompleteMultipartBucketNotFound(t *testing.T) {
	b := setupTestBackend(t)

	_, err := b.CreateMultipartUpload("nope", "file.bin", "application/octet-stream")
	if err != ErrBucketNotFound {
		t.Fatalf("expected ErrBucketNotFound, got %v", err)
	}
}
