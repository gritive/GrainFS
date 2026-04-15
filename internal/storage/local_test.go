package storage

import (
	"bytes"
	"io"
	"os"
	"testing"
)

func setupTestBackend(t *testing.T) *LocalBackend {
	t.Helper()
	dir := t.TempDir()
	b, err := NewLocalBackend(dir)
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

func TestCreateBucket(t *testing.T) {
	b := setupTestBackend(t)

	if err := b.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	// duplicate should fail
	if err := b.CreateBucket("test-bucket"); err != ErrBucketAlreadyExists {
		t.Fatalf("expected ErrBucketAlreadyExists, got %v", err)
	}
}

func TestHeadBucket(t *testing.T) {
	b := setupTestBackend(t)

	if err := b.HeadBucket("nonexistent"); err != ErrBucketNotFound {
		t.Fatalf("expected ErrBucketNotFound, got %v", err)
	}

	b.CreateBucket("test-bucket")
	if err := b.HeadBucket("test-bucket"); err != nil {
		t.Fatalf("HeadBucket: %v", err)
	}
}

func TestDeleteBucket(t *testing.T) {
	b := setupTestBackend(t)

	if err := b.DeleteBucket("nonexistent"); err != ErrBucketNotFound {
		t.Fatalf("expected ErrBucketNotFound, got %v", err)
	}

	b.CreateBucket("test-bucket")
	b.PutObject("test-bucket", "file.txt", bytes.NewReader([]byte("data")), "text/plain")

	if err := b.DeleteBucket("test-bucket"); err != ErrBucketNotEmpty {
		t.Fatalf("expected ErrBucketNotEmpty, got %v", err)
	}

	b.DeleteObject("test-bucket", "file.txt")
	if err := b.DeleteBucket("test-bucket"); err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}
}

func TestListBuckets(t *testing.T) {
	b := setupTestBackend(t)

	buckets, err := b.ListBuckets()
	if err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}
	if len(buckets) != 0 {
		t.Fatalf("expected 0 buckets, got %d", len(buckets))
	}

	b.CreateBucket("alpha")
	b.CreateBucket("bravo")

	buckets, err = b.ListBuckets()
	if err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}
	if len(buckets) != 2 {
		t.Fatalf("expected 2 buckets, got %d", len(buckets))
	}
}

func TestPutAndGetObject(t *testing.T) {
	b := setupTestBackend(t)
	b.CreateBucket("test-bucket")

	data := []byte("hello grainfs")
	obj, err := b.PutObject("test-bucket", "greeting.txt", bytes.NewReader(data), "text/plain")
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if obj.Size != int64(len(data)) {
		t.Fatalf("expected size %d, got %d", len(data), obj.Size)
	}
	if obj.ContentType != "text/plain" {
		t.Fatalf("expected content-type text/plain, got %s", obj.ContentType)
	}
	if obj.ETag == "" {
		t.Fatal("expected non-empty ETag")
	}

	rc, meta, err := b.GetObject("test-bucket", "greeting.txt")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer rc.Close()

	got, _ := io.ReadAll(rc)
	if !bytes.Equal(got, data) {
		t.Fatalf("data mismatch: got %q, want %q", got, data)
	}
	if meta.Size != int64(len(data)) {
		t.Fatalf("meta size mismatch: got %d, want %d", meta.Size, len(data))
	}
}

func TestGetObjectNotFound(t *testing.T) {
	b := setupTestBackend(t)
	b.CreateBucket("test-bucket")

	_, _, err := b.GetObject("test-bucket", "nope.txt")
	if err != ErrObjectNotFound {
		t.Fatalf("expected ErrObjectNotFound, got %v", err)
	}
}

func TestGetObjectBucketNotFound(t *testing.T) {
	b := setupTestBackend(t)

	_, _, err := b.GetObject("nope", "file.txt")
	if err != ErrBucketNotFound {
		t.Fatalf("expected ErrBucketNotFound, got %v", err)
	}
}

func TestHeadObject(t *testing.T) {
	b := setupTestBackend(t)
	b.CreateBucket("test-bucket")

	_, err := b.HeadObject("test-bucket", "nope.txt")
	if err != ErrObjectNotFound {
		t.Fatalf("expected ErrObjectNotFound, got %v", err)
	}

	data := []byte("head test")
	b.PutObject("test-bucket", "file.txt", bytes.NewReader(data), "application/octet-stream")

	obj, err := b.HeadObject("test-bucket", "file.txt")
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if obj.Size != int64(len(data)) {
		t.Fatalf("expected size %d, got %d", len(data), obj.Size)
	}
}

func TestDeleteObject(t *testing.T) {
	b := setupTestBackend(t)
	b.CreateBucket("test-bucket")

	b.PutObject("test-bucket", "file.txt", bytes.NewReader([]byte("data")), "text/plain")

	if err := b.DeleteObject("test-bucket", "file.txt"); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}

	_, err := b.HeadObject("test-bucket", "file.txt")
	if err != ErrObjectNotFound {
		t.Fatalf("expected ErrObjectNotFound after delete, got %v", err)
	}

	// deleting nonexistent is not an error (S3 behavior)
	if err := b.DeleteObject("test-bucket", "nonexistent"); err != nil {
		t.Fatalf("DeleteObject nonexistent should not error, got %v", err)
	}
}

func TestListObjects(t *testing.T) {
	b := setupTestBackend(t)
	b.CreateBucket("test-bucket")

	b.PutObject("test-bucket", "docs/a.txt", bytes.NewReader([]byte("a")), "text/plain")
	b.PutObject("test-bucket", "docs/b.txt", bytes.NewReader([]byte("b")), "text/plain")
	b.PutObject("test-bucket", "images/c.png", bytes.NewReader([]byte("c")), "image/png")

	// list all
	objs, err := b.ListObjects("test-bucket", "", 1000)
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if len(objs) != 3 {
		t.Fatalf("expected 3 objects, got %d", len(objs))
	}

	// list with prefix
	objs, err = b.ListObjects("test-bucket", "docs/", 1000)
	if err != nil {
		t.Fatalf("ListObjects with prefix: %v", err)
	}
	if len(objs) != 2 {
		t.Fatalf("expected 2 objects with prefix docs/, got %d", len(objs))
	}

	// list with maxKeys
	objs, err = b.ListObjects("test-bucket", "", 1)
	if err != nil {
		t.Fatalf("ListObjects with maxKeys: %v", err)
	}
	if len(objs) != 1 {
		t.Fatalf("expected 1 object with maxKeys=1, got %d", len(objs))
	}
}

func TestPutObjectOverwrite(t *testing.T) {
	b := setupTestBackend(t)
	b.CreateBucket("test-bucket")

	b.PutObject("test-bucket", "file.txt", bytes.NewReader([]byte("v1")), "text/plain")
	b.PutObject("test-bucket", "file.txt", bytes.NewReader([]byte("version2")), "text/plain")

	rc, meta, err := b.GetObject("test-bucket", "file.txt")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if string(got) != "version2" {
		t.Fatalf("expected version2, got %s", got)
	}
	if meta.Size != 8 {
		t.Fatalf("expected size 8, got %d", meta.Size)
	}
}

func TestPutObjectToBucketNotFound(t *testing.T) {
	b := setupTestBackend(t)

	_, err := b.PutObject("nope", "file.txt", bytes.NewReader([]byte("data")), "text/plain")
	if err != ErrBucketNotFound {
		t.Fatalf("expected ErrBucketNotFound, got %v", err)
	}
}

func TestLargeObject(t *testing.T) {
	b := setupTestBackend(t)
	b.CreateBucket("test-bucket")

	// 10MB object
	size := 10 * 1024 * 1024
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}

	obj, err := b.PutObject("test-bucket", "large.bin", bytes.NewReader(data), "application/octet-stream")
	if err != nil {
		t.Fatalf("PutObject large: %v", err)
	}
	if obj.Size != int64(size) {
		t.Fatalf("expected size %d, got %d", size, obj.Size)
	}

	rc, _, err := b.GetObject("test-bucket", "large.bin")
	if err != nil {
		t.Fatalf("GetObject large: %v", err)
	}
	defer rc.Close()

	got, _ := io.ReadAll(rc)
	if !bytes.Equal(got, data) {
		t.Fatal("large object data mismatch")
	}

	// verify file on disk
	_, err = os.Stat(b.objectPath("test-bucket", "large.bin"))
	if err != nil {
		t.Fatalf("expected file on disk: %v", err)
	}
}
