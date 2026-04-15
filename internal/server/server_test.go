package server

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

func setupTestServer(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	t.Cleanup(func() { backend.Close() })

	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	srv := New(addr, backend)
	go srv.Run()
	// wait for server to start
	for i := 0; i < 50; i++ {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			conn.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	return "http://" + addr
}

func TestCreateAndHeadBucket(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/test-bucket", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	req, _ = http.NewRequest(http.MethodHead, base+"/test-bucket", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("head bucket: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	req, _ = http.NewRequest(http.MethodHead, base+"/nope", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("head nonexistent: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestCreateBucketConflict(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/dup", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	req, _ = http.NewRequest(http.MethodPut, base+"/dup", nil)
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409, got %d", resp.StatusCode)
	}
}

func TestListBuckets(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/alpha", nil)
	http.DefaultClient.Do(req)
	req, _ = http.NewRequest(http.MethodPut, base+"/bravo", nil)
	http.DefaultClient.Do(req)

	resp, err := http.Get(base + "/")
	if err != nil {
		t.Fatalf("list buckets: %v", err)
	}
	defer resp.Body.Close()

	var result listBucketsResult
	xml.NewDecoder(resp.Body).Decode(&result)
	if len(result.Buckets) != 2 {
		t.Fatalf("expected 2 buckets, got %d", len(result.Buckets))
	}
}

func TestPutGetObject(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	http.DefaultClient.Do(req)

	body := "hello grainfs"
	req, _ = http.NewRequest(http.MethodPut, base+"/mybucket/hello.txt", bytes.NewReader([]byte(body)))
	req.Header.Set("Content-Type", "text/plain")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("put object: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if resp.Header.Get("Etag") == "" {
		t.Fatal("expected ETag header")
	}

	resp, err = http.Get(base + "/mybucket/hello.txt")
	if err != nil {
		t.Fatalf("get object: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	got, _ := io.ReadAll(resp.Body)
	if string(got) != body {
		t.Fatalf("body mismatch: got %q, want %q", got, body)
	}
}

func TestHeadObject(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	http.DefaultClient.Do(req)
	req, _ = http.NewRequest(http.MethodPut, base+"/mybucket/file.txt", bytes.NewReader([]byte("data")))
	http.DefaultClient.Do(req)

	req, _ = http.NewRequest(http.MethodHead, base+"/mybucket/file.txt", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if resp.Header.Get("Content-Length") != "4" {
		t.Fatalf("expected Content-Length 4, got %s", resp.Header.Get("Content-Length"))
	}
}

func TestHeadObjectNotFound(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	http.DefaultClient.Do(req)

	req, _ = http.NewRequest(http.MethodHead, base+"/mybucket/nope.txt", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestDeleteObject(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	http.DefaultClient.Do(req)
	req, _ = http.NewRequest(http.MethodPut, base+"/mybucket/file.txt", bytes.NewReader([]byte("data")))
	http.DefaultClient.Do(req)

	req, _ = http.NewRequest(http.MethodDelete, base+"/mybucket/file.txt", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", resp.StatusCode)
	}

	req, _ = http.NewRequest(http.MethodHead, base+"/mybucket/file.txt", nil)
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 after delete, got %d", resp.StatusCode)
	}
}

func TestDeleteBucket(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	http.DefaultClient.Do(req)

	req, _ = http.NewRequest(http.MethodDelete, base+"/mybucket", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", resp.StatusCode)
	}
}

func TestDeleteBucketNotEmpty(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	http.DefaultClient.Do(req)
	req, _ = http.NewRequest(http.MethodPut, base+"/mybucket/file.txt", bytes.NewReader([]byte("data")))
	http.DefaultClient.Do(req)

	req, _ = http.NewRequest(http.MethodDelete, base+"/mybucket", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409, got %d", resp.StatusCode)
	}
}

func TestListObjects(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	http.DefaultClient.Do(req)

	req, _ = http.NewRequest(http.MethodPut, base+"/mybucket/docs/a.txt", bytes.NewReader([]byte("a")))
	http.DefaultClient.Do(req)
	req, _ = http.NewRequest(http.MethodPut, base+"/mybucket/docs/b.txt", bytes.NewReader([]byte("b")))
	http.DefaultClient.Do(req)
	req, _ = http.NewRequest(http.MethodPut, base+"/mybucket/img/c.png", bytes.NewReader([]byte("c")))
	http.DefaultClient.Do(req)

	resp, _ := http.Get(base + "/mybucket")
	var result listObjectsResult
	xml.NewDecoder(resp.Body).Decode(&result)
	resp.Body.Close()
	if len(result.Contents) != 3 {
		t.Fatalf("expected 3 objects, got %d", len(result.Contents))
	}

	resp, _ = http.Get(base + "/mybucket?prefix=docs/")
	var prefixResult listObjectsResult
	xml.NewDecoder(resp.Body).Decode(&prefixResult)
	resp.Body.Close()
	if len(prefixResult.Contents) != 2 {
		t.Fatalf("expected 2 objects with prefix docs/, got %d", len(prefixResult.Contents))
	}
}

func TestPutObjectToBucketNotFound(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/nope/file.txt", bytes.NewReader([]byte("data")))
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestGetObjectNotFound(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	http.DefaultClient.Do(req)

	resp, _ := http.Get(base + "/mybucket/nope.txt")
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestMultipartUploadAPI(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	http.DefaultClient.Do(req)

	// Initiate multipart upload
	req, _ = http.NewRequest(http.MethodPost, base+"/mybucket/big-file.bin?uploads", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("initiate multipart: %v", err)
	}
	var initResult initiateMultipartUploadResult
	xml.NewDecoder(resp.Body).Decode(&initResult)
	resp.Body.Close()

	if initResult.UploadId == "" {
		t.Fatal("expected non-empty UploadId")
	}
	uploadID := initResult.UploadId

	// Upload part 1
	part1Data := bytes.Repeat([]byte("A"), 1024)
	req, _ = http.NewRequest(http.MethodPut,
		fmt.Sprintf("%s/mybucket/big-file.bin?uploadId=%s&partNumber=1", base, uploadID),
		bytes.NewReader(part1Data))
	resp, _ = http.DefaultClient.Do(req)
	etag1 := resp.Header.Get("Etag")
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("upload part 1: expected 200, got %d", resp.StatusCode)
	}

	// Upload part 2
	part2Data := bytes.Repeat([]byte("B"), 512)
	req, _ = http.NewRequest(http.MethodPut,
		fmt.Sprintf("%s/mybucket/big-file.bin?uploadId=%s&partNumber=2", base, uploadID),
		bytes.NewReader(part2Data))
	resp, _ = http.DefaultClient.Do(req)
	etag2 := resp.Header.Get("Etag")
	resp.Body.Close()

	// Complete multipart upload
	completeXML := fmt.Sprintf(`<CompleteMultipartUpload>
		<Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part>
		<Part><PartNumber>2</PartNumber><ETag>%s</ETag></Part>
	</CompleteMultipartUpload>`, etag1, etag2)

	req, _ = http.NewRequest(http.MethodPost,
		fmt.Sprintf("%s/mybucket/big-file.bin?uploadId=%s", base, uploadID),
		bytes.NewReader([]byte(completeXML)))
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("complete multipart: expected 200, got %d", resp.StatusCode)
	}

	// Verify the object exists
	resp, _ = http.Get(base + "/mybucket/big-file.bin")
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if len(got) != len(part1Data)+len(part2Data) {
		t.Fatalf("expected size %d, got %d", len(part1Data)+len(part2Data), len(got))
	}
}
