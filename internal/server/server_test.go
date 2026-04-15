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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "freePort")
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

func setupTestServer(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err, "NewLocalBackend")
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
	require.NoError(t, err, "create bucket")
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	req, _ = http.NewRequest(http.MethodHead, base+"/test-bucket", nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err, "head bucket")
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	req, _ = http.NewRequest(http.MethodHead, base+"/nope", nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err, "head nonexistent")
	resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestCreateBucketConflict(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/dup", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	req, _ = http.NewRequest(http.MethodPut, base+"/dup", nil)
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
	assert.Equal(t, http.StatusConflict, resp.StatusCode)
}

func TestListBuckets(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/alpha", nil)
	http.DefaultClient.Do(req)
	req, _ = http.NewRequest(http.MethodPut, base+"/bravo", nil)
	http.DefaultClient.Do(req)

	resp, err := http.Get(base + "/")
	require.NoError(t, err, "list buckets")
	defer resp.Body.Close()

	var result listBucketsResult
	xml.NewDecoder(resp.Body).Decode(&result)
	assert.Len(t, result.Buckets, 2)
}

func TestPutGetObject(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	http.DefaultClient.Do(req)

	body := "hello grainfs"
	req, _ = http.NewRequest(http.MethodPut, base+"/mybucket/hello.txt", bytes.NewReader([]byte(body)))
	req.Header.Set("Content-Type", "text/plain")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "put object")
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.NotEmpty(t, resp.Header.Get("Etag"))

	resp, err = http.Get(base + "/mybucket/hello.txt")
	require.NoError(t, err, "get object")
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	got, _ := io.ReadAll(resp.Body)
	assert.Equal(t, body, string(got))
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

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "4", resp.Header.Get("Content-Length"))
}

func TestHeadObjectNotFound(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	http.DefaultClient.Do(req)

	req, _ = http.NewRequest(http.MethodHead, base+"/mybucket/nope.txt", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
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
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	req, _ = http.NewRequest(http.MethodHead, base+"/mybucket/file.txt", nil)
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestDeleteBucket(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	http.DefaultClient.Do(req)

	req, _ = http.NewRequest(http.MethodDelete, base+"/mybucket", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
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
	assert.Equal(t, http.StatusConflict, resp.StatusCode)
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
	assert.Len(t, result.Contents, 3)

	resp, _ = http.Get(base + "/mybucket?prefix=docs/")
	var prefixResult listObjectsResult
	xml.NewDecoder(resp.Body).Decode(&prefixResult)
	resp.Body.Close()
	assert.Len(t, prefixResult.Contents, 2)
}

func TestPutObjectToBucketNotFound(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/nope/file.txt", bytes.NewReader([]byte("data")))
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestGetObjectNotFound(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	http.DefaultClient.Do(req)

	resp, _ := http.Get(base + "/mybucket/nope.txt")
	resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestMultipartUploadAPI(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	http.DefaultClient.Do(req)

	// Initiate multipart upload
	req, _ = http.NewRequest(http.MethodPost, base+"/mybucket/big-file.bin?uploads", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "initiate multipart")
	var initResult initiateMultipartUploadResult
	xml.NewDecoder(resp.Body).Decode(&initResult)
	resp.Body.Close()

	require.NotEmpty(t, initResult.UploadId)
	uploadID := initResult.UploadId

	// Upload part 1
	part1Data := bytes.Repeat([]byte("A"), 1024)
	req, _ = http.NewRequest(http.MethodPut,
		fmt.Sprintf("%s/mybucket/big-file.bin?uploadId=%s&partNumber=1", base, uploadID),
		bytes.NewReader(part1Data))
	resp, _ = http.DefaultClient.Do(req)
	etag1 := resp.Header.Get("Etag")
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode, "upload part 1")

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
	assert.Equal(t, http.StatusOK, resp.StatusCode, "complete multipart")

	// Verify the object exists
	resp, _ = http.Get(base + "/mybucket/big-file.bin")
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Len(t, got, len(part1Data)+len(part2Data))
}
