package server

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
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
	return setupTestServerWithOptions(t)
}

func setupTestServerWithOptions(t *testing.T, opts ...Option) string {
	t.Helper()
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err, "NewLocalBackend")
	t.Cleanup(func() { backend.Close() })

	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	srv := New(addr, backend, opts...)
	go srv.Run() //nolint:errcheck
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

type recordingReadIndexer struct {
	readCalls atomic.Int32
	waitCalls atomic.Int32
	index     uint64
}

func (r *recordingReadIndexer) ReadIndex(context.Context) (uint64, error) {
	r.readCalls.Add(1)
	return r.index, nil
}

func (r *recordingReadIndexer) WaitApplied(_ context.Context, index uint64) error {
	r.waitCalls.Add(1)
	if index != r.index {
		return fmt.Errorf("unexpected wait index: got %d want %d", index, r.index)
	}
	return nil
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

func TestGetAndHeadObjectUseReadIndexer(t *testing.T) {
	ri := &recordingReadIndexer{index: 42}
	base := setupTestServerWithOptions(t, WithReadIndexer(ri))

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	req, _ = http.NewRequest(http.MethodPut, base+"/mybucket/file.txt", bytes.NewReader([]byte("data")))
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	resp, err = http.Get(base + "/mybucket/file.txt")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	req, _ = http.NewRequest(http.MethodHead, base+"/mybucket/file.txt", nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	assert.Equal(t, int32(2), ri.readCalls.Load())
	assert.Equal(t, int32(2), ri.waitCalls.Load())
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

func TestMetricsEndpointReturnsPlainText(t *testing.T) {
	base := setupTestServer(t)

	resp, err := http.Get(base + "/metrics")
	require.NoError(t, err, "GET /metrics")
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	text := string(body)

	// /metrics must return parseable text, not binary protobuf
	assert.Contains(t, text, "grainfs_buckets_total", "should contain buckets_total metric as text")
	assert.Contains(t, text, "grainfs_objects_total", "should contain objects_total metric as text")
	assert.Contains(t, text, "grainfs_storage_bytes_total", "should contain storage_bytes_total metric as text")
}

func TestMetricsUpdateOnCRUD(t *testing.T) {
	base := setupTestServer(t)

	parseMetric := func(body, name string) string {
		for _, line := range strings.Split(body, "\n") {
			if strings.HasPrefix(line, name+" ") {
				return strings.TrimPrefix(line, name+" ")
			}
		}
		return ""
	}

	getMetrics := func() string {
		resp, _ := http.Get(base + "/metrics")
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return string(body)
	}

	// Initial state: no buckets (test starts fresh)
	m := getMetrics()
	assert.Equal(t, "0", parseMetric(m, "grainfs_buckets_total"), "initial buckets should be 0")

	// Create bucket → buckets_total should increment
	req, _ := http.NewRequest(http.MethodPut, base+"/test-bucket", nil)
	http.DefaultClient.Do(req)

	m = getMetrics()
	assert.Equal(t, "1", parseMetric(m, "grainfs_buckets_total"), "after create bucket")

	// Put object → objects_total should increment, storage_bytes should increase
	data := []byte("hello metrics test")
	req, _ = http.NewRequest(http.MethodPut, base+"/test-bucket/file.txt", bytes.NewReader(data))
	http.DefaultClient.Do(req)

	m = getMetrics()
	assert.Equal(t, "1", parseMetric(m, "grainfs_objects_total"), "after put object")
	assert.Equal(t, fmt.Sprintf("%d", len(data)), parseMetric(m, "grainfs_storage_bytes_total"), "storage bytes after put")

	// Delete object → objects_total should decrement
	req, _ = http.NewRequest(http.MethodDelete, base+"/test-bucket/file.txt", nil)
	http.DefaultClient.Do(req)

	m = getMetrics()
	assert.Equal(t, "0", parseMetric(m, "grainfs_objects_total"), "after delete object")
	assert.Equal(t, "0", parseMetric(m, "grainfs_storage_bytes_total"), "storage bytes after delete")

	// Delete bucket → buckets_total should decrement
	req, _ = http.NewRequest(http.MethodDelete, base+"/test-bucket", nil)
	http.DefaultClient.Do(req)

	m = getMetrics()
	assert.Equal(t, "0", parseMetric(m, "grainfs_buckets_total"), "after delete bucket")
}

// --- Policy handler integration tests ---

func TestPutGetDeleteBucketPolicy(t *testing.T) {
	base := setupTestServer(t)

	// Create bucket first
	req, _ := http.NewRequest(http.MethodPut, base+"/policy-bucket", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// PUT policy
	policy := `{
		"Version": "2012-10-17",
		"Statement": [{
			"Effect": "Allow",
			"Principal": "*",
			"Action": ["s3:GetObject"],
			"Resource": ["arn:aws:s3:::policy-bucket/*"]
		}]
	}`
	req, _ = http.NewRequest(http.MethodPut, base+"/policy-bucket?policy", strings.NewReader(policy))
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	// GET policy
	resp, err = http.Get(base + "/policy-bucket?policy")
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), "s3:GetObject")

	// DELETE policy
	req, _ = http.NewRequest(http.MethodDelete, base+"/policy-bucket?policy", nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	// GET policy after delete → not found
	resp, err = http.Get(base + "/policy-bucket?policy")
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestPutBucketPolicy_EmptyBody(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	http.DefaultClient.Do(req)

	req, _ = http.NewRequest(http.MethodPut, base+"/mybucket?policy", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestPutBucketPolicy_InvalidJSON(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	http.DefaultClient.Do(req)

	req, _ = http.NewRequest(http.MethodPut, base+"/mybucket?policy", strings.NewReader("{not json}"))
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// --- Volume handler integration tests ---

func TestVolumeHandlers_CRUD(t *testing.T) {
	base := setupTestServer(t)

	// List volumes (empty)
	resp, err := http.Get(base + "/volumes/")
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "[]", strings.TrimSpace(string(body)))

	// Create volume
	req, _ := http.NewRequest(http.MethodPut, base+"/volumes/testvol?size=1048576", nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, http.StatusCreated, resp.StatusCode)
	assert.Contains(t, string(body), "testvol")

	// Get volume
	resp, err = http.Get(base + "/volumes/testvol")
	require.NoError(t, err)
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), "testvol")

	// List volumes (1 item)
	resp, err = http.Get(base + "/volumes/")
	require.NoError(t, err)
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), "testvol")

	// Delete volume
	req, _ = http.NewRequest(http.MethodDelete, base+"/volumes/testvol", nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
}

func TestVolumeHandlers_AllocatedBytesInResponse(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/volumes/allocvol?size=1048576", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	// New volume: allocated_blocks = -1 (untracked), allocated_bytes = -1
	assert.Contains(t, string(body), `"allocated_bytes":-1`)
	assert.Contains(t, string(body), `"allocated_blocks":-1`)

	// GET should also include the fields
	resp, err = http.Get(base + "/volumes/allocvol")
	require.NoError(t, err)
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), `"allocated_bytes":-1`)
}

func TestVolumeHandlers_GetNotFound(t *testing.T) {
	base := setupTestServer(t)

	resp, _ := http.Get(base + "/volumes/nonexistent")
	resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestVolumeHandlers_DeleteNotFound(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodDelete, base+"/volumes/nonexistent", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestVolumeHandlers_CreateBadSize(t *testing.T) {
	base := setupTestServer(t)

	// No size
	req, _ := http.NewRequest(http.MethodPut, base+"/volumes/badvol", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	// Negative size
	req, _ = http.NewRequest(http.MethodPut, base+"/volumes/badvol?size=-100", nil)
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestVolumeHandlers_CreateConflict(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/volumes/dupvol?size=1048576", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	// Create again → conflict
	req, _ = http.NewRequest(http.MethodPut, base+"/volumes/dupvol?size=1048576", nil)
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
	assert.Equal(t, http.StatusConflict, resp.StatusCode)
}

func TestVolumeHandlers_Recalculate(t *testing.T) {
	base := setupTestServer(t)

	// Create volume and write one block so AllocatedBlocks=1.
	req, _ := http.NewRequest(http.MethodPut, base+"/volumes/recalcvol?size=1048576", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	// Recalculate on fresh volume (no blocks yet) → before=-1, after=0.
	req, _ = http.NewRequest(http.MethodPost, base+"/volumes/recalcvol/recalculate", nil)
	resp, _ = http.DefaultClient.Do(req)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), `"volume":"recalcvol"`)

	// Recalculate on nonexistent volume → 404.
	req, _ = http.NewRequest(http.MethodPost, base+"/volumes/nosuchvol/recalculate", nil)
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// --- Snapshot handlers integration tests ---

func TestSnapshotHandlers_CreateListDelete(t *testing.T) {
	base := setupTestServer(t)

	// Create volume
	req, _ := http.NewRequest(http.MethodPut, base+"/volumes/snapvol?size=1048576", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	// List snapshots (empty)
	resp, err := http.Get(base + "/volumes/snapvol/snapshots")
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "[]", strings.TrimSpace(string(body)))

	// Create snapshot
	req, _ = http.NewRequest(http.MethodPost, base+"/volumes/snapvol/snapshots", nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	var snap struct {
		ID string `json:"id"`
	}
	require.NoError(t, json.Unmarshal(body, &snap))
	assert.NotEmpty(t, snap.ID)

	// List snapshots (1 item)
	resp, _ = http.Get(base + "/volumes/snapvol/snapshots")
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), snap.ID)

	// Delete snapshot
	req, _ = http.NewRequest(http.MethodDelete, base+"/volumes/snapvol/snapshots/"+snap.ID, nil)
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	// List snapshots (empty again)
	resp, _ = http.Get(base + "/volumes/snapvol/snapshots")
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "[]", strings.TrimSpace(string(body)))
}

func TestSnapshotHandlers_Rollback(t *testing.T) {
	base := setupTestServer(t)

	// Create volume
	req, _ := http.NewRequest(http.MethodPut, base+"/volumes/rollvol?size=1048576", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	// Create snapshot
	req, _ = http.NewRequest(http.MethodPost, base+"/volumes/rollvol/snapshots", nil)
	resp, _ = http.DefaultClient.Do(req)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	var snap struct {
		ID string `json:"id"`
	}
	require.NoError(t, json.Unmarshal(body, &snap))

	// Rollback
	req, _ = http.NewRequest(http.MethodPost, base+"/volumes/rollvol/snapshots/"+snap.ID+"/rollback", nil)
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
}

func TestSnapshotHandlers_Clone(t *testing.T) {
	base := setupTestServer(t)

	// Create source volume
	req, _ := http.NewRequest(http.MethodPut, base+"/volumes/srcvol?size=1048576", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	// Clone
	cloneBody := `{"src":"srcvol","dst":"dstvol"}`
	resp, err := http.Post(base+"/volumes/clone", "application/json", strings.NewReader(cloneBody))
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, http.StatusCreated, resp.StatusCode)
	assert.Contains(t, string(body), "dstvol")
}

func TestSnapshotHandlers_CloneBadRequest(t *testing.T) {
	base := setupTestServer(t)

	resp, _ := http.Post(base+"/volumes/clone", "application/json", strings.NewReader(`{}`))
	resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// --- Dashboard tests ---

func TestServeDashboard(t *testing.T) {
	base := setupTestServer(t)

	resp, err := http.Get(base + "/ui/")
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), "<!DOCTYPE html>")
}

// --- Edge case handler tests ---

func TestHandlePost_UnsupportedOperation(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	http.DefaultClient.Do(req)

	// POST without ?uploads or ?uploadId → InvalidRequest
	req, _ = http.NewRequest(http.MethodPost, base+"/mybucket/file.txt", nil)
	resp, _ := http.DefaultClient.Do(req)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Contains(t, string(body), "InvalidRequest")
}

func TestUploadPart_InvalidPartNumber(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	http.DefaultClient.Do(req)

	// Part number = 0 (invalid)
	req, _ = http.NewRequest(http.MethodPut, base+"/mybucket/file.txt?uploadId=fake&partNumber=0", bytes.NewReader([]byte("data")))
	resp, _ := http.DefaultClient.Do(req)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Contains(t, string(body), "InvalidArgument")

	// Part number = abc (not a number)
	req, _ = http.NewRequest(http.MethodPut, base+"/mybucket/file.txt?uploadId=fake&partNumber=abc", bytes.NewReader([]byte("data")))
	resp, _ = http.DefaultClient.Do(req)
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Contains(t, string(body), "InvalidArgument")
}

func TestCompleteMultipartUpload_MalformedXML(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	http.DefaultClient.Do(req)

	// POST with uploadId but malformed XML body
	req, _ = http.NewRequest(http.MethodPost, base+"/mybucket/file.txt?uploadId=fake", bytes.NewReader([]byte("{not xml}")))
	resp, _ := http.DefaultClient.Do(req)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Contains(t, string(body), "MalformedXML")
}

func TestListObjects_WithMaxKeys(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	http.DefaultClient.Do(req)

	// Put 3 objects
	for _, key := range []string{"a.txt", "b.txt", "c.txt"} {
		req, _ = http.NewRequest(http.MethodPut, base+"/mybucket/"+key, bytes.NewReader([]byte("x")))
		http.DefaultClient.Do(req)
	}

	// List with max-keys=2
	resp, _ := http.Get(base + "/mybucket?max-keys=2")
	var result listObjectsResult
	xml.NewDecoder(resp.Body).Decode(&result)
	resp.Body.Close()
	assert.LessOrEqual(t, len(result.Contents), 2)
	assert.Equal(t, 2, result.MaxKeys)
}

func TestPutObject_Overwrite(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	http.DefaultClient.Do(req)

	// Put object first time
	req, _ = http.NewRequest(http.MethodPut, base+"/mybucket/file.txt", bytes.NewReader([]byte("first")))
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Put object second time (overwrite) → should succeed
	req, _ = http.NewRequest(http.MethodPut, base+"/mybucket/file.txt", bytes.NewReader([]byte("second")))
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Verify new content
	resp, _ = http.Get(base + "/mybucket/file.txt")
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, "second", string(got))
}

func TestDeleteObject_NonExistent(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	http.DefaultClient.Do(req)

	// Delete non-existent object — S3 returns 204 even if the object doesn't exist
	req, _ = http.NewRequest(http.MethodDelete, base+"/mybucket/nope.txt", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
}

func TestListObjects_BucketNotFound(t *testing.T) {
	base := setupTestServer(t)

	resp, _ := http.Get(base + "/nonexistent-bucket")
	resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestDeleteBucket_NotFound(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodDelete, base+"/nonexistent-bucket", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestCreateMultipartUpload_BucketNotFound(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPost, base+"/nonexistent/file.txt?uploads", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestGracefulShutdown(t *testing.T) {
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })

	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	srv := New(addr, backend)
	go srv.Run()

	// Wait for server to start
	for i := 0; i < 50; i++ {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			conn.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Verify server is accepting requests
	req, _ := http.NewRequest(http.MethodPut, "http://"+addr+"/test-bucket", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = srv.Shutdown(ctx)
	require.NoError(t, err, "shutdown should not error")

	// Verify server is no longer accepting connections
	time.Sleep(100 * time.Millisecond)
	_, err = net.DialTimeout("tcp", addr, 500*time.Millisecond)
	assert.Error(t, err, "server should no longer accept connections after shutdown")
}
