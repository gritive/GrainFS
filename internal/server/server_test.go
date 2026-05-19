package server

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/incident"
	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/raft"
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
	base, _ := setupTestServerWithBackend(t, opts...)
	return base
}

func setupTestServerWithBackend(t *testing.T, opts ...Option) (string, *storage.LocalBackend) {
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
	return "http://" + addr, backend
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

type recordingRaftSnapshotter struct {
	triggerCalls atomic.Int32
	status       raft.SnapshotStatus
	result       raft.SnapshotResult
	triggerErr   error
	statusErr    error
}

func TestMapError_PlacementTargetsUnavailable(t *testing.T) {
	c := app.NewContext(0)
	err := &cluster.ErrInsufficientPlacementTargets{
		Operation:     "PutObject",
		GroupID:       "group-1",
		Desired:       cluster.ECConfig{DataShards: 2, ParityShards: 1},
		Configured:    []string{"n1", "n2", "n3"},
		Unavailable:   []string{"n3"},
		FailureReason: "shard write failed",
	}

	mapError(c, err)

	require.Equal(t, consts.StatusServiceUnavailable, c.Response.StatusCode())
	var got s3Error
	require.NoError(t, xml.Unmarshal(c.Response.Body(), &got))
	assert.Equal(t, "ServiceUnavailable", got.Code)
	assert.Contains(t, got.Message, "group-1")
}

func (r *recordingRaftSnapshotter) TriggerRaftSnapshot(context.Context) (raft.SnapshotResult, error) {
	r.triggerCalls.Add(1)
	return r.result, r.triggerErr
}

func (r *recordingRaftSnapshotter) RaftSnapshotStatus() (raft.SnapshotStatus, error) {
	return r.status, r.statusErr
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

func TestRaftSnapshotAdminTrigger(t *testing.T) {
	rs := &recordingRaftSnapshotter{
		result: raft.SnapshotResult{Index: 12, Term: 3, SizeBytes: 4096},
	}
	base := setupTestServerWithOptions(t, WithRaftSnapshotter(rs))

	resp, err := http.Post(base+"/admin/raft/snapshot", "application/json", nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var got map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	assert.Equal(t, float64(12), got["index"])
	assert.Equal(t, float64(3), got["term"])
	assert.Equal(t, float64(4096), got["size_bytes"])
	assert.Equal(t, int32(1), rs.triggerCalls.Load())
}

func TestRaftSnapshotAdminTriggerUnavailable(t *testing.T) {
	base := setupTestServer(t)

	resp, err := http.Post(base+"/admin/raft/snapshot", "application/json", nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

func TestRaftSnapshotAdminTriggerNotLeader(t *testing.T) {
	rs := &recordingRaftSnapshotter{
		triggerErr: raft.ErrNotLeader,
	}
	base := setupTestServerWithOptions(t, WithRaftSnapshotter(rs))

	resp, err := http.Post(base+"/admin/raft/snapshot", "application/json", nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusConflict, resp.StatusCode)
	assert.Equal(t, int32(1), rs.triggerCalls.Load())
}

func TestRaftSnapshotAdminStatus(t *testing.T) {
	rs := &recordingRaftSnapshotter{
		status: raft.SnapshotStatus{Available: true, Index: 9, Term: 2, SizeBytes: 128},
	}
	base := setupTestServerWithOptions(t, WithRaftSnapshotter(rs))

	resp, err := http.Get(base + "/admin/raft/snapshot")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var got map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	assert.Equal(t, true, got["available"])
	assert.Equal(t, float64(9), got["index"])
	assert.Equal(t, float64(2), got["term"])
	assert.Equal(t, float64(128), got["size_bytes"])
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
	req.Header.Set("x-amz-acl", "public-read")
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

func TestPutObjectUserMetadataPersistsOnHead(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/b", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	req, _ = http.NewRequest(http.MethodPut, base+"/b/file.txt", strings.NewReader("data"))
	req.Header.Set("x-amz-acl", "public-read")
	req.Header.Set("x-amz-meta-mtime", "1710000000")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	req, _ = http.NewRequest(http.MethodHead, base+"/b/file.txt", nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "1710000000", resp.Header.Get("x-amz-meta-mtime"))
}

func TestGetAndHeadObjectUseReadIndexer(t *testing.T) {
	ri := &recordingReadIndexer{index: 42}
	base := setupTestServerWithOptions(t, WithReadIndexer(ri))

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	req, _ = http.NewRequest(http.MethodPut, base+"/mybucket/file.txt", bytes.NewReader([]byte("data")))
	req.Header.Set("x-amz-acl", "public-read")
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
	req.Header.Set("x-amz-acl", "public-read")
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
	var result listObjectsResultV1
	xml.NewDecoder(resp.Body).Decode(&result)
	resp.Body.Close()
	assert.Len(t, result.Contents, 3)

	resp, _ = http.Get(base + "/mybucket?prefix=docs/")
	var prefixResult listObjectsResultV1
	xml.NewDecoder(resp.Body).Decode(&prefixResult)
	resp.Body.Close()
	assert.Len(t, prefixResult.Contents, 2)
}

func TestGetBucketLocation(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	http.DefaultClient.Do(req)

	resp, err := http.Get(base + "/mybucket?location")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result bucketLocationResult
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&result))
	assert.Equal(t, "us-east-1", result.LocationConstraint)

	resp, err = http.Get(base + "/mybucket/?location=")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestBucketTrailingSlashRoutesAsBucket(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/slashbucket/", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	req, _ = http.NewRequest(http.MethodHead, base+"/slashbucket/", nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp, err = http.Get(base + "/slashbucket/")
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
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
	base, backend := setupTestServerWithBackend(t)

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

	// Grant anonymous read so the anonymous GET below can succeed.
	require.NoError(t, backend.SetObjectACL("mybucket", "big-file.bin", 1)) // ACLPublicRead

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

func TestMetrics_ExposesFDGaugeNames(t *testing.T) {
	metrics.FDOpen.WithLabelValues("test-node").Set(10)
	metrics.FDLimit.WithLabelValues("test-node").Set(100)
	metrics.FDUsedRatio.WithLabelValues("test-node").Set(0.10)
	metrics.FDETASeconds.WithLabelValues("test-node", "critical").Set(300)
	metrics.FDOpenByCategory.WithLabelValues("test-node", "socket").Set(4)

	base := setupTestServer(t)
	resp, err := http.Get(base + "/metrics")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	text := string(body)
	assert.Contains(t, text, "grainfs_fd_open")
	assert.Contains(t, text, "grainfs_fd_limit")
	assert.Contains(t, text, "grainfs_fd_used_ratio")
	assert.Contains(t, text, "grainfs_fd_eta_seconds")
	assert.Contains(t, text, "grainfs_fd_open_by_category")
	assert.NotContains(t, text, "grainfs_fd_open_total")
	assert.NotContains(t, text, "grainfs_fd_category_open_total")
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

func TestMetricsUpdateOnCopyObjectOverwrite(t *testing.T) {
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
		resp, err := http.Get(base + "/metrics")
		require.NoError(t, err, "GET /metrics")
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err, "read metrics")
		require.NoError(t, resp.Body.Close())
		return string(body)
	}

	req, _ := http.NewRequest(http.MethodPut, base+"/test-bucket", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "create bucket")
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode)

	req, _ = http.NewRequest(http.MethodPut, base+"/test-bucket/src.txt", strings.NewReader("abc"))
	req.Header.Set("x-amz-acl", "public-read")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err, "put source")
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode)

	req, _ = http.NewRequest(http.MethodPut, base+"/test-bucket/dst.txt", strings.NewReader("123456"))
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err, "put destination")
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode)

	m := getMetrics()
	require.Equal(t, "2", parseMetric(m, "grainfs_objects_total"), "objects before copy overwrite")
	require.Equal(t, "9", parseMetric(m, "grainfs_storage_bytes_total"), "bytes before copy overwrite")

	req, _ = http.NewRequest(http.MethodPut, base+"/test-bucket/dst.txt", nil)
	req.Header.Set("x-amz-copy-source", "/test-bucket/src.txt")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err, "copy over destination")
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode)

	m = getMetrics()
	assert.Equal(t, "2", parseMetric(m, "grainfs_objects_total"), "copy overwrite must not double-count objects")
	assert.Equal(t, "6", parseMetric(m, "grainfs_storage_bytes_total"), "copy overwrite must subtract previous destination size")
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

func TestMutationGateAllowsIncidentReads(t *testing.T) {
	st := &incidentStoreStub{list: []incident.IncidentState{{ID: "cid", State: incident.StateObserved, UpdatedAt: time.Unix(100, 0).UTC()}}}
	base := setupTestServerWithOptions(t,
		WithIncidentStore(st),
		WithMutationGate(NewMutationGate(errors.New("recovery read-only"))),
	)

	resp, err := http.Get(base + "/api/incidents?limit=10")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
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

func TestAbortMultipartUpload_Success(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	http.DefaultClient.Do(req)

	// Initiate multipart upload
	req, _ = http.NewRequest(http.MethodPost, base+"/mybucket/abandon.bin?uploads", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	var initResult initiateMultipartUploadResult
	xml.NewDecoder(resp.Body).Decode(&initResult)
	resp.Body.Close()
	require.NotEmpty(t, initResult.UploadId)
	uploadID := initResult.UploadId

	// Upload one part so the staging dir exists.
	req, _ = http.NewRequest(http.MethodPut,
		fmt.Sprintf("%s/mybucket/abandon.bin?uploadId=%s&partNumber=1", base, uploadID),
		bytes.NewReader([]byte("part-data")))
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Abort: DELETE with ?uploadId=
	req, _ = http.NewRequest(http.MethodDelete,
		fmt.Sprintf("%s/mybucket/abandon.bin?uploadId=%s", base, uploadID),
		nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	// A second abort with the same uploadID must surface NoSuchUpload — confirms
	// the first abort actually removed the multipart record, not just hung up.
	req, _ = http.NewRequest(http.MethodDelete,
		fmt.Sprintf("%s/mybucket/abandon.bin?uploadId=%s", base, uploadID),
		nil)
	resp, _ = http.DefaultClient.Do(req)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Contains(t, string(body), "NoSuchUpload")
}

func TestAbortMultipartUpload_NotFound(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	http.DefaultClient.Do(req)

	req, _ = http.NewRequest(http.MethodDelete,
		base+"/mybucket/missing.bin?uploadId=does-not-exist",
		nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Contains(t, string(body), "NoSuchUpload")
}

func TestAbortMultipartUpload_DoesNotAffectOtherUploads(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	http.DefaultClient.Do(req)

	// Open two parallel multipart uploads on the same key.
	openUpload := func() string {
		req, _ := http.NewRequest(http.MethodPost, base+"/mybucket/parallel.bin?uploads", nil)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		var r initiateMultipartUploadResult
		xml.NewDecoder(resp.Body).Decode(&r)
		resp.Body.Close()
		require.NotEmpty(t, r.UploadId)
		return r.UploadId
	}
	doomed := openUpload()
	survivor := openUpload()
	require.NotEqual(t, doomed, survivor)

	// Upload one part on the survivor so we can complete it after the abort.
	partData := []byte("survivor-part")
	req, _ = http.NewRequest(http.MethodPut,
		fmt.Sprintf("%s/mybucket/parallel.bin?uploadId=%s&partNumber=1", base, survivor),
		bytes.NewReader(partData))
	resp, _ := http.DefaultClient.Do(req)
	etag := resp.Header.Get("Etag")
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Abort the doomed upload.
	req, _ = http.NewRequest(http.MethodDelete,
		fmt.Sprintf("%s/mybucket/parallel.bin?uploadId=%s", base, doomed),
		nil)
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	// Survivor must still complete cleanly.
	completeXML := fmt.Sprintf(`<CompleteMultipartUpload>
		<Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part>
	</CompleteMultipartUpload>`, etag)
	req, _ = http.NewRequest(http.MethodPost,
		fmt.Sprintf("%s/mybucket/parallel.bin?uploadId=%s", base, survivor),
		bytes.NewReader([]byte(completeXML)))
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode, "survivor multipart must still complete")
}

func TestListMultipartUploads_Success(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	http.DefaultClient.Do(req)

	openUpload := func(key string) string {
		req, _ := http.NewRequest(http.MethodPost, base+"/mybucket/"+key+"?uploads", nil)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		var r initiateMultipartUploadResult
		xml.NewDecoder(resp.Body).Decode(&r)
		resp.Body.Close()
		require.NotEmpty(t, r.UploadId)
		return r.UploadId
	}
	id1 := openUpload("logs/a.bin")
	id2 := openUpload("logs/b.bin")
	id3 := openUpload("snapshots/c.bin")

	// GET /:bucket?uploads — full bucket listing
	req, _ = http.NewRequest(http.MethodGet, base+"/mybucket?uploads", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var listed listMultipartUploadsResult
	require.NoError(t, xml.Unmarshal(body, &listed))
	assert.Equal(t, "mybucket", listed.Bucket)
	assert.Equal(t, 1000, listed.MaxUploads)
	assert.False(t, listed.IsTruncated)

	gotIDs := make(map[string]string)
	for _, u := range listed.Uploads {
		gotIDs[u.UploadId] = u.Key
	}
	assert.Equal(t, "logs/a.bin", gotIDs[id1])
	assert.Equal(t, "logs/b.bin", gotIDs[id2])
	assert.Equal(t, "snapshots/c.bin", gotIDs[id3])

	// Prefix filter via ?prefix=
	req, _ = http.NewRequest(http.MethodGet, base+"/mybucket?uploads&prefix=logs/", nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	var filtered listMultipartUploadsResult
	require.NoError(t, xml.Unmarshal(body, &filtered))
	gotPrefixIDs := make(map[string]bool)
	for _, u := range filtered.Uploads {
		gotPrefixIDs[u.UploadId] = true
	}
	assert.True(t, gotPrefixIDs[id1])
	assert.True(t, gotPrefixIDs[id2])
	assert.False(t, gotPrefixIDs[id3], "snapshots/ upload must not match logs/ prefix")

	// Aborting one upload removes it from the listing.
	req, _ = http.NewRequest(http.MethodDelete,
		fmt.Sprintf("%s/mybucket/logs/a.bin?uploadId=%s", base, id1), nil)
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	req, _ = http.NewRequest(http.MethodGet, base+"/mybucket?uploads", nil)
	resp, _ = http.DefaultClient.Do(req)
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	var afterAbort listMultipartUploadsResult
	require.NoError(t, xml.Unmarshal(body, &afterAbort))
	for _, u := range afterAbort.Uploads {
		assert.NotEqual(t, id1, u.UploadId, "aborted upload must not appear after abort")
	}
}

func TestListMultipartUploads_BucketNotFound(t *testing.T) {
	base := setupTestServer(t)
	req, _ := http.NewRequest(http.MethodGet, base+"/ghost?uploads", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Contains(t, string(body), "NoSuchBucket")
}

func TestListParts_Success(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	http.DefaultClient.Do(req)

	req, _ = http.NewRequest(http.MethodPost, base+"/mybucket/big.bin?uploads", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	var initResult initiateMultipartUploadResult
	xml.NewDecoder(resp.Body).Decode(&initResult)
	resp.Body.Close()
	uploadID := initResult.UploadId

	uploadedETags := map[int]string{}
	for _, n := range []int{2, 1, 3} {
		data := bytes.Repeat([]byte{byte('x' + n)}, 4096)
		req, _ = http.NewRequest(http.MethodPut,
			fmt.Sprintf("%s/mybucket/big.bin?uploadId=%s&partNumber=%d", base, uploadID, n),
			bytes.NewReader(data))
		resp, _ = http.DefaultClient.Do(req)
		etag := strings.Trim(resp.Header.Get("Etag"), "\"")
		resp.Body.Close()
		uploadedETags[n] = etag
	}

	req, _ = http.NewRequest(http.MethodGet,
		fmt.Sprintf("%s/mybucket/big.bin?uploadId=%s", base, uploadID),
		nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var listed listPartsResult
	require.NoError(t, xml.Unmarshal(body, &listed))
	assert.Equal(t, "mybucket", listed.Bucket)
	assert.Equal(t, "big.bin", listed.Key)
	assert.Equal(t, uploadID, listed.UploadId)
	require.Len(t, listed.Parts, 3)
	for i, p := range listed.Parts {
		assert.Equal(t, i+1, p.PartNumber, "ListParts must return parts sorted by part number")
		assert.Equal(t, int64(4096), p.Size)
		gotETag := strings.Trim(p.ETag, "\"")
		assert.Equal(t, uploadedETags[p.PartNumber], gotETag, "ETag must match upload-time hash")
	}
}

func TestListParts_NotFound(t *testing.T) {
	base := setupTestServer(t)
	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	http.DefaultClient.Do(req)

	req, _ = http.NewRequest(http.MethodGet,
		base+"/mybucket/whatever.bin?uploadId=does-not-exist", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Contains(t, string(body), "NoSuchUpload")
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
	var result listObjectsResultV1
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

	// Put object second time (overwrite) → should succeed; public-read for anonymous GET below
	req, _ = http.NewRequest(http.MethodPut, base+"/mybucket/file.txt", bytes.NewReader([]byte("second")))
	req.Header.Set("x-amz-acl", "public-read")
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Verify new content
	resp, _ = http.Get(base + "/mybucket/file.txt")
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, "second", string(got))
}

func TestCopyObjectParsesEncodedSourceVersionAndReplacesContentType(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/src", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	req, _ = http.NewRequest(http.MethodPut, base+"/dst", nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	req, _ = http.NewRequest(http.MethodPut, base+"/src/dir%20one/file.txt", strings.NewReader("copy me"))
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("x-amz-acl", "public-read")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	req, _ = http.NewRequest(http.MethodPut, base+"/dst/copied.txt", nil)
	req.Header.Set("x-amz-copy-source", "/src/dir%20one/file.txt?versionId=v1")
	req.Header.Set("x-amz-metadata-directive", "REPLACE")
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	require.Equal(t, http.StatusNotImplemented, resp.StatusCode)
	require.Contains(t, string(body), "NotImplemented")

	req, _ = http.NewRequest(http.MethodPut, base+"/dst/copied.txt", nil)
	req.Header.Set("x-amz-copy-source", "/src/dir%20one/file.txt")
	req.Header.Set("x-amz-metadata-directive", "REPLACE")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-amz-acl", "public-read")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	req, _ = http.NewRequest(http.MethodHead, base+"/dst/copied.txt", nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))
}

func TestCopyObjectReplaceUserMetadataPersistsOnHead(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/b", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	req, _ = http.NewRequest(http.MethodPut, base+"/b/src.txt", strings.NewReader("data"))
	req.Header.Set("x-amz-acl", "public-read")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	req, _ = http.NewRequest(http.MethodPut, base+"/b/dst.txt", nil)
	req.Header.Set("x-amz-copy-source", "/b/src.txt")
	req.Header.Set("x-amz-metadata-directive", "REPLACE")
	req.Header.Set("x-amz-meta-owner", "me")
	req.Header.Set("x-amz-acl", "public-read")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	req, _ = http.NewRequest(http.MethodHead, base+"/b/dst.txt", nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "me", resp.Header.Get("x-amz-meta-owner"))
}

func TestCopyObjectIfNoneMatchReturnsPreconditionFailed(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/b", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	req, _ = http.NewRequest(http.MethodPut, base+"/b/src.txt", strings.NewReader("data"))
	req.Header.Set("x-amz-acl", "public-read")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	etag := resp.Header.Get("ETag")
	resp.Body.Close()

	req, _ = http.NewRequest(http.MethodPut, base+"/b/dst.txt", nil)
	req.Header.Set("x-amz-copy-source", "/b/src.txt")
	req.Header.Set("x-amz-copy-source-if-none-match", etag)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	require.Equal(t, http.StatusPreconditionFailed, resp.StatusCode)
	require.Contains(t, string(body), "PreconditionFailed")
}

func TestCopyObjectInvalidMetadataDirectiveReturnsInvalidArgument(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/b", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	req, _ = http.NewRequest(http.MethodPut, base+"/b/src.txt", strings.NewReader("data"))
	req.Header.Set("x-amz-acl", "public-read")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	req, _ = http.NewRequest(http.MethodPut, base+"/b/dst.txt", nil)
	req.Header.Set("x-amz-copy-source", "/b/src.txt")
	req.Header.Set("x-amz-metadata-directive", "MOVE")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Contains(t, string(body), "InvalidArgument")
}

func TestCopyObjectInvalidConditionalDateReturnsInvalidArgument(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/b", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	req, _ = http.NewRequest(http.MethodPut, base+"/b/src.txt", strings.NewReader("data"))
	req.Header.Set("x-amz-acl", "public-read")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	req, _ = http.NewRequest(http.MethodPut, base+"/b/dst.txt", nil)
	req.Header.Set("x-amz-copy-source", "/b/src.txt")
	req.Header.Set("x-amz-copy-source-if-modified-since", "not-a-date")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Contains(t, string(body), "InvalidArgument")
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
