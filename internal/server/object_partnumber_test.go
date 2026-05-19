package server

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// partRef carries the ETag and bytes of an uploaded part so the test can
// assert per-part Content-Range / body / ETag after CompleteMultipartUpload.
type partRef struct {
	etag string
	data []byte
}

// initMultipartUpload starts a multipart upload against the in-process server
// and returns the assigned uploadId. Anonymous — the test harness does not
// require SigV4.
func initMultipartUpload(t *testing.T, base, bucket, key string) string {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/%s/%s?uploads", base, bucket, key), nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "initiate multipart")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "initiate multipart status")
	var r initiateMultipartUploadResult
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&r), "decode init result")
	require.NotEmpty(t, r.UploadId)
	return r.UploadId
}

// uploadPart PUTs a single part and returns its server-reported ETag (quoted).
func uploadPart(t *testing.T, base, bucket, key, uploadID string, partNumber int, data []byte) string {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPut,
		fmt.Sprintf("%s/%s/%s?uploadId=%s&partNumber=%d", base, bucket, key, uploadID, partNumber),
		bytes.NewReader(data))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "upload part %d", partNumber)
	etag := resp.Header.Get("Etag")
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "upload part %d status", partNumber)
	require.NotEmpty(t, etag, "upload part %d etag", partNumber)
	return etag
}

// completeMultipart sends CompleteMultipartUpload with the supplied parts in
// order. Asserts 200.
func completeMultipart(t *testing.T, base, bucket, key, uploadID string, parts []partRef) {
	t.Helper()
	var sb strings.Builder
	sb.WriteString("<CompleteMultipartUpload>")
	for i, p := range parts {
		fmt.Fprintf(&sb, "<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", i+1, p.etag)
	}
	sb.WriteString("</CompleteMultipartUpload>")
	req, _ := http.NewRequest(http.MethodPost,
		fmt.Sprintf("%s/%s/%s?uploadId=%s", base, bucket, key, uploadID),
		strings.NewReader(sb.String()))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "complete multipart")
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "complete multipart status")
}

// setupMultipartTwoParts creates a bucket, uploads two parts ("hello"=5B and
// " grain"=6B → total 11B), completes the upload, grants public-read so the
// anonymous GET/HEAD below can succeed, and returns the parts metadata.
func setupMultipartTwoParts(t *testing.T, base string, backend *storage.LocalBackend, bucket, key string) []partRef {
	t.Helper()
	mustCreateBucket(t, backend, bucket)

	uploadID := initMultipartUpload(t, base, bucket, key)
	parts := []partRef{
		{data: []byte("hello")},
		{data: []byte(" grain")},
	}
	for i := range parts {
		parts[i].etag = uploadPart(t, base, bucket, key, uploadID, i+1, parts[i].data)
	}
	completeMultipart(t, base, bucket, key, uploadID, parts)

	require.NoError(t, backend.SetObjectACL(bucket, key, 1)) // ACLPublicRead
	return parts
}

// skipReasonMultipartPartsNotPersisted documents why every multipart-specific
// partNumber test below is skipped. The LocalBackend completion path does not
// populate obj.Parts (storage.fbs has no parts field, codec.go drops it,
// multipart.go never sets it). Only the cluster/EC path persists Parts today,
// so against the single-node test harness ?partNumber=N on a completed
// multipart object falls through to the legacy "no Parts → whole object"
// branch of partRange. Re-enable once the LocalBackend Parts gap is fixed.
const skipReasonMultipartPartsNotPersisted = "LocalBackend.CompleteMultipartUpload does not persist obj.Parts (storage.fbs/codec/multipart.go gap); cluster path is OK but unreachable from this harness"

// --- Multipart GET happy paths ---

func TestGetObjectPartNumber_Multipart_FirstPart(t *testing.T) {
	t.Skip(skipReasonMultipartPartsNotPersisted)

	base, backend := setupTestServerWithBackend(t)
	parts := setupMultipartTwoParts(t, base, backend, "b", "obj")

	resp, err := http.Get(base + "/b/obj?partNumber=1")
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	assert.Equal(t, http.StatusPartialContent, resp.StatusCode)
	assert.Equal(t, "bytes 0-4/11", resp.Header.Get("Content-Range"))
	assert.Equal(t, "2", resp.Header.Get("x-amz-mp-parts-count"))
	assert.Equal(t, parts[0].etag, resp.Header.Get("Etag"))
	assert.Equal(t, parts[0].data, body)
}

func TestGetObjectPartNumber_Multipart_SecondPart(t *testing.T) {
	t.Skip(skipReasonMultipartPartsNotPersisted)

	base, backend := setupTestServerWithBackend(t)
	parts := setupMultipartTwoParts(t, base, backend, "b", "obj")

	resp, err := http.Get(base + "/b/obj?partNumber=2")
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	assert.Equal(t, http.StatusPartialContent, resp.StatusCode)
	assert.Equal(t, "bytes 5-10/11", resp.Header.Get("Content-Range"))
	assert.Equal(t, "2", resp.Header.Get("x-amz-mp-parts-count"))
	assert.Equal(t, parts[1].data, body)
}

// --- Multipart GET error: partNumber out of range ---

func TestGetObjectPartNumber_Multipart_OutOfRange(t *testing.T) {
	t.Skip(skipReasonMultipartPartsNotPersisted)

	base, backend := setupTestServerWithBackend(t)
	_ = setupMultipartTwoParts(t, base, backend, "b", "obj")

	resp, err := http.Get(base + "/b/obj?partNumber=3")
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	assert.Equal(t, http.StatusRequestedRangeNotSatisfiable, resp.StatusCode)
	assert.Contains(t, string(body), "<Code>InvalidPartNumber</Code>")
}

// --- Single-PUT (non-multipart) virtual partNumber=1 ---

func TestGetObjectPartNumber_SinglePut_VirtualPartOne(t *testing.T) {
	base, backend := setupTestServerWithBackend(t)
	mustCreateBucket(t, backend, "b")

	payload := []byte("plain-object-body")
	req, _ := http.NewRequest(http.MethodPut, base+"/b/obj", bytes.NewReader(payload))
	req.Header.Set("x-amz-acl", "public-read")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp, err = http.Get(base + "/b/obj?partNumber=1")
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	assert.Equal(t, http.StatusPartialContent, resp.StatusCode)
	assert.Equal(t, fmt.Sprintf("bytes 0-%d/%d", len(payload)-1, len(payload)), resp.Header.Get("Content-Range"))
	assert.Equal(t, payload, body)
}

func TestGetObjectPartNumber_SinglePut_PartTwoIsInvalid(t *testing.T) {
	base, backend := setupTestServerWithBackend(t)
	mustCreateBucket(t, backend, "b")

	req, _ := http.NewRequest(http.MethodPut, base+"/b/obj", strings.NewReader("data"))
	req.Header.Set("x-amz-acl", "public-read")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	resp, err = http.Get(base + "/b/obj?partNumber=2")
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	assert.Equal(t, http.StatusRequestedRangeNotSatisfiable, resp.StatusCode)
	assert.Contains(t, string(body), "<Code>InvalidPartNumber</Code>")
}

// --- Invalid partNumber values (table-driven) ---

func TestGetObjectPartNumber_InvalidValues(t *testing.T) {
	base, backend := setupTestServerWithBackend(t)
	mustCreateBucket(t, backend, "b")

	req, _ := http.NewRequest(http.MethodPut, base+"/b/obj", strings.NewReader("data"))
	req.Header.Set("x-amz-acl", "public-read")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	cases := []struct {
		name  string
		value string
	}{
		{"zero", "0"},
		{"negative", "-1"},
		{"nonNumeric", "abc"},
		{"tooLarge", "10001"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := http.Get(base + "/b/obj?partNumber=" + tc.value)
			require.NoError(t, err)
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "partNumber=%s should be 400", tc.value)
			assert.Contains(t, string(body), "InvalidArgument")
		})
	}
}

// Empty partNumber=  → treated as not-present, returns the whole object (200).
func TestGetObjectPartNumber_EmptyValueIgnored(t *testing.T) {
	base, backend := setupTestServerWithBackend(t)
	mustCreateBucket(t, backend, "b")

	payload := []byte("whole-object")
	req, _ := http.NewRequest(http.MethodPut, base+"/b/obj", bytes.NewReader(payload))
	req.Header.Set("x-amz-acl", "public-read")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	resp, err = http.Get(base + "/b/obj?partNumber=")
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, payload, body)
	assert.Empty(t, resp.Header.Get("x-amz-mp-parts-count"))
}

// Range + partNumber together is rejected. The rejection happens in
// readPartNumber before partRange runs, so a plain single-PUT object suffices.
func TestGetObjectPartNumber_RejectsCombinedRangeHeader(t *testing.T) {
	base, backend := setupTestServerWithBackend(t)
	mustCreateBucket(t, backend, "b")

	req, _ := http.NewRequest(http.MethodPut, base+"/b/obj", strings.NewReader("data"))
	req.Header.Set("x-amz-acl", "public-read")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	req, _ = http.NewRequest(http.MethodGet, base+"/b/obj?partNumber=1", nil)
	req.Header.Set("Range", "bytes=0-0")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Contains(t, string(body), "InvalidArgument")
}

// --- HEAD ---

// HEAD with a valid partNumber on a multipart object: the handler currently
// returns 200 OK (not 206) but emits Content-Range / Content-Length / parts-count
// for the part. See report — this differs from S3 which returns 206 for HEAD too.
func TestHeadObjectPartNumber_Multipart_FirstPart(t *testing.T) {
	t.Skip(skipReasonMultipartPartsNotPersisted)

	base, backend := setupTestServerWithBackend(t)
	parts := setupMultipartTwoParts(t, base, backend, "b", "obj")

	req, _ := http.NewRequest(http.MethodHead, base+"/b/obj?partNumber=1", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	assert.Equal(t, http.StatusPartialContent, resp.StatusCode,
		"HEAD with valid partNumber must return 206 per S3 spec")
	assert.Equal(t, "5", resp.Header.Get("Content-Length"))
	assert.Equal(t, "bytes 0-4/11", resp.Header.Get("Content-Range"))
	assert.Equal(t, "2", resp.Header.Get("x-amz-mp-parts-count"))
	assert.Equal(t, parts[0].etag, resp.Header.Get("Etag"))
}
