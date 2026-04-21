package server

import (
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/storage"
)

// setupECTestServer starts a test server backed by a singleton DistributedBackend.
func setupECTestServer(t *testing.T) string {
	t.Helper()
	b := cluster.NewSingletonBackendForTest(t)

	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	srv := New(addr, b)
	go srv.Run() //nolint:errcheck
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

// TestPutBucketVersioning_NotImplemented verifies that LocalBackend returns 501.
func TestPutBucketVersioning_NotImplemented(t *testing.T) {
	base := setupTestServer(t) // uses LocalBackend

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	http.DefaultClient.Do(req) //nolint:errcheck

	body := `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>`
	req, _ = http.NewRequest(http.MethodPut, base+"/mybucket?versioning", strings.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotImplemented, resp.StatusCode)
}

// TestGetBucketVersioning_NotImplemented verifies that LocalBackend returns 501.
func TestGetBucketVersioning_NotImplemented(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	http.DefaultClient.Do(req) //nolint:errcheck

	resp, err := http.Get(base + "/mybucket?versioning")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotImplemented, resp.StatusCode)
}

// TestPutGetBucketVersioning_EC verifies the full versioning round-trip with ECBackend.
func TestPutGetBucketVersioning_EC(t *testing.T) {
	base := setupECTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/ver-bucket", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// GET before enabling → Unversioned
	resp, err = http.Get(base + "/ver-bucket?versioning")
	require.NoError(t, err)
	var vc versioningConfiguration
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&vc))
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "Unversioned", vc.Status)

	// PUT to enable versioning
	putBody := `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>`
	req, _ = http.NewRequest(http.MethodPut, base+"/ver-bucket?versioning", strings.NewReader(putBody))
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// GET after enabling → Enabled
	resp, err = http.Get(base + "/ver-bucket?versioning")
	require.NoError(t, err)
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&vc))
	resp.Body.Close()
	assert.Equal(t, "Enabled", vc.Status)
}

// TestPutBucketVersioning_InvalidStatus verifies bad status values are rejected.
func TestPutBucketVersioning_InvalidStatus(t *testing.T) {
	base := setupECTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	http.DefaultClient.Do(req) //nolint:errcheck

	body := `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Invalid</Status></VersioningConfiguration>`
	req, _ = http.NewRequest(http.MethodPut, base+"/mybucket?versioning", strings.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// TestPutBucketVersioning_BucketNotFound verifies 404 for non-existent bucket.
func TestPutBucketVersioning_BucketNotFound(t *testing.T) {
	base := setupECTestServer(t)

	body := `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>`
	req, _ := http.NewRequest(http.MethodPut, base+"/no-such-bucket?versioning", strings.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// TestGetObjectByVersionID_EC verifies GET /<bucket>/<key>?versionId= returns specific version.
func TestGetObjectByVersionID_EC(t *testing.T) {
	base := setupECTestServer(t)

	// Create bucket with versioning enabled
	req, _ := http.NewRequest(http.MethodPut, base+"/ver-bucket", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body := `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>`
	req, _ = http.NewRequest(http.MethodPut, base+"/ver-bucket?versioning", strings.NewReader(body))
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// PUT v1
	req, _ = http.NewRequest(http.MethodPut, base+"/ver-bucket/obj.txt", strings.NewReader("content-v1"))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	versionID1 := resp.Header.Get("X-Amz-Version-Id")
	resp.Body.Close()
	require.NotEmpty(t, versionID1)

	// PUT v2
	req, _ = http.NewRequest(http.MethodPut, base+"/ver-bucket/obj.txt", strings.NewReader("content-v2"))
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	versionID2 := resp.Header.Get("X-Amz-Version-Id")
	resp.Body.Close()
	require.NotEmpty(t, versionID2)
	assert.NotEqual(t, versionID1, versionID2)

	// GET latest → v2
	resp, err = http.Get(base + "/ver-bucket/obj.txt")
	require.NoError(t, err)
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, "content-v2", string(got))

	// GET ?versionId=v1 → v1
	resp, err = http.Get(base + "/ver-bucket/obj.txt?versionId=" + versionID1)
	require.NoError(t, err)
	got, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "content-v1", string(got))
}

// TestListObjectVersions_EC verifies GET /<bucket>?versions returns all versions.
func TestListObjectVersions_EC(t *testing.T) {
	base := setupECTestServer(t)

	// Create bucket and enable versioning
	req, _ := http.NewRequest(http.MethodPut, base+"/ver-bucket", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body := `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>`
	req, _ = http.NewRequest(http.MethodPut, base+"/ver-bucket?versioning", strings.NewReader(body))
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// PUT two versions
	req, _ = http.NewRequest(http.MethodPut, base+"/ver-bucket/obj.txt", strings.NewReader("v1"))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	vid1 := resp.Header.Get("X-Amz-Version-Id")
	resp.Body.Close()
	require.NotEmpty(t, vid1)

	req, _ = http.NewRequest(http.MethodPut, base+"/ver-bucket/obj.txt", strings.NewReader("v2"))
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	vid2 := resp.Header.Get("X-Amz-Version-Id")
	resp.Body.Close()
	require.NotEmpty(t, vid2)

	// GET ?versions
	resp, err = http.Get(base + "/ver-bucket?versions")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result listVersionsResult
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&result))
	assert.Equal(t, "ver-bucket", result.Name)

	// Should have 2 Version entries (no delete markers)
	assert.Len(t, result.Versions, 2)
	assert.Len(t, result.DeleteMarkers, 0)

	// Latest should be vid2
	latestIDs := []string{}
	for _, v := range result.Versions {
		if v.IsLatest {
			latestIDs = append(latestIDs, v.VersionID)
		}
	}
	require.Len(t, latestIDs, 1)
	assert.Equal(t, vid2, latestIDs[0])
}

// TestListObjectVersions_WithDeleteMarker_EC verifies DELETE appears as DeleteMarker.
func TestListObjectVersions_WithDeleteMarker_EC(t *testing.T) {
	base := setupECTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/ver-bucket", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body := `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>`
	req, _ = http.NewRequest(http.MethodPut, base+"/ver-bucket?versioning", strings.NewReader(body))
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()

	req, _ = http.NewRequest(http.MethodPut, base+"/ver-bucket/obj.txt", strings.NewReader("v1"))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	req, _ = http.NewRequest(http.MethodDelete, base+"/ver-bucket/obj.txt", nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	resp, err = http.Get(base + "/ver-bucket?versions")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result listVersionsResult
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&result))
	assert.Len(t, result.Versions, 1)
	assert.Len(t, result.DeleteMarkers, 1)
	assert.True(t, result.DeleteMarkers[0].IsLatest)
}

// TestListObjectVersions_NotImplemented_Local verifies LocalBackend returns 501.
func TestListObjectVersions_NotImplemented_Local(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	http.DefaultClient.Do(req) //nolint:errcheck

	resp, err := http.Get(base + "/mybucket?versions")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotImplemented, resp.StatusCode)
}

// Ensure LocalBackend still satisfies storage.Backend (compilation check).
var _ storage.Backend = (*storage.LocalBackend)(nil)

// TestDeleteObjectVersion_EC verifies DELETE /<bucket>/<key>?versionId= hard-deletes a version.
func TestDeleteObjectVersion_EC(t *testing.T) {
	base := setupECTestServer(t)

	// Create bucket, enable versioning
	req, _ := http.NewRequest(http.MethodPut, base+"/ver-bucket", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	putVC := `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>`
	req, _ = http.NewRequest(http.MethodPut, base+"/ver-bucket?versioning", strings.NewReader(putVC))
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()

	// PUT an object → get versionId
	req, _ = http.NewRequest(http.MethodPut, base+"/ver-bucket/file.txt", strings.NewReader("hello"))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	versionID := resp.Header.Get("X-Amz-Version-Id")
	resp.Body.Close()
	require.NotEmpty(t, versionID)

	// DELETE ?versionId= → 204
	req, _ = http.NewRequest(http.MethodDelete, base+"/ver-bucket/file.txt?versionId="+versionID, nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	// GET ?versionId= must be 404
	resp, err = http.Get(base + "/ver-bucket/file.txt?versionId=" + versionID)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	// ListVersions must be empty
	resp, err = http.Get(base + "/ver-bucket?versions")
	require.NoError(t, err)
	defer resp.Body.Close()
	var result listVersionsResult
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&result))
	assert.Empty(t, result.Versions)
	assert.Empty(t, result.DeleteMarkers)
}

// TestGetObjectVersion_DeleteMarker_EC verifies GET ?versionId=<deleteMarkerID>
// returns 405 MethodNotAllowed with x-amz-delete-marker: true (S3 spec).
func TestGetObjectVersion_DeleteMarker_EC(t *testing.T) {
	base := setupECTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/ver-bucket", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	putVC := `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>`
	req, _ = http.NewRequest(http.MethodPut, base+"/ver-bucket?versioning", strings.NewReader(putVC))
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()

	req, _ = http.NewRequest(http.MethodPut, base+"/ver-bucket/obj.txt", strings.NewReader("v1"))
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()

	// DELETE creates a delete marker
	req, _ = http.NewRequest(http.MethodDelete, base+"/ver-bucket/obj.txt", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	markerID := resp.Header.Get("x-amz-version-id")
	resp.Body.Close()
	require.NotEmpty(t, markerID)

	// GET ?versionId=<deleteMarkerID> → 405 + x-amz-delete-marker: true
	resp, err = http.Get(base + "/ver-bucket/obj.txt?versionId=" + markerID)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
	assert.Equal(t, "true", resp.Header.Get("x-amz-delete-marker"))
	assert.Equal(t, markerID, resp.Header.Get("x-amz-version-id"))
}

// TestHeadObjectVersion_EC verifies HEAD /<bucket>/<key>?versionId=<id> routes
// through HeadObjectVersion and returns 405 for delete markers.
func TestHeadObjectVersion_EC(t *testing.T) {
	base := setupECTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/ver-bucket", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	putVC := `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>`
	req, _ = http.NewRequest(http.MethodPut, base+"/ver-bucket?versioning", strings.NewReader(putVC))
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()

	req, _ = http.NewRequest(http.MethodPut, base+"/ver-bucket/obj.txt", strings.NewReader("v1"))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	versionID := resp.Header.Get("X-Amz-Version-Id")
	resp.Body.Close()
	require.NotEmpty(t, versionID)

	// HEAD ?versionId=<id> of regular version → 200 + version header
	req, _ = http.NewRequest(http.MethodHead, base+"/ver-bucket/obj.txt?versionId="+versionID, nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, versionID, resp.Header.Get("x-amz-version-id"))

	// Create delete marker; HEAD ?versionId=<markerID> → 405
	req, _ = http.NewRequest(http.MethodDelete, base+"/ver-bucket/obj.txt", nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	markerID := resp.Header.Get("x-amz-version-id")
	resp.Body.Close()
	require.NotEmpty(t, markerID)

	req, _ = http.NewRequest(http.MethodHead, base+"/ver-bucket/obj.txt?versionId="+markerID, nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
	assert.Equal(t, "true", resp.Header.Get("x-amz-delete-marker"))
}

// TestPutBucketVersioning_RejectsUnversioned verifies that "Unversioned" is
// rejected as a PUT status value (S3 spec allows only Enabled|Suspended).
func TestPutBucketVersioning_RejectsUnversioned(t *testing.T) {
	base := setupECTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	http.DefaultClient.Do(req) //nolint:errcheck

	body := `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Unversioned</Status></VersioningConfiguration>`
	req, _ = http.NewRequest(http.MethodPut, base+"/mybucket?versioning", strings.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}
