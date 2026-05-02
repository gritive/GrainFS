package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cowSnapResp mirrors the snapshot response from POST /volumes/:name/snapshots.
type cowSnapResp struct {
	ID         string    `json:"id"`
	CreatedAt  time.Time `json:"created_at"`
	BlockCount int64     `json:"block_count"`
}

func cowCreateVolume(t *testing.T, name string, sizeBytes int64) {
	t.Helper()
	url := fmt.Sprintf("%s/volumes/%s?size=%d", testServerURL, name, sizeBytes)
	var (
		statusCode int
		body       []byte
		err        error
	)
	require.Eventually(t, func() bool {
		req, _ := http.NewRequest(http.MethodPut, url, nil)
		var resp *http.Response
		resp, err = http.DefaultClient.Do(req)
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		statusCode = resp.StatusCode
		body, _ = io.ReadAll(resp.Body)
		return statusCode == http.StatusCreated
	}, 30*time.Second, 500*time.Millisecond, "create volume %s: status=%d body=%s err=%v", name, statusCode, string(body), err)
}

func cowDeleteVolume(t *testing.T, name string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodDelete, testServerURL+"/volumes/"+name, nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
}

func cowCreateSnapshot(t *testing.T, volName string) string {
	t.Helper()
	resp, err := http.Post(testServerURL+"/volumes/"+volName+"/snapshots", "application/json", nil) //nolint:noctx
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode, "create snapshot for %s", volName)

	var snap cowSnapResp
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&snap))
	require.NotEmpty(t, snap.ID, "snapshot ID must be non-empty")
	return snap.ID
}

func cowRollback(t *testing.T, volName, snapID string) {
	t.Helper()
	url := fmt.Sprintf("%s/volumes/%s/snapshots/%s/rollback", testServerURL, volName, snapID)
	resp, err := http.Post(url, "application/json", nil) //nolint:noctx
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode, "rollback %s → %s", volName, snapID)
}

func cowListSnapshots(t *testing.T, volName string) []cowSnapResp {
	t.Helper()
	resp, err := http.Get(testServerURL + "/volumes/" + volName + "/snapshots") //nolint:noctx
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var snaps []cowSnapResp
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&snaps))
	return snaps
}

func cowDeleteSnapshot(t *testing.T, volName, snapID string) {
	t.Helper()
	url := fmt.Sprintf("%s/volumes/%s/snapshots/%s", testServerURL, volName, snapID)
	req, _ := http.NewRequest(http.MethodDelete, url, nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
}

// nfsWriteFile writes content to the "default" bucket via S3, simulating an NFS write.
// path must start with "/" — the leading slash is stripped to form the S3 key.
func nfsWriteFile(t *testing.T, path string, content []byte) {
	t.Helper()
	ctx := context.Background()
	key := strings.TrimPrefix(path, "/")
	_, err := testS3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("default"),
		Key:    aws.String(key),
		Body:   bytes.NewReader(content),
	})
	require.NoError(t, err, "S3 PutObject: %s", path)
}

// nfsReadFile reads content from the "default" bucket via S3.
// Returns nil if the object does not exist.
func nfsReadFile(t *testing.T, path string) []byte {
	t.Helper()
	ctx := context.Background()
	key := strings.TrimPrefix(path, "/")
	resp, err := testS3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("default"),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	buf, _ := io.ReadAll(resp.Body)
	return buf
}

// TestCoW_SnapshotRollbackRestoresData verifies the full snapshot+rollback cycle:
// write → snapshot → overwrite → rollback → original content restored.
//
// Uses the shared "default" NFS volume. Writes to a uniquely-named file so
// concurrent (sequential) NFS tests don't interfere.
func TestCoW_SnapshotRollbackRestoresData(t *testing.T) {
	// TODO: volume snapshot rollback is block-level; S3 object writes are not
	// reflected by rollback. This test requires block-level write access
	// (previously via NFSv3, removed in #79). Re-enable once volume snapshot
	// rollback is wired to the S3/VFS write path.
	t.Skip("block-level rollback not reflected in S3 object writes — needs investigation")

	const filePath = "/cow-rollback-test.txt"
	original := []byte("cow-original-content")
	modified := []byte("cow-modified-content")

	// Write original content.
	nfsWriteFile(t, filePath, original)
	time.Sleep(50 * time.Millisecond)

	// Create snapshot of the "default" volume.
	snapID := cowCreateSnapshot(t, "default")

	// Overwrite with modified content.
	nfsWriteFile(t, filePath, modified)
	time.Sleep(50 * time.Millisecond)

	// Verify that modified content is visible.
	got := nfsReadFile(t, filePath)
	require.True(t, bytes.Contains(got, modified),
		"before rollback: expected modified content, got %q", got)

	// Roll back.
	cowRollback(t, "default", snapID)
	time.Sleep(1200 * time.Millisecond) // wait out the 1s NFS stat cache

	// After rollback, file should contain original content.
	got = nfsReadFile(t, filePath)
	assert.True(t, bytes.Contains(got, original),
		"after rollback: expected original content %q, got %q", original, got)
}

// TestCoW_SnapshotListAndDelete verifies snapshot list/delete operations.
func TestCoW_SnapshotListAndDelete(t *testing.T) {
	const volSize = 4 * 1024 * 1024 // 4MB
	volName := fmt.Sprintf("cow-snaplist-vol-%d", time.Now().UnixNano())

	cowCreateVolume(t, volName, volSize)
	t.Cleanup(func() { cowDeleteVolume(t, volName) })

	// Create 3 snapshots.
	var ids []string
	for i := 0; i < 3; i++ {
		ids = append(ids, cowCreateSnapshot(t, volName))
	}

	snaps := cowListSnapshots(t, volName)
	require.Len(t, snaps, 3, "expected 3 snapshots after creation")

	// Delete one snapshot.
	cowDeleteSnapshot(t, volName, ids[1])

	snaps = cowListSnapshots(t, volName)
	assert.Len(t, snaps, 2, "expected 2 snapshots after deleting one")

	// Verify the deleted snapshot is gone.
	for _, s := range snaps {
		assert.NotEqual(t, ids[1], s.ID, "deleted snapshot must not appear in list")
	}
}

// TestCoW_CloneLifecycleIndependence verifies that source and clone are
// independent at the lifecycle level: deleting one does not delete the other.
//
// Note: full block-data independence (write to clone, verify source unchanged)
// requires NFS access to the cloned volume and is covered in Step 3 (NBD/Docker E2E).
func TestCoW_CloneLifecycleIndependence(t *testing.T) {
	const volSize = 4 * 1024 * 1024
	srcName := fmt.Sprintf("cow-clone-src-%d", time.Now().UnixNano())
	dstName := fmt.Sprintf("cow-clone-dst-%d", time.Now().UnixNano())

	cowCreateVolume(t, srcName, volSize)
	t.Cleanup(func() { cowDeleteVolume(t, srcName) })

	// Clone src → dst.
	body := fmt.Sprintf(`{"src":%q,"dst":%q}`, srcName, dstName)
	resp, err := http.Post(testServerURL+"/volumes/clone", "application/json", //nolint:noctx
		bytes.NewBufferString(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode, "clone %s → %s", srcName, dstName)

	// Delete source; clone must still be accessible.
	cowDeleteVolume(t, srcName)
	resp2, err := http.Get(testServerURL + "/volumes/" + dstName) //nolint:noctx
	require.NoError(t, err)
	defer resp2.Body.Close()
	assert.Equal(t, http.StatusOK, resp2.StatusCode,
		"clone must survive deletion of its source")

	// Clean up.
	t.Cleanup(func() { cowDeleteVolume(t, dstName) })
}
