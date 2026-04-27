package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

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
	req, _ := http.NewRequest(http.MethodPut, url, nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode, "create volume %s", name)
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

// nfsWriteFile writes content to path on the NFS target, then flushes and closes.
func nfsWriteFile(t *testing.T, path string, content []byte) {
	t.Helper()
	target := dialNFSTarget(t)
	wr, err := target.OpenFile(path, 0644)
	require.NoError(t, err, "NFS open for write: %s", path)
	_, err = wr.Write(content)
	require.NoError(t, err, "NFS write: %s", path)
	wr.Close()
}

// nfsReadFile reads content from path on the NFS target.
// Returns nil if the file does not exist.
func nfsReadFile(t *testing.T, path string) []byte {
	t.Helper()
	target := dialNFSTarget(t)
	rd, err := target.Open(path)
	if err != nil {
		return nil
	}
	buf, _ := io.ReadAll(rd)
	rd.Close()
	return buf
}

// TestCoW_SnapshotRollbackRestoresData verifies the full snapshot+rollback cycle:
// write → snapshot → overwrite → rollback → original content restored.
//
// Uses the shared "default" NFS volume. Writes to a uniquely-named file so
// concurrent (sequential) NFS tests don't interfere.
func TestCoW_SnapshotRollbackRestoresData(t *testing.T) {
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
	const volName = "cow-snaplist-vol"
	const volSize = 4 * 1024 * 1024 // 4MB

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
	const srcName = "cow-clone-src"
	const dstName = "cow-clone-dst"
	const volSize = 4 * 1024 * 1024

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
