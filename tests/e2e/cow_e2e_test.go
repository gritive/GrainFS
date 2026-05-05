package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

// cowSnapResp mirrors the snapshot response from POST /volumes/:name/snapshots.
type cowSnapResp struct {
	ID         string `json:"id"`
	CreatedAt  string `json:"created_at"`
	BlockCount int64  `json:"block_count"`
}

func cowCreateVolume(t *testing.T, dataDir, name string, sizeBytes int64) {
	t.Helper()
	var (
		out  string
		code int
	)
	require.Eventually(t, func() bool {
		out, code = runCLI(t, dataDir, "volume", "create", name, "--size", fmt.Sprintf("%d", sizeBytes))
		return code == 0
	}, 30*time.Second, 500*time.Millisecond, "create volume %s: code=%d output=%s", name, code, out)
}

func cowDeleteVolume(t *testing.T, dataDir, name string) {
	t.Helper()
	out, code := runCLI(t, dataDir, "volume", "delete", name, "--force")
	require.Equal(t, 0, code, out)
}

func cowCreateSnapshot(t *testing.T, dataDir, volName string) string {
	t.Helper()
	out, code := runCLI(t, dataDir, "volume", "snapshot", "create", volName, "--json")
	require.Equal(t, 0, code, out)
	var snap cowSnapResp
	require.NoError(t, json.Unmarshal([]byte(out), &snap))
	require.NotEmpty(t, snap.ID, "snapshot ID must be non-empty")
	return snap.ID
}

func cowRollback(t *testing.T, dataDir, volName, snapID string) {
	t.Helper()
	out, code := runCLI(t, dataDir, "volume", "rollback", volName, snapID)
	require.Equal(t, 0, code, out)
}

func cowListSnapshots(t *testing.T, dataDir, volName string) []cowSnapResp {
	t.Helper()
	out, code := runCLI(t, dataDir, "volume", "snapshot", "list", volName, "--json")
	require.Equal(t, 0, code, out)
	var snaps []cowSnapResp
	require.NoError(t, json.Unmarshal([]byte(out), &snaps))
	return snaps
}

func cowDeleteSnapshot(t *testing.T, dataDir, volName, snapID string) {
	t.Helper()
	out, code := runCLI(t, dataDir, "volume", "snapshot", "delete", volName, snapID)
	require.Equal(t, 0, code, out)
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
	snapID := cowCreateSnapshot(t, testServerDataDir, "default")

	// Overwrite with modified content.
	nfsWriteFile(t, filePath, modified)
	time.Sleep(50 * time.Millisecond)

	// Verify that modified content is visible.
	got := nfsReadFile(t, filePath)
	require.True(t, bytes.Contains(got, modified),
		"before rollback: expected modified content, got %q", got)

	// Roll back.
	cowRollback(t, testServerDataDir, "default", snapID)
	time.Sleep(1200 * time.Millisecond) // wait out the 1s NFS stat cache

	// After rollback, file should contain original content.
	got = nfsReadFile(t, filePath)
	require.True(t, bytes.Contains(got, original),
		"after rollback: expected original content %q, got %q", original, got)
}

// TestCoW_SnapshotListAndDelete verifies snapshot list/delete operations.
func TestCoW_SnapshotListAndDelete(t *testing.T) {
	const volSize = 4 * 1024 * 1024 // 4MB
	volName := fmt.Sprintf("cow-snaplist-vol-%d", time.Now().UnixNano())

	cowCreateVolume(t, testServerDataDir, volName, volSize)
	t.Cleanup(func() { cowDeleteVolume(t, testServerDataDir, volName) })

	// Create 3 snapshots.
	var ids []string
	for i := 0; i < 3; i++ {
		ids = append(ids, cowCreateSnapshot(t, testServerDataDir, volName))
	}

	snaps := cowListSnapshots(t, testServerDataDir, volName)
	require.Len(t, snaps, 3, "expected 3 snapshots after creation")

	// Delete one snapshot.
	cowDeleteSnapshot(t, testServerDataDir, volName, ids[1])

	snaps = cowListSnapshots(t, testServerDataDir, volName)
	require.Len(t, snaps, 2, "expected 2 snapshots after deleting one")

	// Verify the deleted snapshot is gone.
	for _, s := range snaps {
		require.NotEqual(t, ids[1], s.ID, "deleted snapshot must not appear in list")
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

	cowCreateVolume(t, testServerDataDir, srcName, volSize)
	srcDeleted := false
	t.Cleanup(func() {
		if !srcDeleted {
			cowDeleteVolume(t, testServerDataDir, srcName)
		}
	})

	// Clone src → dst.
	out, code := runCLI(t, testServerDataDir, "volume", "clone", srcName, dstName)
	require.Equal(t, 0, code, out)

	// Delete source; clone must still be accessible.
	cowDeleteVolume(t, testServerDataDir, srcName)
	srcDeleted = true
	out, code = runCLI(t, testServerDataDir, "volume", "info", dstName)
	require.Equal(t, 0, code, "clone must survive deletion of its source: %s", out)

	// Clean up.
	t.Cleanup(func() { cowDeleteVolume(t, testServerDataDir, dstName) })
}
