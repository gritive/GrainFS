package e2e

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// CoW (copy-on-write) tests exercise the volume snapshot/rollback/clone CLI
// surface. The volume CLI talks to the admin UDS, which is per-node — for
// cluster targets the tests route through the leader's admin sock (the same
// path TestClusterStatusCLIE2E + TestClusterHealthCLIE2E use). All three
// tests are bucket-isolated (each uses a unique volName), so they share the
// fixture safely.

// cowSnapResp mirrors the snapshot response from POST /volumes/:name/snapshots.
type cowSnapResp struct {
	ID         string `json:"id"`
	CreatedAt  string `json:"created_at"`
	BlockCount int64  `json:"block_count"`
}

// cowDataDir returns the data directory of the writable node (single-node
// or cluster leader). volume CLI helpers take dataDir and derive the admin
// UDS path from it; this keeps the dual pattern symmetric.
func cowDataDir(tgt s3Target) string {
	return filepath.Dir(tgt.adminSockPath())
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

func cowCleanupVolume(t *testing.T, dataDir, name string) {
	t.Helper()
	out, code := runCLI(t, dataDir, "volume", "delete", name, "--force")
	if code == 0 || strings.Contains(out, "volume not found") {
		return
	}
	require.Equal(t, 0, code, out)
}

func cowCreateSnapshot(t *testing.T, dataDir, volName string) string {
	t.Helper()
	out, code := runCLI(t, dataDir, "volume", "snapshot", "create", volName, "--format", "json")
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
	out, code := runCLI(t, dataDir, "volume", "snapshot", "list", volName, "--format", "json")
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

func cowWriteAt(t *testing.T, dataDir, volName string, offset int64, content string) {
	t.Helper()
	out, code := runCLI(t, dataDir, "volume", "write-at", volName, "--offset", fmt.Sprintf("%d", offset), "--content", content)
	require.Equal(t, 0, code, out)
}

func cowReadAt(t *testing.T, dataDir, volName string, offset, length int64) string {
	t.Helper()
	out, code := runCLI(t, dataDir, "volume", "read-at", volName, "--offset", fmt.Sprintf("%d", offset), "--length", fmt.Sprintf("%d", length))
	require.Equal(t, 0, code, out)
	return out
}

// TestCoWE2E groups the three CoW CLI flows (snapshot rollback, list/delete,
// clone lifecycle independence) under one entry. Shared single + shared
// cluster fixtures; each sub-test picks a unique volume name.
func TestCoWE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runCoWCases(t, newSingleNodeS3Target())
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		runCoWCases(t, newSharedClusterS3Target(t))
	})
}

func runCoWCases(t *testing.T, tgt s3Target) {
	t.Helper()
	const volSize = 4 * 1024 * 1024
	dataDir := cowDataDir(tgt)

	t.Run("SnapshotRollbackRestoresData", func(t *testing.T) {
		volName := fmt.Sprintf("cow-rollback-vol-%d", time.Now().UnixNano())
		original := "cow-original-content"
		modified := "cow-modified-content"

		cowCreateVolume(t, dataDir, volName, volSize)
		t.Cleanup(func() { cowCleanupVolume(t, dataDir, volName) })

		cowWriteAt(t, dataDir, volName, 0, original)
		snapID := cowCreateSnapshot(t, dataDir, volName)

		cowWriteAt(t, dataDir, volName, 0, modified)
		got := cowReadAt(t, dataDir, volName, 0, int64(len(modified)))
		require.Equal(t, modified, got)

		cowRollback(t, dataDir, volName, snapID)
		got = cowReadAt(t, dataDir, volName, 0, int64(len(original)))
		require.Equal(t, original, got)
	})

	t.Run("SnapshotListAndDelete", func(t *testing.T) {
		volName := fmt.Sprintf("cow-snaplist-vol-%d", time.Now().UnixNano())

		cowCreateVolume(t, dataDir, volName, volSize)
		t.Cleanup(func() { cowCleanupVolume(t, dataDir, volName) })

		var ids []string
		for i := 0; i < 3; i++ {
			ids = append(ids, cowCreateSnapshot(t, dataDir, volName))
		}

		snaps := cowListSnapshots(t, dataDir, volName)
		require.Len(t, snaps, 3, "expected 3 snapshots after creation")

		cowDeleteSnapshot(t, dataDir, volName, ids[1])

		snaps = cowListSnapshots(t, dataDir, volName)
		require.Len(t, snaps, 2, "expected 2 snapshots after deleting one")

		for _, s := range snaps {
			require.NotEqual(t, ids[1], s.ID, "deleted snapshot must not appear in list")
		}
	})

	// CloneLifecycleIndependence verifies source and clone are independent at
	// the lifecycle level: deleting one does not delete the other. Full
	// block-data independence (write to clone, verify source unchanged)
	// requires NFS access to the cloned volume and is covered in NBD E2E.
	t.Run("CloneLifecycleIndependence", func(t *testing.T) {
		srcName := fmt.Sprintf("cow-clone-src-%d", time.Now().UnixNano())
		dstName := fmt.Sprintf("cow-clone-dst-%d", time.Now().UnixNano())
		original := "clone-original-content"
		modified := "clone-modified-content"

		cowCreateVolume(t, dataDir, srcName, volSize)
		srcDeleted := false
		t.Cleanup(func() {
			if !srcDeleted {
				cowDeleteVolume(t, dataDir, srcName)
			}
		})
		cowWriteAt(t, dataDir, srcName, 0, original)

		out, code := runCLI(t, dataDir, "volume", "clone", srcName, dstName)
		require.Equal(t, 0, code, out)
		t.Cleanup(func() { cowCleanupVolume(t, dataDir, dstName) })

		got := cowReadAt(t, dataDir, dstName, 0, int64(len(original)))
		require.Equal(t, original, got)

		cowWriteAt(t, dataDir, dstName, 0, modified)
		got = cowReadAt(t, dataDir, srcName, 0, int64(len(original)))
		require.Equal(t, original, got, "clone writes must not modify source")

		cowDeleteVolume(t, dataDir, srcName)
		srcDeleted = true
		out, code = runCLI(t, dataDir, "volume", "info", dstName)
		require.Equal(t, 0, code, "clone must survive deletion of its source: %s", out)
		got = cowReadAt(t, dataDir, dstName, 0, int64(len(modified)))
		require.Equal(t, modified, got)
	})
}
