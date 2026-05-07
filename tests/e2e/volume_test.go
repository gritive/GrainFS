package e2e

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Volume admin CLI tests. The legacy data-plane /volumes/* REST endpoints were
// intentionally removed; volume administration now goes through admin.sock.

type volumeResp struct {
	Name            string `json:"name"`
	Size            int64  `json:"size"`
	BlockSize       int    `json:"block_size"`
	AllocatedBlocks int64  `json:"allocated_blocks"`
	AllocatedBytes  int64  `json:"allocated_bytes"`
	SnapshotCount   int32  `json:"snapshot_count"`
}

type volumeListResp struct {
	Volumes []volumeResp `json:"volumes"`
}

func createVolumeEventually(t *testing.T, name string, size int64) volumeResp {
	t.Helper()
	return createVolumeWithSizeEventually(t, name, fmt.Sprintf("%d", size))
}

func createVolumeWithSizeEventually(t *testing.T, name, size string) volumeResp {
	t.Helper()
	var out string
	var code int
	require.Eventually(t, func() bool {
		out, code = runCLI(t, testServerDataDir, "volume", "create", name, "--size", size, "--format", "json")
		return code == 0
	}, 30*time.Second, 500*time.Millisecond, "create volume %s: code=%d output=%s", name, code, out)

	var vol volumeResp
	require.NoError(t, json.Unmarshal([]byte(out), &vol))
	require.Equal(t, name, vol.Name)
	return vol
}

func getVolume(t *testing.T, name string) (volumeResp, int, string) {
	t.Helper()
	out, code := runCLI(t, testServerDataDir, "volume", "info", name, "--format", "json")
	if code != 0 {
		return volumeResp{}, code, out
	}
	var vol volumeResp
	require.NoError(t, json.Unmarshal([]byte(out), &vol))
	return vol, code, out
}

func listVolumes(t *testing.T) []volumeResp {
	t.Helper()
	out, code := runCLI(t, testServerDataDir, "volume", "list", "--format", "json")
	require.Equal(t, 0, code, out)
	var list volumeListResp
	require.NoError(t, json.Unmarshal([]byte(out), &list))
	return list.Volumes
}

func deleteVolume(t *testing.T, name string) {
	t.Helper()
	out, code := runCLI(t, testServerDataDir, "volume", "delete", name, "--force", "--format", "json")
	require.Equal(t, 0, code, out)
	var resp struct {
		Deleted bool `json:"deleted"`
	}
	require.NoError(t, json.Unmarshal([]byte(out), &resp))
	require.True(t, resp.Deleted)
}

func deleteVolumeEventually(t *testing.T, name string) {
	t.Helper()
	var out string
	var code int
	require.Eventually(t, func() bool {
		out, code = runCLI(t, testServerDataDir, "volume", "delete", name, "--force", "--format", "json")
		return code == 0
	}, 30*time.Second, 500*time.Millisecond, "delete volume %s: code=%d output=%s", name, code, out)
}

func cleanupVolume(t *testing.T, name string) {
	t.Helper()
	t.Cleanup(func() {
		_, code, _ := getVolume(t, name)
		if code == 0 {
			deleteVolumeEventually(t, name)
		}
	})
}

func requireVolumeMissingEventually(t *testing.T, name string) {
	t.Helper()
	var code int
	var out string
	require.Eventually(t, func() bool {
		_, code, out = getVolume(t, name)
		return code != 0
	}, 30*time.Second, 500*time.Millisecond, "volume %s should be missing; last output=%s", name, out)
}

func requireVolumePresentEventually(t *testing.T, name string) volumeResp {
	t.Helper()
	var vol volumeResp
	var out string
	var code int
	require.Eventually(t, func() bool {
		vol, code, out = getVolume(t, name)
		return code == 0
	}, 30*time.Second, 500*time.Millisecond, "volume %s should be present; last output=%s", name, out)
	return vol
}

func TestVolume_CreateAndGet(t *testing.T) {
	vol := createVolumeEventually(t, "test-vol-1", 1048576)
	cleanupVolume(t, "test-vol-1")
	require.Equal(t, "test-vol-1", vol.Name)
	require.EqualValues(t, 1048576, vol.Size)

	vol2 := requireVolumePresentEventually(t, "test-vol-1")
	require.Equal(t, "test-vol-1", vol2.Name)
}

func TestVolume_List(t *testing.T) {
	expected := []string{"list-vol-a", "list-vol-b"}
	for _, name := range expected {
		createVolumeEventually(t, name, 4096)
		cleanupVolume(t, name)
	}

	vols := listVolumes(t)
	found := make(map[string]bool, len(vols))
	for _, vol := range vols {
		found[vol.Name] = true
	}
	for _, name := range expected {
		require.True(t, found[name], "expected volume %s in list, got %+v", name, vols)
	}
}

func TestVolume_Delete(t *testing.T) {
	createVolumeEventually(t, "del-vol", 4096)
	deleteVolume(t, "del-vol")
	requireVolumeMissingEventually(t, "del-vol")
}

func TestVolume_CreateWithRawByteSize(t *testing.T) {
	vol := createVolumeWithSizeEventually(t, "raw-size-vol", "8192")
	cleanupVolume(t, "raw-size-vol")
	require.Equal(t, "raw-size-vol", vol.Name)
	require.EqualValues(t, 8192, vol.Size)
}
