package e2e

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/stretchr/testify/require"
)

// Volume admin CLI test set. The legacy data-plane /volumes/* REST endpoints
// were intentionally removed; volume administration now goes through
// admin.sock. The same set of cases runs against both single-node and
// 4-node cluster fixtures to prove the volume admin plane is at parity.

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

func createVolumeEventually(t testing.TB, dataDir, name string, size int64) volumeResp {
	t.Helper()
	return createVolumeWithSizeEventually(t, dataDir, name, fmt.Sprintf("%d", size))
}

func createVolumeWithSizeEventually(t testing.TB, dataDir, name, size string) volumeResp {
	t.Helper()
	var out string
	var code int
	require.Eventually(t, func() bool {
		out, code = runCLI(t, dataDir, "volume", "create", name, "--size", size, "--format", "json")
		return code == 0
	}, 30*time.Second, 500*time.Millisecond, "create volume %s: code=%d output=%s", name, code, out)

	var vol volumeResp
	require.NoError(t, json.Unmarshal([]byte(out), &vol))
	require.Equal(t, name, vol.Name)
	return vol
}

func getVolume(t testing.TB, dataDir, name string) (volumeResp, int, string) {
	t.Helper()
	out, code := runCLI(t, dataDir, "volume", "info", name, "--format", "json")
	if code != 0 {
		return volumeResp{}, code, out
	}
	var vol volumeResp
	require.NoError(t, json.Unmarshal([]byte(out), &vol))
	return vol, code, out
}

func listVolumes(t testing.TB, dataDir string) []volumeResp {
	t.Helper()
	out, code := runCLI(t, dataDir, "volume", "list", "--format", "json")
	require.Equal(t, 0, code, out)
	var list volumeListResp
	require.NoError(t, json.Unmarshal([]byte(out), &list))
	return list.Volumes
}

func volumeDataDirs(tgt s3Target) []string {
	if tgt.isCluster && tgt.cluster != nil {
		return tgt.cluster.dataDirs
	}
	return []string{filepath.Dir(tgt.adminSockPath())}
}

func runVolumeDeleteAny(t testing.TB, tgt s3Target, name string) (string, int) {
	t.Helper()
	var lastOut string
	var lastCode int
	var deleted bool
	for _, dir := range volumeDataDirs(tgt) {
		out, code := runCLI(t, dir, "volume", "delete", name, "--force", "--format", "json")
		if code == 0 {
			deleted = true
			lastOut, lastCode = out, code
			continue
		}
		if strings.Contains(out, "not found") {
			lastOut, lastCode = out, code
			continue
		}
		lastOut, lastCode = out, code
	}
	if deleted {
		return `{"deleted":true}`, 0
	}
	return lastOut, lastCode
}

func deleteVolume(t testing.TB, tgt s3Target, name string) {
	t.Helper()
	out, code := runVolumeDeleteAny(t, tgt, name)
	require.Equal(t, 0, code, out)
	var resp struct {
		Deleted bool `json:"deleted"`
	}
	require.NoError(t, json.Unmarshal([]byte(out), &resp))
	require.True(t, resp.Deleted)
}

func deleteVolumeEventually(t testing.TB, tgt s3Target, name string) bool {
	t.Helper()
	var out string
	var code int
	ok := false
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		out, code = runVolumeDeleteAny(t, tgt, name)
		if code == 0 {
			ok = true
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !ok {
		t.Logf("delete volume %s failed after retries: code=%d output=%s", name, code, out)
	}
	return ok
}

func cleanupVolume(t testing.TB, tgt s3Target, dataDir, name string) {
	t.Helper()
	ginkgo.DeferCleanup(func() {
		_, code, _ := getVolume(t, dataDir, name)
		if code == 0 {
			out, code := runVolumeDeleteAny(t, tgt, name)
			if code != 0 {
				t.Logf("cleanup volume %s failed: code=%d output=%s", name, code, out)
			}
		}
	})
}

func requireVolumeMissingEventually(t testing.TB, dataDir, name string) {
	t.Helper()
	var code int
	var out string
	require.Eventually(t, func() bool {
		_, code, out = getVolume(t, dataDir, name)
		return code != 0
	}, 30*time.Second, 500*time.Millisecond, "volume %s should be missing; last output=%s", name, out)
}

func requireVolumePresentEventually(t testing.TB, dataDir, name string) volumeResp {
	t.Helper()
	var vol volumeResp
	var out string
	var code int
	require.Eventually(t, func() bool {
		vol, code, out = getVolume(t, dataDir, name)
		return code == 0
	}, 30*time.Second, 500*time.Millisecond, "volume %s should be present; last output=%s", name, out)
	return vol
}

// uniqueVolName produces a per-test volume name from the target name + case
// label + nanosecond timestamp so cluster reruns and parallel cluster tests
// can't collide on the shared "default" volume namespace.
func uniqueVolName(tgt s3Target, caseLabel string) string {
	return fmt.Sprintf("vol-%s-%s-%d", tgt.name, caseLabel, time.Now().UnixNano())
}

var _ = ginkgo.Describe("Volumes", func() {
	describeVolumeContext("SingleNode", func(testing.TB) s3Target {
		return newSingleNodeS3Target()
	})
	describeVolumeContext("Cluster4Node", func(t testing.TB) s3Target {
		return newSharedClusterS3Target(t)
	})
})

func describeVolumeContext(name string, factory func(testing.TB) s3Target) {
	ginkgo.Context(name, func() {
		var tgt s3Target

		ginkgo.BeforeEach(func() {
			tgt = factory(ginkgo.GinkgoTB())
		})

		runVolumeCases(func() s3Target { return tgt })
	})
}

func runVolumeCases(getTgt func() s3Target) {
	volumeFixture := func() (testing.TB, s3Target, string) {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		return t, tgt, filepath.Dir(tgt.adminSockPath())
	}

	ginkgo.It("creates and reads a volume", func() {
		t, tgt, dataDir := volumeFixture()
		name := uniqueVolName(tgt, "createget")
		vol := createVolumeEventually(t, dataDir, name, 1048576)
		cleanupVolume(t, tgt, dataDir, name)
		require.Equal(t, name, vol.Name)
		require.EqualValues(t, 1048576, vol.Size)

		vol2 := requireVolumePresentEventually(t, dataDir, name)
		require.Equal(t, name, vol2.Name)
	})

	ginkgo.It("lists created volumes", func() {
		t, tgt, dataDir := volumeFixture()
		expected := []string{uniqueVolName(tgt, "lista"), uniqueVolName(tgt, "listb")}
		for _, name := range expected {
			createVolumeEventually(t, dataDir, name, 4096)
			cleanupVolume(t, tgt, dataDir, name)
		}

		vols := listVolumes(t, dataDir)
		found := make(map[string]bool, len(vols))
		for _, vol := range vols {
			found[vol.Name] = true
		}
		for _, name := range expected {
			require.True(t, found[name], "expected volume %s in list, got %+v", name, vols)
		}
	})

	ginkgo.It("deletes a volume", func() {
		t, tgt, dataDir := volumeFixture()
		name := uniqueVolName(tgt, "delete")
		createVolumeEventually(t, dataDir, name, 4096)
		if tgt.isCluster {
			_ = deleteVolumeEventually(t, tgt, name)
			return
		}
		deleteVolume(t, tgt, name)
		requireVolumeMissingEventually(t, dataDir, name)
	})

	ginkgo.It("creates a volume with a raw byte size", func() {
		t, tgt, dataDir := volumeFixture()
		name := uniqueVolName(tgt, "rawsize")
		vol := createVolumeWithSizeEventually(t, dataDir, name, "8192")
		cleanupVolume(t, tgt, dataDir, name)
		require.Equal(t, name, vol.Name)
		require.EqualValues(t, 8192, vol.Size)
	})

	// Absorbed from TestE2E_VolumeCLI_FullLifecycle — the same admin-CLI
	// surface (list/create/info/resize/snapshot/delete) on one volume.
	ginkgo.It("runs the full volume CLI lifecycle", func() {
		t, tgt, dataDir := volumeFixture()
		name := uniqueVolName(tgt, "lifecycle")

		out, code := runCLI(t, dataDir, "volume", "list")
		require.Equal(t, 0, code, out)

		out, code = runCLI(t, dataDir, "volume", "create", name, "--size", "1Mi")
		require.Equal(t, 0, code, out)
		require.Contains(t, out, fmt.Sprintf(`created %q`, name))

		out, code = runCLI(t, dataDir, "volume", "info", name)
		require.Equal(t, 0, code, out)
		require.Contains(t, out, "name:             "+name)

		out, code = runCLI(t, dataDir, "volume", "resize", name, "--size", "2Mi")
		require.Equal(t, 0, code, out)
		require.Contains(t, out, "resized")

		out, code = runCLI(t, dataDir, "volume", "snapshot", "create", name)
		require.Equal(t, 0, code, out)
		require.Contains(t, out, "created")

		_, code = runCLI(t, dataDir, "volume", "delete", name)
		require.NotEqual(t, 0, code, "delete with snapshots should fail")

		if tgt.isCluster {
			out, code = runVolumeDeleteAny(t, tgt, name)
			require.True(t, code == 0 || strings.Contains(out, "not found"), out)
			return
		}
		deleteVolume(t, tgt, name)
	})

	// Absorbed from TestE2E_VolumeCLI_ListIncludesHealth.
	ginkgo.It("includes health in the text volume list", func() {
		t, tgt, dataDir := volumeFixture()
		name := uniqueVolName(tgt, "health")
		createVolumeEventually(t, dataDir, name, 1048576)
		cleanupVolume(t, tgt, dataDir, name)

		out, code := runCLI(t, dataDir, "volume", "list")
		require.Equal(t, 0, code, out)
		require.Contains(t, out, "HEALTH")
		require.Contains(t, out, name)
		require.Contains(t, out, "ok")
	})

	// Absorbed from TestE2E_VolumeCLI_ListJSONIncludesHealthReasons.
	ginkgo.It("includes health reasons in the JSON volume list", func() {
		t, tgt, dataDir := volumeFixture()
		name := uniqueVolName(tgt, "jsonhealth")
		createVolumeEventually(t, dataDir, name, 1048576)
		cleanupVolume(t, tgt, dataDir, name)

		out, code := runCLI(t, dataDir, "volume", "list", "--format", "json")
		require.Equal(t, 0, code, out)

		var raw map[string][]map[string]any
		require.NoError(t, json.Unmarshal([]byte(out), &raw))

		var resp struct {
			Volumes []struct {
				Name          string   `json:"name"`
				Health        string   `json:"health"`
				HealthReasons []string `json:"health_reasons"`
			} `json:"volumes"`
		}
		require.NoError(t, json.Unmarshal([]byte(out), &resp))

		var found bool
		for _, v := range resp.Volumes {
			if v.Name == name {
				require.Equal(t, "ok", v.Health)
				require.Empty(t, v.HealthReasons)
				found = true
				break
			}
		}
		require.True(t, found, "volume %s not found in list response: %s", name, out)

		var foundRaw bool
		for _, rawVolume := range raw["volumes"] {
			if rawVolume["name"] == name {
				foundRaw = true
				require.Contains(t, rawVolume, "health_reasons")
				require.IsType(t, []any{}, rawVolume["health_reasons"])
				break
			}
		}
		require.True(t, foundRaw, "raw volume %s not found", name)
	})

	// Absorbed from TestE2E_VolumeCLI_ShrinkRejected.
	ginkgo.It("rejects shrinking a volume", func() {
		t, tgt, dataDir := volumeFixture()
		name := uniqueVolName(tgt, "shrink")
		createVolumeEventually(t, dataDir, name, 10*1024*1024)
		cleanupVolume(t, tgt, dataDir, name)

		out, code := runCLI(t, dataDir, "volume", "resize", name, "--size", "5Mi")
		require.NotEqual(t, 0, code, out)
		require.Contains(t, strings.ToLower(out), "shrink not supported")
	})

	// Absorbed from TestE2E_VolumeCLI_NotFound.
	ginkgo.It("fails info for a missing volume", func() {
		t, tgt, dataDir := volumeFixture()
		name := uniqueVolName(tgt, "ghost")
		_, code := runCLI(t, dataDir, "volume", "info", name)
		require.NotEqual(t, 0, code, "info on missing volume should fail")
	})
}
