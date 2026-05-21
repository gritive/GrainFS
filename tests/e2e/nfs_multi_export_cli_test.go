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

type e2eNfsExport struct {
	Bucket     string `json:"bucket"`
	ReadOnly   bool   `json:"read_only"`
	FsidMajor  uint64 `json:"fsid_major"`
	FsidMinor  uint64 `json:"fsid_minor"`
	Generation uint64 `json:"generation"`
}

type e2eNfsExportList struct {
	Exports []e2eNfsExport `json:"exports"`
}

// TestNFSMultiExportCLIE2E exercises the `grainfs nfs export` admin CLI
// surface (add/list/update/remove + missing-bucket rejection). Shared
// single + shared cluster fixtures — sub-tests pick unique bucket names.
var _ = ginkgo.Describe("NFS multi-export CLI", func() {
	for _, tc := range []struct {
		name string
		mk   func() s3Target
	}{
		{name: "SingleNode", mk: newSingleNodeS3Target},
		{name: "Cluster4Node", mk: func() s3Target { return newSharedClusterS3Target(ginkgo.GinkgoTB()) }},
	} {
		tc := tc
		ginkgo.Context(tc.name, func() {
			var tgt s3Target

			ginkgo.BeforeEach(func() {
				tgt = tc.mk()
			})

			runNFSMultiExportCLICases(func() s3Target { return tgt })
		})
	}
})

func runNFSMultiExportCLICases(getTgt func() s3Target) {
	ginkgo.It("adds, lists, updates, and removes an export", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		dataDir := filepath.Dir(tgt.adminSockPath())
		bucket := tgt.uniqueBucket(t, "nfsexp")

		added := runNfsExportJSON(t, dataDir, "add", bucket, "--ro")
		require.Equal(t, bucket, added.Bucket)
		require.True(t, added.ReadOnly)
		require.Equal(t, uint64(1), added.FsidMajor)
		require.NotZero(t, added.FsidMinor)
		require.Equal(t, uint64(1), added.Generation)

		list := listNfsExports(t, dataDir)
		require.Contains(t, exportBuckets(list), bucket)

		updated := runNfsExportJSON(t, dataDir, "update", bucket, "--rw")
		require.Equal(t, bucket, updated.Bucket)
		require.False(t, updated.ReadOnly)
		require.Greater(t, updated.Generation, added.Generation)
		require.Equal(t, added.FsidMinor, updated.FsidMinor, "fsid minor must remain stable across update")

		out, code := runCLI(t, dataDir, "nfs", "export", "remove", bucket, "--quiet")
		require.Equal(t, 0, code, out)
		require.NotContains(t, exportBuckets(listNfsExports(t, dataDir)), bucket)
	})

	ginkgo.It("rejects a missing bucket", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		dataDir := filepath.Dir(tgt.adminSockPath())
		missing := fmt.Sprintf("nfs-missing-%d", freePort())
		out, code := runCLI(t, dataDir, "nfs", "export", "add", missing)
		require.NotEqual(t, 0, code)
		require.Contains(t, out, "bucket_not_found")
	})
}

func runNfsExportJSON(t testing.TB, dataDir, verb, bucket string, flags ...string) e2eNfsExport {
	t.Helper()
	args := []string{"nfs", "export", verb, bucket, "--json"}
	args = append(args, flags...)
	var out string
	var code int
	deadline := time.Now().Add(30 * time.Second)
	for {
		out, code = runCLI(t, dataDir, args...)
		if code == 0 || !strings.Contains(out, "finish the rolling upgrade") || time.Now().After(deadline) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	require.Equalf(t, 0, code, "%s", out)
	return parseSingleNfsExport(t, out)
}

func parseSingleNfsExport(t testing.TB, raw string) e2eNfsExport {
	t.Helper()
	var resp e2eNfsExportList
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(raw)), &resp))
	require.Len(t, resp.Exports, 1)
	return resp.Exports[0]
}

func listNfsExports(t testing.TB, dataDir string) []e2eNfsExport {
	t.Helper()
	out, code := runCLI(t, dataDir, "nfs", "export", "list", "--json")
	require.Equalf(t, 0, code, "%s", out)
	return parseNfsExportList(t, out)
}

func parseNfsExportList(t testing.TB, raw string) []e2eNfsExport {
	t.Helper()
	var resp e2eNfsExportList
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(raw)), &resp))
	return resp.Exports
}

func exportBuckets(exports []e2eNfsExport) []string {
	out := make([]string, 0, len(exports))
	for _, e := range exports {
		out = append(out, e.Bucket)
	}
	return out
}
