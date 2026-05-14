package e2e

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

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

func TestE2E_NFSMultiExportCLI_Lifecycle(t *testing.T) {
	bucket := fmt.Sprintf("nfs-e2e-%d", freePort())
	createBucket(t, bucket)

	added := runNfsExportJSON(t, "add", bucket, "--ro")
	require.Equal(t, bucket, added.Bucket)
	require.True(t, added.ReadOnly)
	require.Equal(t, uint64(1), added.FsidMajor)
	require.NotZero(t, added.FsidMinor)
	require.Equal(t, uint64(1), added.Generation)

	list := listNfsExports(t)
	require.Contains(t, exportBuckets(list), bucket)

	updated := runNfsExportJSON(t, "update", bucket, "--rw")
	require.Equal(t, bucket, updated.Bucket)
	require.False(t, updated.ReadOnly)
	require.Greater(t, updated.Generation, added.Generation)
	require.Equal(t, added.FsidMinor, updated.FsidMinor, "fsid minor must remain stable across update")

	out, code := runCLI(t, testServerDataDir, "nfs", "export", "remove", bucket, "--quiet")
	require.Equal(t, 0, code, out)
	require.NotContains(t, exportBuckets(listNfsExports(t)), bucket)
}

func TestE2E_NFSMultiExportCLI_RejectsMissingBucket(t *testing.T) {
	missing := fmt.Sprintf("nfs-missing-%d", freePort())
	out, code := runCLI(t, testServerDataDir, "nfs", "export", "add", missing)
	require.NotEqual(t, 0, code)
	require.Contains(t, out, "bucket_not_found")
}

func runNfsExportJSON(t *testing.T, verb, bucket string, flags ...string) e2eNfsExport {
	t.Helper()
	args := []string{"nfs", "export", verb, bucket, "--format", "json"}
	args = append(args, flags...)
	out, code := runCLI(t, testServerDataDir, args...)
	require.Equalf(t, 0, code, "%s", out)
	return parseSingleNfsExport(t, out)
}

func parseSingleNfsExport(t *testing.T, raw string) e2eNfsExport {
	t.Helper()
	var resp e2eNfsExportList
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(raw)), &resp))
	require.Len(t, resp.Exports, 1)
	return resp.Exports[0]
}

func listNfsExports(t *testing.T) []e2eNfsExport {
	t.Helper()
	out, code := runCLI(t, testServerDataDir, "nfs", "export", "list", "--format", "json")
	require.Equalf(t, 0, code, "%s", out)
	return parseNfsExportList(t, out)
}

func parseNfsExportList(t *testing.T, raw string) []e2eNfsExport {
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
