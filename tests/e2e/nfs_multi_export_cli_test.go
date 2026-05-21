package e2e

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
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
		gomega.Expect(added.Bucket).To(gomega.Equal(bucket))
		gomega.Expect(added.ReadOnly).To(gomega.BeTrue())
		gomega.Expect(added.FsidMajor).To(gomega.Equal(uint64(1)))
		gomega.Expect(added.FsidMinor).NotTo(gomega.BeZero())
		gomega.Expect(added.Generation).To(gomega.Equal(uint64(1)))

		list := listNfsExports(t, dataDir)
		gomega.Expect(exportBuckets(list)).To(gomega.ContainElement(bucket))

		updated := runNfsExportJSON(t, dataDir, "update", bucket, "--rw")
		gomega.Expect(updated.Bucket).To(gomega.Equal(bucket))
		gomega.Expect(updated.ReadOnly).To(gomega.BeFalse())
		gomega.Expect(updated.Generation).To(gomega.BeNumerically(">", added.Generation))
		gomega.Expect(updated.FsidMinor).To(gomega.Equal(added.FsidMinor), "fsid minor must remain stable across update")

		out, code := runCLI(t, dataDir, "nfs", "export", "remove", bucket, "--quiet")
		gomega.Expect(code).To(gomega.Equal(0), out)
		gomega.Expect(exportBuckets(listNfsExports(t, dataDir))).NotTo(gomega.ContainElement(bucket))
	})

	ginkgo.It("rejects a missing bucket", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		dataDir := filepath.Dir(tgt.adminSockPath())
		missing := fmt.Sprintf("nfs-missing-%d", freePort())
		out, code := runCLI(t, dataDir, "nfs", "export", "add", missing)
		gomega.Expect(code).NotTo(gomega.Equal(0))
		gomega.Expect(out).To(gomega.ContainSubstring("bucket_not_found"))
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
	gomega.Expect(code).To(gomega.Equal(0), "%s", out)
	return parseSingleNfsExport(t, out)
}

func parseSingleNfsExport(t testing.TB, raw string) e2eNfsExport {
	t.Helper()
	var resp e2eNfsExportList
	gomega.Expect(json.Unmarshal([]byte(strings.TrimSpace(raw)), &resp)).To(gomega.Succeed())
	gomega.Expect(resp.Exports).To(gomega.HaveLen(1))
	return resp.Exports[0]
}

func listNfsExports(t testing.TB, dataDir string) []e2eNfsExport {
	t.Helper()
	out, code := runCLI(t, dataDir, "nfs", "export", "list", "--json")
	gomega.Expect(code).To(gomega.Equal(0), "%s", out)
	return parseNfsExportList(t, out)
}

func parseNfsExportList(t testing.TB, raw string) []e2eNfsExport {
	t.Helper()
	var resp e2eNfsExportList
	gomega.Expect(json.Unmarshal([]byte(strings.TrimSpace(raw)), &resp)).To(gomega.Succeed())
	return resp.Exports
}

func exportBuckets(exports []e2eNfsExport) []string {
	out := make([]string, 0, len(exports))
	for _, e := range exports {
		out = append(out, e.Bucket)
	}
	return out
}
