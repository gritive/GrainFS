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

func cowCreateVolume(t testing.TB, dataDir, name string, sizeBytes int64) {
	t.Helper()
	var (
		out  string
		code int
	)
	gomega.Eventually(func() bool {
		out, code = runCLI(t, dataDir, "volume", "create", name, "--size", fmt.Sprintf("%d", sizeBytes))
		return code == 0
	}).WithTimeout(30*time.Second).WithPolling(500*time.Millisecond).Should(gomega.BeTrue(), "create volume %s: code=%d output=%s", name, code, out)
}

func cowDeleteVolume(t testing.TB, dataDir, name string) {
	t.Helper()
	var (
		out  string
		code int
	)
	gomega.Eventually(func() bool {
		out, code = runCLI(t, dataDir, "volume", "delete", name, "--force")
		return code == 0 || strings.Contains(out, "volume not found")
	}).WithTimeout(30*time.Second).WithPolling(500*time.Millisecond).Should(gomega.BeTrue(), "delete volume %s: code=%d output=%s", name, code, out)
}

func cowCleanupVolume(t testing.TB, dataDir, name string) {
	t.Helper()
	var (
		out  string
		code int
	)
	deadline := time.Now().Add(5 * time.Second)
	for {
		out, code = runCLI(t, dataDir, "volume", "delete", name, "--force")
		if code == 0 || strings.Contains(out, "volume not found") {
			return
		}
		if time.Now().After(deadline) {
			t.Logf("cleanup volume %s failed after retries: code=%d output=%s", name, code, out)
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func cowCreateSnapshot(t testing.TB, dataDir, volName string) string {
	t.Helper()
	out, code := runCLI(t, dataDir, "volume", "snapshot", "create", volName, "--format", "json")
	gomega.Expect(code).To(gomega.Equal(0), out)
	var snap cowSnapResp
	gomega.Expect(json.Unmarshal([]byte(out), &snap)).To(gomega.Succeed())
	gomega.Expect(snap.ID).NotTo(gomega.BeEmpty(), "snapshot ID must be non-empty")
	return snap.ID
}

func cowRollback(t testing.TB, dataDir, volName, snapID string) {
	t.Helper()
	out, code := runCLI(t, dataDir, "volume", "rollback", volName, snapID)
	gomega.Expect(code).To(gomega.Equal(0), out)
}

func cowListSnapshots(t testing.TB, dataDir, volName string) []cowSnapResp {
	t.Helper()
	out, code := runCLI(t, dataDir, "volume", "snapshot", "list", volName, "--format", "json")
	gomega.Expect(code).To(gomega.Equal(0), out)
	var snaps []cowSnapResp
	gomega.Expect(json.Unmarshal([]byte(out), &snaps)).To(gomega.Succeed())
	return snaps
}

func cowDeleteSnapshot(t testing.TB, dataDir, volName, snapID string) {
	t.Helper()
	out, code := runCLI(t, dataDir, "volume", "snapshot", "delete", volName, snapID)
	gomega.Expect(code).To(gomega.Equal(0), out)
}

func cowWriteAt(t testing.TB, dataDir, volName string, offset int64, content string) {
	t.Helper()
	out, code := runCLI(t, dataDir, "volume", "write-at", volName, "--offset", fmt.Sprintf("%d", offset), "--content", content)
	gomega.Expect(code).To(gomega.Equal(0), out)
}

func cowReadAt(t testing.TB, dataDir, volName string, offset, length int64) string {
	t.Helper()
	out, code := runCLI(t, dataDir, "volume", "read-at", volName, "--offset", fmt.Sprintf("%d", offset), "--length", fmt.Sprintf("%d", length))
	gomega.Expect(code).To(gomega.Equal(0), out)
	return out
}

var _ = ginkgo.Describe("CoW volumes", func() {
	describeCoWContext("SingleNode", func(testing.TB) s3Target {
		return newSingleNodeS3Target()
	})
	describeCoWContext("Cluster4Node", func(t testing.TB) s3Target {
		return newSharedClusterS3Target(t)
	})
})

func describeCoWContext(name string, factory func(testing.TB) s3Target) {
	ginkgo.Context(name, func() {
		var tgt s3Target

		ginkgo.BeforeEach(func() {
			tgt = factory(ginkgo.GinkgoTB())
		})

		runCoWCases(func() s3Target { return tgt })
	})
}

func runCoWCases(getTgt func() s3Target) {
	const volSize = 4 * 1024 * 1024

	ginkgo.It("restores volume data after snapshot rollback", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		dataDir := func() string { return cowDataDir(tgt) }
		volName := fmt.Sprintf("cow-rollback-vol-%d", time.Now().UnixNano())
		original := "cow-original-content"
		modified := "cow-modified-content"

		cowCreateVolume(t, dataDir(), volName, volSize)
		ginkgo.DeferCleanup(cowCleanupVolume, t, dataDir(), volName)

		cowWriteAt(t, dataDir(), volName, 0, original)
		snapID := cowCreateSnapshot(t, dataDir(), volName)

		cowWriteAt(t, dataDir(), volName, 0, modified)
		got := cowReadAt(t, dataDir(), volName, 0, int64(len(modified)))
		gomega.Expect(got).To(gomega.Equal(modified))

		cowRollback(t, dataDir(), volName, snapID)
		got = cowReadAt(t, dataDir(), volName, 0, int64(len(original)))
		gomega.Expect(got).To(gomega.Equal(original))
	})

	ginkgo.It("lists and deletes snapshots", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		dataDir := func() string { return cowDataDir(tgt) }
		volName := fmt.Sprintf("cow-snaplist-vol-%d", time.Now().UnixNano())

		cowCreateVolume(t, dataDir(), volName, volSize)
		ginkgo.DeferCleanup(cowCleanupVolume, t, dataDir(), volName)

		var ids []string
		for i := 0; i < 3; i++ {
			ids = append(ids, cowCreateSnapshot(t, dataDir(), volName))
		}

		snaps := cowListSnapshots(t, dataDir(), volName)
		gomega.Expect(snaps).To(gomega.HaveLen(3), "expected 3 snapshots after creation")

		cowDeleteSnapshot(t, dataDir(), volName, ids[1])

		snaps = cowListSnapshots(t, dataDir(), volName)
		gomega.Expect(snaps).To(gomega.HaveLen(2), "expected 2 snapshots after deleting one")

		for _, s := range snaps {
			gomega.Expect(s.ID).NotTo(gomega.Equal(ids[1]), "deleted snapshot must not appear in list")
		}
	})

	// CloneLifecycleIndependence verifies source and clone are independent at
	// the lifecycle level: deleting one does not delete the other. Full
	// block-data independence (write to clone, verify source unchanged)
	// requires NFS access to the cloned volume and is covered in NBD E2E.
	ginkgo.It("keeps clone lifecycle independent from the source", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		dataDir := func() string { return cowDataDir(tgt) }
		srcName := fmt.Sprintf("cow-clone-src-%d", time.Now().UnixNano())
		dstName := fmt.Sprintf("cow-clone-dst-%d", time.Now().UnixNano())
		original := "clone-original-content"
		modified := "clone-modified-content"

		cowCreateVolume(t, dataDir(), srcName, volSize)
		srcDeleted := false
		ginkgo.DeferCleanup(func() {
			if !srcDeleted {
				cowDeleteVolume(t, dataDir(), srcName)
			}
		})
		cowWriteAt(t, dataDir(), srcName, 0, original)

		out, code := runCLI(t, dataDir(), "volume", "clone", srcName, dstName)
		gomega.Expect(code).To(gomega.Equal(0), out)
		ginkgo.DeferCleanup(cowCleanupVolume, t, dataDir(), dstName)

		got := cowReadAt(t, dataDir(), dstName, 0, int64(len(original)))
		gomega.Expect(got).To(gomega.Equal(original))

		cowWriteAt(t, dataDir(), dstName, 0, modified)
		got = cowReadAt(t, dataDir(), srcName, 0, int64(len(original)))
		gomega.Expect(got).To(gomega.Equal(original), "clone writes must not modify source")

		cowDeleteVolume(t, dataDir(), srcName)
		srcDeleted = true
		out, code = runCLI(t, dataDir(), "volume", "info", dstName)
		gomega.Expect(code).To(gomega.Equal(0), "clone must survive deletion of its source: %s", out)
		got = cowReadAt(t, dataDir(), dstName, 0, int64(len(modified)))
		gomega.Expect(got).To(gomega.Equal(modified))
	})
}
