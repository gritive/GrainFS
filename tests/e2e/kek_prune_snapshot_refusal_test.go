package e2e

// Single-node operator e2e for KEK prune-refusal when a retained object
// snapshot is sealed under the target KEK version (Task 3, plan §2).
//
// Flow (single-node; all voters == self, so the local self-probe closure runs
// snapshot.CountSnapshotsSealedUnderKEK directly):
//
//   1. Start a KEK-enabled node (active = v0).
//   2. Rotate once: active = v1.  Snapshot taken ← sealed under v1.
//   3. Rotate again: active = v2.  v1 is now retire-eligible.
//   4. Retire v1 — succeeds (only prune is blocked, not retire).
//   5. Attempt prune v1 → MUST FAIL; error names the node + count.
//   6. Assert keys/1.key still on disk (not unlinked).
//   7. Delete the snapshot (DELETE /admin/snapshots/:seq).
//   8. Prune v1 → MUST SUCCEED now.
//   9. Assert keys/1.key gone and status == "pruned".
//
// NOTE: This exercises the real snapshotRefCountFn path wired in
// boot_phases_kek_rotation_leader.go. The self-shortcut closure calls
// snapshot.CountSnapshotsSealedUnderKEK(<dataDir>/snapshots, version),
// so a snapshot written by POST /admin/snapshots is found by prune-probe
// and causes the refusal. This is the wire path the handler-wire integration
// test (#1) proves at the codec level; this test proves the full stack
// (CLI → admin UDS → leader → self probe → scan → FSM re-check).

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// snapshotCreateResp mirrors the JSON response from POST /admin/snapshots.
type snapshotCreateResp struct {
	Seq uint64 `json:"seq"`
}

// startSingleKEKNodeWithHTTP is like startSingleKEKNode from kek_rotation_test.go
// but also returns the HTTP server URL so callers can POST /admin/snapshots.
func startSingleKEKNodeWithHTTP(t testing.TB) (dataDir, serverURL, clusterName string) {
	t.Helper()
	dir := shortTempDir(t)
	httpPort := freePort()
	raftPort := freePort()

	ctx, cancel := context.WithCancel(context.Background())
	ginkgo.DeferCleanup(cancel)

	args := []string{
		"serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", httpPort),
		"--raft-addr", fmt.Sprintf("127.0.0.1:%d", raftPort),
		"--node-id", "kek-prune-refusal",
		"--nfs4-port", "0",
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
	}
	logFile, err := os.CreateTemp("", "kek-prune-refusal-*.log")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.DeferCleanup(os.Remove, logFile.Name())

	srv := exec.CommandContext(ctx, getBinary(), args...)
	srv.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	srv.Stdout = logFile
	srv.Stderr = logFile
	gomega.Expect(srv.Start()).To(gomega.Succeed())
	ginkgo.DeferCleanup(func() {
		cancel()
		_ = srv.Wait()
	})

	url := fmt.Sprintf("http://127.0.0.1:%d", httpPort)
	waitHTTPReady(t, httpPort, 30*time.Second)
	waitSocketReady(t, filepath.Join(dir, "admin.sock"), 15*time.Second)
	return dir, url, kekClusterName(t, dir)
}

// createSnapshotReturningSeq posts to POST /admin/snapshots and returns the
// created snapshot's seq number. It retries for up to 30 s while the snapshot
// subsystem is warming up (same approach as createSnapshot).
func createSnapshotReturningSeq(serverURL string) uint64 {
	ginkgo.GinkgoHelper()
	var resp snapshotCreateResp
	gomega.Eventually(func() error {
		httpResp, err := postJSON(serverURL+"/admin/snapshots", map[string]string{"reason": "kek-prune-refusal-test"})
		if err != nil {
			return err
		}
		defer httpResp.Body.Close()
		if httpResp.StatusCode != http.StatusOK {
			return fmt.Errorf("POST /admin/snapshots returned %d", httpResp.StatusCode)
		}
		return json.NewDecoder(httpResp.Body).Decode(&resp)
	}, 30*time.Second, 500*time.Millisecond).Should(gomega.Succeed(),
		"snapshot should be created within 30s")
	return resp.Seq
}

// deleteSnapshot issues DELETE /admin/snapshots/:seq.
func deleteSnapshot(serverURL string, seq uint64) {
	ginkgo.GinkgoHelper()
	url := fmt.Sprintf("%s/admin/snapshots/%d", serverURL, seq)
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	cli := &http.Client{Timeout: 15 * time.Second}
	resp, err := cli.Do(req)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer resp.Body.Close()
	gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusOK),
		"DELETE /admin/snapshots/%d should succeed", seq)
}

var _ = ginkgo.Describe("KEK prune refusal: object snapshot", func() {
	ginkgo.Context("SingleNode", ginkgo.Ordered, func() {
		ginkgo.It("refuses to prune a KEK version a retained snapshot is sealed under", func() {
			t := ginkgo.GinkgoTB()
			dir, serverURL, nodeID := startSingleKEKNodeWithHTTP(t)

			// Step 1: rotate to v1.
			_, err := runKEKCLI(dir, "rotate", "--i-know")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			st := kekStatusViaSocket(t, dir)
			gomega.Expect(st.ActiveVersion).To(gomega.Equal(uint32(1)))

			// Step 2: create a snapshot — sealed under v1 (current active).
			createSnapshotReturningSeq(serverURL)

			// Confirm the snapshot file exists and is enveloped.
			pattern := filepath.Join(dir, "snapshots", "snapshot-*.json.zst")
			matches, err := filepath.Glob(pattern)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(matches).NotTo(gomega.BeEmpty(), "snapshot file must exist after creation")
			raw, err := os.ReadFile(matches[0])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(raw[:4]).To(gomega.Equal([]byte("GSNE")),
				"snapshot must be enveloped (GSNE magic)")

			// Step 3: rotate to v2 — v1 is now retire-eligible.
			_, err = runKEKCLI(dir, "rotate", "--i-know")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			st2 := kekStatusViaSocket(t, dir)
			gomega.Expect(st2.ActiveVersion).To(gomega.Equal(uint32(2)))

			// Step 4: retire v1 — should succeed (retire is not blocked).
			_, err = runKEKCLI(dir, "retire",
				"--version", "1",
				"--confirm-name", confirmName(1),
				"--cluster-name", nodeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Step 5: prune v1 — MUST FAIL (snapshot sealed under v1 is retained).
			_, pruneErr := runKEKCLI(dir, "prune",
				"--version", "1",
				"--confirm-name", confirmName(1),
				"--cluster-name", nodeID)
			gomega.Expect(pruneErr).To(gomega.HaveOccurred(),
				"prune must fail while a snapshot sealed under v1 is retained")
			gomega.Expect(pruneErr.Error()).To(gomega.ContainSubstring("snapshot"),
				"prune error must mention snapshot refusal")

			// Step 6: v1 key file must still be on disk.
			gomega.Expect(keystoreKeyExists(dir, 1)).To(gomega.BeTrue(),
				"keys/1.key must NOT be deleted when prune is refused")
		})

		ginkgo.It("allows prune after the snapshot is deleted", func() {
			t := ginkgo.GinkgoTB()
			dir, serverURL, nodeID := startSingleKEKNodeWithHTTP(t)

			// Setup: rotate v0→v1, snapshot (sealed under v1), rotate v1→v2,
			// retire v1, confirm prune is blocked.
			_, err := runKEKCLI(dir, "rotate", "--i-know")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			seq := createSnapshotReturningSeq(serverURL)

			_, err = runKEKCLI(dir, "rotate", "--i-know")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			_, err = runKEKCLI(dir, "retire",
				"--version", "1",
				"--confirm-name", confirmName(1),
				"--cluster-name", nodeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Verify prune is still blocked before we delete the snapshot.
			_, pruneErr := runKEKCLI(dir, "prune",
				"--version", "1",
				"--confirm-name", confirmName(1),
				"--cluster-name", nodeID)
			gomega.Expect(pruneErr).To(gomega.HaveOccurred(), "prune must fail before snapshot deleted")

			// Step 7: delete the snapshot.
			deleteSnapshot(serverURL, seq)

			// Snapshot file must be gone now.
			pattern := filepath.Join(dir, "snapshots", "snapshot-*.json.zst")
			remainingMatches, err := filepath.Glob(pattern)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(remainingMatches).To(gomega.BeEmpty(),
				"snapshot file must be removed after DELETE")

			// Step 8: prune v1 — MUST SUCCEED now.
			runKEKMutationGateRetry(dir, "prune",
				"--version", "1",
				"--confirm-name", confirmName(1),
				"--cluster-name", nodeID)

			// Step 9: key file gone, status == pruned.
			gomega.Expect(keystoreKeyExists(dir, 1)).To(gomega.BeFalse(),
				"keys/1.key must be unlinked after successful prune")
			st := kekStatusViaSocket(t, dir)
			v1, ok := st.version(1)
			gomega.Expect(ok).To(gomega.BeTrue())
			gomega.Expect(v1.Status).To(gomega.Equal("pruned"))
		})
	})
})
