package e2e

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Cluster PSK", func() {
	ginkgo.Context("SingleNode", func() {
		ginkgo.It("refuses an empty cluster key in join mode", func() {
			t := ginkgo.GinkgoTB()

			dir := t.TempDir()
			encKeyFile := makeSharedEncryptionKeyFile(t)
			port := freePort()
			raft := freePort()

			// Write .join-pending to trigger join mode (which requires --cluster-key).
			gomega.Expect(os.WriteFile(
				fmt.Sprintf("%s/%s", dir, joinPendingFile),
				[]byte(fmt.Sprintf("127.0.0.1:%d", freePort())), 0o600)).To(gomega.Succeed())

			cmd := exec.Command(getBinary(), "serve",
				"--data", dir,
				"--port", fmt.Sprintf("%d", port),
				"--raft-addr", fmt.Sprintf("127.0.0.1:%d", raft),
				"--node-id", "n-no-key",
				"--nfs4-port", "0",
				"--nbd-port", "0",
				"--encryption-key-file", encKeyFile,
			)
			cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			out, err := cmd.CombinedOutput()
			gomega.Expect(err).To(gomega.HaveOccurred(), "process must exit non-zero without --cluster-key")
			if !strings.Contains(string(out), "--cluster-key is required") {
				t.Fatalf("expected '--cluster-key is required' in output, got:\n%s", string(out))
			}
		})
	})

	ginkgo.Context("Cluster3Node", func() {
		ginkgo.It("rejects a joiner with a different PSK", func() {
			t := ginkgo.GinkgoTB()

			keyA := strings.Repeat("a", 64)
			keyB := strings.Repeat("b", 64)

			leaderDataDir := shortTempDir(t)
			joinerDataDir := shortTempDir(t)
			encKeyFile := makeSharedEncryptionKeyFile(t)

			leaderHTTP := freePort()
			leaderRaft := freePort()
			joinerHTTP := freePort()
			joinerRaft := freePort()

			// Start leader with keyA (solo bootstrap).
			leaderCtx, leaderCancel := context.WithCancel(context.Background())

			leaderArgs := []string{
				"serve",
				"--data", leaderDataDir,
				"--port", fmt.Sprintf("%d", leaderHTTP),
				"--raft-addr", fmt.Sprintf("127.0.0.1:%d", leaderRaft),
				"--node-id", "leader",
				"--cluster-key", keyA,
				"--nfs4-port", "0",
				"--nbd-port", "0",
				"--encryption-key-file", encKeyFile,
				"--scrub-interval", "0",
				"--lifecycle-interval", "0",
			}
			leaderLog, err := os.CreateTemp("", "leader-*.log")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.DeferCleanup(func() {
				if t.Failed() {
					if b, err := os.ReadFile(leaderLog.Name()); err == nil {
						t.Logf("leader log:\n%s", b)
					}
				}
				_ = os.Remove(leaderLog.Name())
			})

			leader := exec.CommandContext(leaderCtx, getBinary(), leaderArgs...)
			leader.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			leader.Stdout = leaderLog
			leader.Stderr = leaderLog
			gomega.Expect(leader.Start()).To(gomega.Succeed())
			ginkgo.DeferCleanup(func() {
				leaderCancel()
				_ = leader.Wait()
			})

			waitForPort(t, leaderHTTP, 15*time.Second)

			// Joiner with keyB: write .join-pending pointing to leader, then boot.
			// Must fail (SPKI mismatch on QUIC handshake; cluster join cannot complete).
			gomega.Expect(os.WriteFile(
				fmt.Sprintf("%s/%s", joinerDataDir, joinPendingFile),
				[]byte(fmt.Sprintf("127.0.0.1:%d", leaderRaft)), 0o600)).To(gomega.Succeed())

			joinerCtx, joinerCancel := context.WithTimeout(context.Background(), 3*time.Second)
			ginkgo.DeferCleanup(joinerCancel)

			joinerArgs := []string{
				"serve",
				"--data", joinerDataDir,
				"--port", fmt.Sprintf("%d", joinerHTTP),
				"--raft-addr", fmt.Sprintf("127.0.0.1:%d", joinerRaft),
				"--node-id", "joiner",
				"--cluster-key", keyB, // MISMATCH
				"--nfs4-port", "0",
				"--nbd-port", "0",
				"--encryption-key-file", encKeyFile,
				"--scrub-interval", "0",
				"--lifecycle-interval", "0",
			}
			joiner := exec.CommandContext(joinerCtx, getBinary(), joinerArgs...)
			joiner.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			out, joinErr := combinedOutputWithWaitDelay(joiner)

			gomega.Expect(joinErr).To(gomega.HaveOccurred(), "joiner with mismatched --cluster-key must not succeed. out: %s", string(out))
			gomega.Expect(errors.Is(joinerCtx.Err(), context.DeadlineExceeded)).To(gomega.BeFalse(), "joiner must fail from PSK rejection, not from test timeout. out: %s", string(out))
			gomega.Expect(string(out)).To(gomega.ContainSubstring("peer cert SPKI"), "joiner should surface the PSK/SPKI rejection. out: %s", string(out))
		})
	})
})
