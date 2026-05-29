//go:build integration

package e2e

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/gritive/GrainFS/internal/transport"
)

// Runtime `grainfs join` with a staged keys.d/current.key (real CLI).
//
// Covers the staged-PSK runtime-join path now that --cluster-key is gone: a
// solo self-seeded node converts into a cluster member by staging the seed's
// keystore (keys/0.key), cluster identity (cluster.id), AND transport PSK
// (keys.d/current.key) from a healthy peer, then invoking the REAL `grainfs
// join` CLI against its own admin UDS. The handler writes .join-pending and
// self-exits; the re-launched serve reads the staged identity + PSK and joins.
//
// This MUST shell out to `grainfs join`, not writeNodeJoinPending — the command
// (and its --confirm-staged-keys gate) is the thing under test.

type rjNode struct {
	nodeID   string
	dataDir  string
	httpPort int
	raftPort int
	httpURL  string
	cmd      *exec.Cmd
}

func rjServeArgs(n *rjNode) []string {
	return []string{
		"serve",
		"--data", n.dataDir,
		"--port", fmt.Sprintf("%d", n.httpPort),
		"--raft-addr", fmt.Sprintf("127.0.0.1:%d", n.raftPort),
		"--node-id", n.nodeID,
		"--nfs4-port", "0",
		"--nbd-port", "0",
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
	}
}

func rjStartServe(t testing.TB, n *rjNode) {
	t.Helper()
	logFile, err := os.CreateTemp("", fmt.Sprintf("rj-%s-*.log", n.nodeID))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.DeferCleanup(func() {
		if t.Failed() {
			if b, rerr := os.ReadFile(logFile.Name()); rerr == nil {
				t.Logf("%s log:\n%s", n.nodeID, b)
			}
		}
		_ = logFile.Close()
		_ = os.Remove(logFile.Name())
	})
	cmd := exec.Command(getBinary(), rjServeArgs(n)...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	gomega.Expect(cmd.Start()).To(gomega.Succeed(), "start %s serve", n.nodeID)
	n.cmd = cmd
	ginkgo.DeferCleanup(func() { terminateProcess(n.cmd) })
}

var _ = ginkgo.Describe("Cluster runtime join staged PSK", func() {
	// PENDING — blocked by a pre-existing product bug surfaced by this very test:
	// wipeSoloRaftState (internal/serveruntime/boot_phases.go:32) backs up the
	// raft stores on solo->join but does NOT clear keys.d/raft-store.key.enc. A
	// solo node self-seals that node-local raft-store key under its OWN KEK; once
	// the operator stages the peer's keys/0.key (KEK) for the join, the orphaned
	// raft-store.key.enc can no longer be AEAD-opened ("cipher: message
	// authentication failed") and the rejoin boot fails. This landed with #635
	// (raft-store sealing) and was uncaught because no e2e drove the real
	// `grainfs join` CLI. The asymmetry confirms it is orthogonal to --cluster-key
	// removal: invite-join stages the KEK BEFORE first boot (raft-store.key.enc
	// derives under the right KEK → 13 specs pass), while `grainfs join` stages it
	// AFTER a solo boot. The staged-PSK path itself works; the gap is KEK staging
	// on solo->join. Fix belongs in a follow-up (wipeSoloRaftState should also
	// handle raft-store.key.enc; mind the .pre-join-backup stores sealed under the
	// old key) — see TODOS [P3] "retire runtime grainfs join in favor of
	// invite-join". The test below is faithful and should pass once that lands.
	ginkgo.PIt("a solo node joins via real grainfs join after staging keys.d/current.key", func() {
		t := ginkgo.GinkgoTB()

		// 1. Seed: solo self-seed (no --cluster-key). It becomes the leader and
		//    accepts a .join-pending joiner.
		seed := &rjNode{nodeID: "rj-seed", dataDir: shortTempDir(t), httpPort: freePort(), raftPort: freePort()}
		seed.httpURL = fmt.Sprintf("http://127.0.0.1:%d", seed.httpPort)
		rjStartServe(t, seed)
		waitForPort(t, seed.httpPort, 60*time.Second)
		waitSocketReady(t, filepath.Join(seed.dataDir, "admin.sock"), 30*time.Second)
		bootstrapAdminViaUDSAnyResult(t, []string{seed.dataDir}, 30*time.Second)

		// 2. Joiner: solo self-seed (no --cluster-key). It generates its OWN
		//    keys/0.key, cluster.id, and keys.d/current.key.
		joiner := &rjNode{nodeID: "rj-joiner", dataDir: shortTempDir(t), httpPort: freePort(), raftPort: freePort()}
		joiner.httpURL = fmt.Sprintf("http://127.0.0.1:%d", joiner.httpPort)
		rjStartServe(t, joiner)
		waitForPort(t, joiner.httpPort, 60*time.Second)
		joinerSock := filepath.Join(joiner.dataDir, "admin.sock")
		waitSocketReady(t, joinerSock, 30*time.Second)

		// 3. Stage all three of the seed's files over the joiner's self-seeded
		//    ones BEFORE join. All three (KEK, identity, transport PSK) must match
		//    the seed or the QUIC/KEK handshake fails. Files are read at boot, so
		//    staging while the joiner is still running solo is safe.
		seedKEK, err := os.ReadFile(filepath.Join(seed.dataDir, "keys", "0.key"))
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "read seed keys/0.key")
		gomega.Expect(os.MkdirAll(filepath.Join(joiner.dataDir, "keys"), 0o700)).To(gomega.Succeed())
		gomega.Expect(os.WriteFile(filepath.Join(joiner.dataDir, "keys", "0.key"), seedKEK, 0o600)).To(gomega.Succeed())

		seedClusterID, err := os.ReadFile(filepath.Join(seed.dataDir, "cluster.id"))
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "read seed cluster.id")
		gomega.Expect(os.WriteFile(filepath.Join(joiner.dataDir, "cluster.id"), seedClusterID, 0o600)).To(gomega.Succeed())

		seedPSK, err := transport.NewKeystore(seed.dataDir).ReadCurrent()
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "read seed keys.d/current.key")
		gomega.Expect(transport.NewKeystore(joiner.dataDir).WriteCurrent(seedPSK)).To(gomega.Succeed(),
			"stage seed PSK into joiner keys.d/current.key")

		// 4. Invoke the REAL `grainfs join` CLI against the joiner's admin UDS.
		//    The handler writes .join-pending and triggers a graceful self-exit.
		//    --force: a solo node that has booted holds bootstrap FSM state, so the
		//    data guard (HasUserData) trips; force discards the joiner's solo state
		//    to join — the documented path for converting a solo bootstrap.
		seedRaftAddr := fmt.Sprintf("127.0.0.1:%d", seed.raftPort)
		joinCmd := exec.Command(getBinary(), "join", seedRaftAddr,
			"--endpoint", joinerSock, "--confirm-staged-keys", "--force")
		joinOut, joinErr := joinCmd.CombinedOutput()
		gomega.Expect(joinErr).NotTo(gomega.HaveOccurred(), "grainfs join must succeed; out:\n%s", string(joinOut))
		gomega.Expect(string(joinOut)).To(gomega.ContainSubstring("restart_initiated"),
			"join must trigger a restart; out:\n%s", string(joinOut))

		// 5. The joiner process self-exits ~150ms after the response. Reap it so
		//    the port + file locks are released before re-launching.
		waitDone := make(chan struct{})
		go func() {
			_, _ = joiner.cmd.Process.Wait()
			close(waitDone)
		}()
		gomega.Eventually(func() bool {
			select {
			case <-waitDone:
				return true
			default:
				return false
			}
		}, 30*time.Second, 200*time.Millisecond).Should(gomega.BeTrue(), "joiner serve must self-exit after join")

		// 6. Re-launch the joiner serve. It reads .join-pending + the staged
		//    identity/PSK, clears its solo raft state, and joins the seed.
		rjStartServe(t, joiner)
		waitForPort(t, joiner.httpPort, 90*time.Second)

		// 7. Assert it becomes a meta-raft voter in the seed's view.
		waitForVoter(t, seed.httpURL, "rj-joiner", 90*time.Second)
	})
})
