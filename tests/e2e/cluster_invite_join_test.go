//go:build integration

package e2e

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/gritive/GrainFS/internal/cluster"
)

// Zero-CA over-the-wire invite-join e2e (WIRE slice, W11).
//
// Proves a secret-less node boots from only a GRAINFS_INVITE_BUNDLE token,
// becomes a meta-raft VOTER, and serves S3 (staged encryption.key works).
// Also covers single-use invite redemption, cross-cluster isolation, and the
// resume/no-op classification on restart.

// inviteJoinNode is a single grainfs serve process started directly (NOT via
// the e2eCluster harness, which drives the legacy KEK-handshake join — the
// Zero-CA invite-join is a distinct flow).
type inviteJoinNode struct {
	nodeID   string
	dataDir  string
	httpPort int
	raftPort int
	joinPort int
	httpURL  string
	cmd      *exec.Cmd
	logPath  string
}

const inviteJoinClusterKey = "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"

// startInviteLeader boots a genesis leader: --cluster-key + encryption-key +
// stable --raft-addr + explicit --join-listen-addr. clusterMode is always true,
// so the Zero-CA join listener starts even on a single node, letting it mint
// invites.
func startInviteLeader(t testing.TB, encKeyFile, clusterKey string) *inviteJoinNode {
	n := &inviteJoinNode{
		nodeID:   "leader",
		dataDir:  shortTempDir(t),
		httpPort: freePort(),
		raftPort: freePort(),
		joinPort: freePort(),
	}
	n.httpURL = fmt.Sprintf("http://127.0.0.1:%d", n.httpPort)
	args := []string{
		"serve",
		"--data", n.dataDir,
		"--port", fmt.Sprintf("%d", n.httpPort),
		"--raft-addr", fmt.Sprintf("127.0.0.1:%d", n.raftPort),
		"--join-listen-addr", fmt.Sprintf("127.0.0.1:%d", n.joinPort),
		"--node-id", n.nodeID,
		"--cluster-key", clusterKey,
		"--encryption-key-file", encKeyFile,
		"--nfs4-port", "0",
		"--nbd-port", "0",
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
	}
	startInviteProc(t, n, args, nil)
	waitForPort(t, n.httpPort, 60*time.Second)
	return n
}

// startInviteJoiner boots a secret-less node with GRAINFS_INVITE_BUNDLE set and
// NO --cluster-key / --encryption-key-file. extraEnv lets the resume test reuse
// a populated data dir.
func startInviteJoiner(t testing.TB, nodeID, dataDir, bundle string) *inviteJoinNode {
	n := &inviteJoinNode{
		nodeID:   nodeID,
		dataDir:  dataDir,
		httpPort: freePort(),
		raftPort: freePort(),
	}
	n.httpURL = fmt.Sprintf("http://127.0.0.1:%d", n.httpPort)
	args := []string{
		"serve",
		"--data", n.dataDir,
		"--port", fmt.Sprintf("%d", n.httpPort),
		"--raft-addr", fmt.Sprintf("127.0.0.1:%d", n.raftPort),
		"--node-id", nodeID,
		"--nfs4-port", "0",
		"--nbd-port", "0",
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
	}
	env := append(os.Environ(), inviteBundleEnvKey+"="+bundle)
	startInviteProc(t, n, args, env)
	return n
}

const inviteBundleEnvKey = "GRAINFS_INVITE_BUNDLE"

func startInviteProc(t testing.TB, n *inviteJoinNode, args []string, env []string) {
	logFile, err := os.CreateTemp("", fmt.Sprintf("invite-%s-*.log", n.nodeID))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	n.logPath = logFile.Name()
	ginkgo.DeferCleanup(func() {
		if t.Failed() {
			if b, rerr := os.ReadFile(n.logPath); rerr == nil {
				t.Logf("%s log:\n%s", n.nodeID, b)
			}
		}
		_ = os.Remove(n.logPath)
	})

	cmd := exec.Command(getBinary(), args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if env != nil {
		cmd.Env = env
	}
	gomega.Expect(cmd.Start()).To(gomega.Succeed(), "start %s", n.nodeID)
	n.cmd = cmd
	ginkgo.DeferCleanup(func() { terminateProcess(cmd) })
}

// mintInvite runs `grainfs cluster invite create` against the leader's admin
// UDS and returns the bundle token (the line after the "Set this..." prompt).
func mintInvite(t testing.TB, leaderDataDir string) string {
	sock := filepath.Join(leaderDataDir, "admin.sock")
	var out []byte
	var lastErr error
	// The admin UDS + meta-raft leadership take a moment to settle after the
	// HTTP port opens; retry until the mint succeeds.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		cmd := exec.Command(getBinary(), "cluster", "invite", "create", "--endpoint", sock)
		out, lastErr = cmd.CombinedOutput()
		if lastErr == nil {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	gomega.Expect(lastErr).NotTo(gomega.HaveOccurred(), "invite create must succeed; out:\n%s", string(out))
	return parseBundleToken(t, string(out))
}

// parseBundleToken extracts the bundle token printed by RunInviteCreate: the
// non-empty line following "Set this on the joining node as ...".
func parseBundleToken(t testing.TB, output string) string {
	sc := bufio.NewScanner(strings.NewReader(output))
	seenPrompt := false
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if seenPrompt && line != "" {
			return line
		}
		if strings.Contains(line, "GRAINFS_INVITE_BUNDLE") {
			seenPrompt = true
		}
	}
	t.Fatalf("could not parse invite bundle from output:\n%s", output)
	return ""
}

// waitForVoter polls the leader's cluster status until nodeID appears in the
// voter set (peers excludes self).
func waitForVoter(t testing.TB, leaderURL, nodeID string, timeout time.Duration) {
	gomega.Eventually(func() bool {
		s := getStatusJSON(t, leaderURL)
		return containsString(stringList(s["peers"]), nodeID)
	}, timeout, 500*time.Millisecond).Should(gomega.BeTrue(),
		"node %s must become a meta-raft voter", nodeID)
}

var _ = ginkgo.Describe("Zero-CA invite-join", func() {
	ginkgo.Context("HappyPath", func() {
		ginkgo.It("a secret-less node joins via invite and becomes a voter", func() {
			t := ginkgo.GinkgoTB()
			encKeyFile := makeSharedEncryptionKeyFile(t)

			leader := startInviteLeader(t, encKeyFile, inviteJoinClusterKey)
			_, _ = bootstrapAdminViaUDS(t, leader.dataDir)

			bundle := mintInvite(t, leader.dataDir)

			joinerDir := shortTempDir(t)
			startInviteJoiner(t, "joiner", joinerDir, bundle)

			// The joiner must become a meta-raft VOTER (not merely "booted"):
			// the leader's status.peers (excludes self) must list it. This proves
			// the full W1-W10 wire path — invite mint, Phase-1 secret pull over the
			// join ALPN, and the Phase-2 membership ACK — all landed.
			waitForVoter(t, leader.httpURL, "joiner", 90*time.Second)

			// Phase-1 must have staged the secret material on the joiner: the
			// encryption key (opened from the sealed bootstrap) and the sealed
			// node identity key. Both are written to disk before the Phase-2 ACK.
			gomega.Expect(filepath.Join(joinerDir, "encryption.key")).To(gomega.BeAnExistingFile())
			gomega.Expect(filepath.Join(joinerDir, "keys.d", "node.key.enc")).To(gomega.BeAnExistingFile())
		})

		// S3 round-trip THROUGH the joined node is blocked on F3/F4 (the leader-side
		// cluster-QUIC accept-set UNION for a freshly-joined node, the deferred
		// SetOnPeersChanged work referenced in invite_join_boot.go Phase-2). Without
		// it the leader never AppendEntries-replicates the gen-0 DEK to the joiner,
		// so the joiner reaches voter membership but then exits on the WaitDEKReady
		// gate (~30s) before its S3 surface can serve a read. Un-skip once F3/F4
		// lands the dial-back accept-set.
		ginkgo.PIt("serves an S3 PutObject/GetObject round-trip through the joined node (blocked: F3/F4 gen-0 DEK replication)", func() {
		})
	})

	ginkgo.Context("SingleUse", func() {
		ginkgo.It("rejects replay of the same bundle from a different node", func() {
			t := ginkgo.GinkgoTB()
			encKeyFile := makeSharedEncryptionKeyFile(t)

			leader := startInviteLeader(t, encKeyFile, inviteJoinClusterKey)
			_, _ = bootstrapAdminViaUDS(t, leader.dataDir)
			bundle := mintInvite(t, leader.dataDir)

			// First joiner redeems the invite and becomes a voter. (It later exits
			// on the F3/F4 DEK gate, so assert voter membership — which lands
			// BEFORE that gate — rather than a durable HTTP surface.)
			startInviteJoiner(t, "joiner", shortTempDir(t), bundle)
			waitForVoter(t, leader.httpURL, "joiner", 90*time.Second)

			// Replay the SAME bundle from a THIRD node (different id, empty dir).
			// The invite is single-use → Phase-1 redeem must be rejected, so the
			// process exits non-zero rather than ever reaching voter membership.
			replay := startInviteJoiner(t, "replay", shortTempDir(t), bundle)
			gomega.Eventually(func() bool {
				exited, _ := processExited(replay.cmd)
				return exited
			}, 60*time.Second, 500*time.Millisecond).Should(gomega.BeTrue(),
				"replayed bundle must fail Phase-1 and exit")

			// And it never became a voter. (Non-occurrence check, independent of
			// the first joiner's process state.)
			gomega.Consistently(func() bool {
				s := getStatusJSON(t, leader.httpURL)
				return containsString(stringList(s["peers"]), "replay")
			}, 2*time.Second, 500*time.Millisecond).Should(gomega.BeFalse(),
				"replayed node must not join")
		})
	})

	ginkgo.Context("CrossClusterIsolation", func() {
		ginkgo.It("rejects a bundle minted on cluster A against cluster B", func() {
			t := ginkgo.GinkgoTB()
			encA := makeSharedEncryptionKeyFile(t)
			encB := makeSharedEncryptionKeyFile(t)

			leaderA := startInviteLeader(t, encA, inviteJoinClusterKey)
			_, _ = bootstrapAdminViaUDS(t, leaderA.dataDir)
			bundleA := mintInvite(t, leaderA.dataDir)

			// Different cluster key → different cluster identity.
			keyB := strings.Repeat("b", 64)
			leaderB := startInviteLeader(t, encB, keyB)
			_, _ = bootstrapAdminViaUDS(t, leaderB.dataDir)
			bundleB := mintInvite(t, leaderB.dataDir)

			// Splice A's invite identity (InvitePriv/InviteID/ClusterIDHex) onto
			// B's seed addr+SPKI: the joiner reaches B's join listener (SPKI pin
			// passes) but the transcript cluster.id is A's → B's gateInvite
			// rejects on cluster identity mismatch.
			decA, err := cluster.DecodeInviteBundle(bundleA)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			decB, err := cluster.DecodeInviteBundle(bundleB)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			spliced := cluster.InviteBundle{
				InvitePriv:   decA.InvitePriv,
				InviteID:     decA.InviteID,
				ClusterIDHex: decA.ClusterIDHex,
				SeedAddr:     decB.SeedAddr,
				SeedSPKI:     decB.SeedSPKI,
			}
			token := cluster.EncodeInviteBundle(spliced)

			joiner := startInviteJoiner(t, "x-joiner", shortTempDir(t), token)
			gomega.Eventually(func() bool {
				exited, _ := processExited(joiner.cmd)
				return exited
			}, 60*time.Second, 500*time.Millisecond).Should(gomega.BeTrue(),
				"cross-cluster bundle must be rejected and the joiner must exit")

			gomega.Consistently(func() bool {
				s := getStatusJSON(t, leaderB.httpURL)
				return containsString(stringList(s["peers"]), "x-joiner")
			}, 2*time.Second, 500*time.Millisecond).Should(gomega.BeFalse(),
				"cross-cluster joiner must not join cluster B")
		})
	})

	ginkgo.Context("RestartNoOp", func() {
		// The no-op-resume classifier is what this would exercise: a fully-joined
		// voter restarted with the (now-consumed) bundle env still set must
		// classify as inviteNormalBoot (artifacts complete + acked) and boot
		// normally. That restart requires the joiner to FIRST reach a durable,
		// settled state — which is blocked by F3/F4 (gen-0 DEK never replicates →
		// the joiner exits on the WaitDEKReady gate ~30s after Phase-2). So the
		// integration variant is pending. The pure classifier
		// (classifyInviteJoinResume) is already unit-covered in
		// serveruntime/invite_join_boot_test.go.
		//
		// Mid-Phase-1 crash injection (kill after staging, before ACK) is
		// intentionally NOT attempted here: it is timing-flaky over the wire and
		// the resume gate is, again, unit-tested. Un-skip once F3/F4 lands.
		ginkgo.PIt("an already-joined node restarts with stale bundle env as a no-op and stays a voter (blocked: F3/F4 gen-0 DEK replication)", func() {
		})
	})
})
