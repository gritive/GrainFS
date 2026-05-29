//go:build integration

package e2e

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"io"
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
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/transport"
)

// Zero-CA over-the-wire invite-join e2e (WIRE slice, W11).
//
// Proves a secret-less node boots from only a GRAINFS_INVITE_BUNDLE token,
// becomes a meta-raft VOTER, and serves S3 using staged KEK material.
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

// startInviteLeader boots a genesis leader: --cluster-key +
// stable --raft-addr + explicit --join-listen-addr. clusterMode is always true,
// so the Zero-CA join listener starts even on a single node, letting it mint
// invites.
func startInviteLeader(t testing.TB, clusterKey string) *inviteJoinNode {
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
// NO --cluster-key. extraEnv lets the resume test reuse
// a populated data dir.
func startInviteJoiner(t testing.TB, nodeID, dataDir, bundle string) *inviteJoinNode {
	n := &inviteJoinNode{
		nodeID:   nodeID,
		dataDir:  dataDir,
		httpPort: freePort(),
		raftPort: freePort(),
	}
	n.httpURL = fmt.Sprintf("http://127.0.0.1:%d", n.httpPort)
	env := append(os.Environ(), inviteBundleEnvKey+"="+bundle)
	startInviteProc(t, n, n.joinerArgs(), env)
	return n
}

// restartInviteJoiner terminates the running joiner process and boots a fresh
// one on the SAME data dir, ports, and (now-consumed) bundle env. Used by the
// resume/no-op test: an already-joined node restarted with the stale bundle env
// must classify as a normal boot and stay a voter.
func restartInviteJoiner(t testing.TB, n *inviteJoinNode, bundle string) {
	terminateProcess(n.cmd)
	env := append(os.Environ(), inviteBundleEnvKey+"="+bundle)
	startInviteProc(t, n, n.joinerArgs(), env)
}

func (n *inviteJoinNode) joinerArgs() []string {
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

func runCompleteCutover(t testing.TB, leaderDataDir string) {
	t.Helper()
	sock := filepath.Join(leaderDataDir, "admin.sock")
	cmd := exec.Command(getBinary(), "cluster", "--endpoint", sock, "complete-cutover")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("complete-cutover must succeed in one call; err=%v out:\n%s", err, string(out))
	}
	if !strings.Contains(string(out), "Zero-CA cutover complete") {
		t.Fatalf("complete-cutover output missing success line:\n%s", string(out))
	}
}

func runRevokeNode(t testing.TB, leaderDataDir, nodeID string) {
	t.Helper()
	sock := filepath.Join(leaderDataDir, "admin.sock")
	cmd := exec.Command(getBinary(), "cluster", "--endpoint", sock, "revoke-node", nodeID)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("revoke-node %s must succeed; err=%v out:\n%s", nodeID, err, string(out))
	}
	if !strings.Contains(string(out), "Zero-CA node revoked: "+nodeID) {
		t.Fatalf("revoke-node output missing success line:\n%s", string(out))
	}
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

// --- channel-binding negative-test helpers --------------------------------
//
// These drive the seed join listener DIRECTLY in-process (the e2e suite already
// imports internal/cluster/transport/encrypt), mirroring the joiner boot path in
// internal/serveruntime/invite_join_boot.go: GenerateNodeIdentity → DialJoin
// (capturing the live RFC 5705 exporter), sign the InviteTranscript over a chosen
// bind, frame the Phase-1 JoinRequest, send it, and read the framed JoinReply.
// They let us forge the bind a Phase-1 transcript is signed over without going
// through the subprocess joiner, which always signs over its own live exporter.

// phase1Material is a freshly minted invite + a fresh joiner identity, ready to
// build a Phase-1 JoinRequest against the leader.
type phase1Material struct {
	bundle  cluster.InviteBundle
	tlsCert tls.Certificate
	priv    *ecdsa.PrivateKey
	spki    [32]byte
	nodeID  string
}

// buildSignedPhase1 builds + signs a Phase-1 JoinRequest over the supplied bind,
// mirroring inviteJoinPhase1.buildPhase1. The signatures verify ONLY if bind
// equals the live join-listener session exporter the leader reconstructs over.
func buildSignedPhase1(m phase1Material, clusterID []byte, bind []byte) cluster.JoinRequest {
	nonce := bytes.Repeat([]byte{0x5a}, 16)
	tr := encrypt.InviteTranscript{
		ClusterID: clusterID,
		Nonce:     nonce,
		NodeID:    m.nodeID,
		Address:   "127.0.0.1:1",
		SPKI:      m.spki[:],
		Bind:      bind,
	}
	inviteSig := encrypt.SignInviteTranscript(m.bundle.InvitePriv, tr)
	nodeSig, err := encrypt.SignNodeTranscript(m.priv, tr)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return cluster.JoinRequest{
		JoinPhase:      1,
		NodeID:         m.nodeID,
		Address:        "127.0.0.1:1",
		SPKI:           m.spki[:],
		CertDER:        m.tlsCert.Certificate[0],
		NodeSig:        nodeSig,
		InviteSig:      inviteSig,
		InviteID:       m.bundle.InviteID,
		HandshakeNonce: nonce,
	}
}

// newPhase1Material mints a fresh single-use invite on the leader and a fresh
// joiner ECDSA identity, decodes the bundle, and returns both. Each negative
// test mints its OWN invite so it never consumes the positive test's invite.
func newPhase1Material(t testing.TB, leader *inviteJoinNode, nodeID string) (phase1Material, []byte) {
	token := mintInvite(t, leader.dataDir)
	bundle, err := cluster.DecodeInviteBundle(token)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	clusterID, err := hex.DecodeString(bundle.ClusterIDHex)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	cert, spki, err := transport.GenerateNodeIdentity(bundle.ClusterIDHex, nodeID)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	priv, ok := cert.PrivateKey.(*ecdsa.PrivateKey)
	gomega.Expect(ok).To(gomega.BeTrue(), "node identity key must be ECDSA")
	return phase1Material{
		bundle:  bundle,
		tlsCert: cert,
		priv:    priv,
		spki:    spki,
		nodeID:  nodeID,
	}, clusterID
}

// sendPhase1 frames + writes req onto stream and reads/decodes the framed reply.
// A rejected Phase-1 is written as a framed JoinReply{Accepted:false} by the
// leader's HandleJoinStream, so the reply is always decodable. A torn-down stream
// surfaces as a non-nil error, which callers also treat as a reject.
func sendPhase1(stream interface {
	io.Reader
	Write([]byte) (int, error)
	Close() error
}, req cluster.JoinRequest) (cluster.JoinReply, error) {
	blob, err := cluster.EncodeJoinRequest(req)
	if err != nil {
		return cluster.JoinReply{}, err
	}
	if _, err := stream.Write(transport.JoinPutField(nil, blob)); err != nil {
		return cluster.JoinReply{}, err
	}
	_ = stream.Close()
	fields, err := transport.JoinReadFields(stream, 1)
	if err != nil {
		return cluster.JoinReply{}, err
	}
	reply, err := cluster.DecodeJoinReply(fields[0])
	if err != nil {
		return cluster.JoinReply{}, err
	}
	return *reply, nil
}

// relayPhase1AcrossSessions opens session A (capturing bindA), signs a Phase-1
// transcript over bindA, then sends the session-A-signed request onto a SECOND
// session B. The leader reconstructs the transcript with B's server-side exporter
// (≠ bindA), so BOTH signatures fail → reject. This is the test that actually
// proves channel binding rather than mere signature verification.
func relayPhase1AcrossSessions(ctx context.Context, t testing.TB, leader *inviteJoinNode) (cluster.JoinReply, []byte, []byte, error) {
	m, clusterID := newPhase1Material(t, leader, "relay-joiner")

	streamA, bindA, closeA, err := transport.DialJoin(ctx, m.bundle.SeedAddr, m.bundle.SeedSPKI, m.tlsCert)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "session A dial")
	defer func() { _ = closeA() }()
	_ = streamA // session A is opened only to capture its live exporter (bindA).

	reqA := buildSignedPhase1(m, clusterID, bindA)

	streamB, bindB, closeB, err := transport.DialJoin(ctx, m.bundle.SeedAddr, m.bundle.SeedSPKI, m.tlsCert)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "session B dial")
	defer func() { _ = closeB() }()

	reply, sendErr := sendPhase1(streamB, reqA)
	return reply, bindA, bindB, sendErr
}

// dialPhase1WithForgedBind signs a Phase-1 transcript over a forged bind that is
// NOT any live session exporter, dials once, and sends it on the SAME session.
// The leader reconstructs over the real session exporter → signatures fail.
func dialPhase1WithForgedBind(ctx context.Context, t testing.TB, leader *inviteJoinNode, forged []byte) (cluster.JoinReply, error) {
	m, clusterID := newPhase1Material(t, leader, "forged-bind-joiner")

	stream, _, closeConn, err := transport.DialJoin(ctx, m.bundle.SeedAddr, m.bundle.SeedSPKI, m.tlsCert)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "forged-bind dial")
	defer func() { _ = closeConn() }()

	req := buildSignedPhase1(m, clusterID, forged)
	return sendPhase1(stream, req)
}

// isBindingReject asserts the Phase-1 request was DELIVERED and a framed reply
// DECODED, and that the reply is a channel-binding/signature rejection — NOT a
// transport teardown and NOT an unrelated reject (already_member/addr_mismatch/
// cluster_full/not_leader). The leader's HandleJoinStream always writes a framed
// JoinReply{Accepted:false} on reject (no teardown), so a delivered relay/forged
// request MUST return a decodable "signature invalid" reply; treating a transport
// error as a pass would be vacuous (it could mask an early conn close, a failed
// exporter derivation, or broken handler wiring rather than the bind being what
// rejected the request).
func isBindingReject(reply cluster.JoinReply, sendErr error) {
	gomega.Expect(sendErr).NotTo(gomega.HaveOccurred(),
		"the relayed/forged Phase-1 request must reach the leader and a framed reply must come back, not a transport teardown")
	gomega.Expect(reply.Accepted).To(gomega.BeFalse())
	gomega.Expect(reply.Status).To(gomega.Equal(cluster.JoinStatusError),
		"reject must be a generic invite-gate error (got %q), not an unrelated status", reply.Status)
	gomega.Expect(reply.Message).To(gomega.ContainSubstring("signature invalid"),
		"reject must be due to signature/binding mismatch, not an unrelated cause (got %q)", reply.Message)
}

var _ = ginkgo.Describe("Zero-CA invite-join", func() {
	ginkgo.Context("HappyPath", func() {
		ginkgo.It("a secret-less node joins via invite and becomes a voter", func() {
			t := ginkgo.GinkgoTB()
			leader := startInviteLeader(t, inviteJoinClusterKey)
			_, _ = bootstrapAdminViaUDS(t, leader.dataDir)

			bundle := mintInvite(t, leader.dataDir)

			joinerDir := shortTempDir(t)
			startInviteJoiner(t, "joiner", joinerDir, bundle)

			// The joiner must become a meta-raft VOTER (not merely "booted"):
			// the leader's status.peers (excludes self) must list it. This proves
			// the full W1-W10 wire path — invite mint, Phase-1 secret pull over the
			// join ALPN, and the Phase-2 membership ACK — all landed.
			waitForVoter(t, leader.httpURL, "joiner", 90*time.Second)

			// Phase-1 must stage KEK-sealed identity material without recreating
			// the legacy static encryption key on the joiner.
			gomega.Expect(filepath.Join(joinerDir, "encryption.key")).NotTo(gomega.BeAnExistingFile())
			gomega.Expect(filepath.Join(joinerDir, "keys.d", "node.key.enc")).To(gomega.BeAnExistingFile())
		})

		// Full S3 round-trip THROUGH the invite-joined node. Both write and read
		// are routed to the joiner's own S3 endpoint. PutObject through the joiner
		// works (forwarded to the group leader), and GetObject now works the same
		// way: the invite-joiner no longer installs the group-0 read-index fence
		// (it is not the group-0 leader), so reads skip ReadIndex/ErrNotLeader and
		// reach the ClusterCoordinator forward path that routes them to the real
		// group leader — exactly the path PUTs already use. The joiner does not
		// need its own data-group membership; it forwards reads to the group leader.
		ginkgo.It("serves an S3 PutObject/GetObject round-trip through the joined node", func() {
			t := ginkgo.GinkgoTB()
			leader := startInviteLeader(t, inviteJoinClusterKey)
			admin, err := bootstrapAdminResultViaUDSForTestMain(leader.dataDir, 30*time.Second)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "bootstrap admin SA")
			ak, sk, saID := admin.AccessKey, admin.SecretKey, admin.SAID

			bundle := mintInvite(t, leader.dataDir)

			joinerDir := shortTempDir(t)
			joiner := startInviteJoiner(t, "joiner", joinerDir, bundle)
			waitForVoter(t, leader.httpURL, "joiner", 90*time.Second)
			waitForPort(t, joiner.httpPort, 60*time.Second)

			bucket := "invite-join-roundtrip"
			gomega.Expect(adminCreateBucketWithPolicyAttachAny(
				[]string{leader.dataDir}, saID, bucket, 60*time.Second)).To(gomega.Succeed())

			joinerCli := s3ClientFor(joiner.httpURL, ak, sk)
			gomega.Expect(waitForIAMReady(joinerCli, 60*time.Second)).To(gomega.Succeed())

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			ginkgo.DeferCleanup(cancel)

			body := []byte("invite-join round-trip payload")
			key := "roundtrip.txt"

			// PUT through the joiner (forwarded to the group leader, committed).
			gomega.Eventually(func() error {
				return tryPutObject(ctx, joinerCli, bucket, key, body)
			}, 30*time.Second, 500*time.Millisecond).Should(gomega.Succeed(),
				"PutObject through the invite-joined node must succeed")

			// GET through the joiner: the router sync (invite-join Phase-2 parity
			// fix) completes asynchronously during boot, so poll until the read
			// resolves and returns the bytes that were PUT.
			gomega.Eventually(func() ([]byte, error) {
				return getObjectBytes(ctx, joinerCli, bucket, key)
			}, 30*time.Second, 500*time.Millisecond).Should(gomega.Equal(body),
				"GetObject through the invite-joined node must return the PUT bytes")
		})

		ginkgo.It("runs complete-cutover and accepts a post-drop invite-join without a shared PSK", func() {
			t := ginkgo.GinkgoTB()
			leader := startInviteLeader(t, inviteJoinClusterKey)
			admin, err := bootstrapAdminResultViaUDSForTestMain(leader.dataDir, 30*time.Second)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "bootstrap admin SA")
			ak, sk, saID := admin.AccessKey, admin.SecretKey, admin.SAID

			preDropBundle := mintInvite(t, leader.dataDir)
			preDropDir := shortTempDir(t)
			preDropJoiner := startInviteJoiner(t, "pre-drop-joiner", preDropDir, preDropBundle)
			waitForVoter(t, leader.httpURL, "pre-drop-joiner", 90*time.Second)
			waitForPort(t, preDropJoiner.httpPort, 60*time.Second)

			runCompleteCutover(t, leader.dataDir)

			postDropBundle := mintInvite(t, leader.dataDir)
			postDropDir := shortTempDir(t)
			postDropJoiner := startInviteJoiner(t, "post-drop-joiner", postDropDir, postDropBundle)
			waitForVoter(t, leader.httpURL, "post-drop-joiner", 90*time.Second)
			waitForPort(t, postDropJoiner.httpPort, 60*time.Second)

			currentKey, err := os.ReadFile(filepath.Join(postDropDir, "keys.d", "current.key"))
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "post-drop joiner must stage a local transport placeholder")
			gomega.Expect(strings.TrimSpace(string(currentKey))).NotTo(gomega.Equal(inviteJoinClusterKey),
				"post-drop invite-join must not stage the revoked shared transport PSK")
			gomega.Expect(filepath.Join(postDropDir, "keys.d", "node.key.enc")).To(gomega.BeAnExistingFile())

			bucket := "post-drop-invite-join"
			gomega.Expect(adminCreateBucketWithPolicyAttachAny(
				[]string{leader.dataDir}, saID, bucket, 60*time.Second)).To(gomega.Succeed())

			postDropCli := s3ClientFor(postDropJoiner.httpURL, ak, sk)
			gomega.Expect(waitForIAMReady(postDropCli, 60*time.Second)).To(gomega.Succeed())

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			ginkgo.DeferCleanup(cancel)

			body := []byte("post-drop invite-join payload")
			key := "post-drop.txt"
			gomega.Eventually(func() error {
				return tryPutObject(ctx, postDropCli, bucket, key, body)
			}, 30*time.Second, 500*time.Millisecond).Should(gomega.Succeed(),
				"PutObject through the post-drop invite-joined node must succeed")

			gomega.Eventually(func() ([]byte, error) {
				return getObjectBytes(ctx, postDropCli, bucket, key)
			}, 30*time.Second, 500*time.Millisecond).Should(gomega.Equal(body),
				"GetObject through the post-drop invite-joined node must return the PUT bytes")
		})

		ginkgo.It("revokes a post-drop joined node and rejects rejoin with the same node-id", func() {
			t := ginkgo.GinkgoTB()
			leader := startInviteLeader(t, inviteJoinClusterKey)
			_, _ = bootstrapAdminViaUDS(t, leader.dataDir)

			preDropBundle := mintInvite(t, leader.dataDir)
			preDropJoiner := startInviteJoiner(t, "pre-drop-revoker", shortTempDir(t), preDropBundle)
			waitForVoter(t, leader.httpURL, "pre-drop-revoker", 90*time.Second)
			waitForPort(t, preDropJoiner.httpPort, 60*time.Second)

			runCompleteCutover(t, leader.dataDir)

			bundle := mintInvite(t, leader.dataDir)
			joinerDir := shortTempDir(t)
			joiner := startInviteJoiner(t, "revoked-joiner", joinerDir, bundle)
			waitForVoter(t, leader.httpURL, "revoked-joiner", 90*time.Second)
			waitForPort(t, joiner.httpPort, 60*time.Second)

			runRevokeNode(t, leader.dataDir, "revoked-joiner")
			terminateProcess(joiner.cmd)

			replayBundle := mintInvite(t, leader.dataDir)
			replay := startInviteJoiner(t, "revoked-joiner", shortTempDir(t), replayBundle)
			gomega.Eventually(func() bool {
				exited, _ := processExited(replay.cmd)
				return exited
			}, 60*time.Second, 500*time.Millisecond).Should(gomega.BeTrue(),
				"revoked joiner must fail when trying to rejoin with the same node-id")

			gomega.Consistently(func() bool {
				s := getStatusJSON(t, leader.httpURL)
				return containsString(stringList(s["peers"]), "revoked-joiner")
			}, 2*time.Second, 500*time.Millisecond).Should(gomega.BeFalse(),
				"revoked node must not rejoin")
		})
	})

	ginkgo.Context("SingleUse", func() {
		ginkgo.It("rejects replay of the same bundle from a different node", func() {
			t := ginkgo.GinkgoTB()
			leader := startInviteLeader(t, inviteJoinClusterKey)
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
			leaderA := startInviteLeader(t, inviteJoinClusterKey)
			_, _ = bootstrapAdminViaUDS(t, leaderA.dataDir)
			bundleA := mintInvite(t, leaderA.dataDir)

			// Different cluster key → different cluster identity.
			keyB := strings.Repeat("b", 64)
			leaderB := startInviteLeader(t, keyB)
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

	ginkgo.Context("ChannelBinding", func() {
		// POSITIVE regression: the existing HappyPath "a secret-less node joins via
		// invite and becomes a voter" already drives a matching channel binding end
		// to end (both ends derive the same RFC 5705 exporter). It is the key
		// regression check for mandatory binding — kept there, not duplicated here.

		// NEGATIVE primary: a TRUE cross-session relay. Session A's signed Phase-1
		// is replayed onto session B; the leader reconstructs the transcript with
		// B's exporter, so both signatures fail. This proves channel binding rather
		// than mere signature verification.
		ginkgo.It("rejects a Phase-1 request relayed onto a different TLS session", func() {
			t := ginkgo.GinkgoTB()
			leader := startInviteLeader(t, inviteJoinClusterKey)
			_, _ = bootstrapAdminViaUDS(t, leader.dataDir)

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			ginkgo.DeferCleanup(cancel)

			reply, bindA, bindB, sendErr := relayPhase1AcrossSessions(ctx, t, leader)
			// The two sessions must produce different exporters, else the relay
			// would be a no-op and the test would prove nothing.
			gomega.Expect(bytes.Equal(bindA, bindB)).To(gomega.BeFalse(),
				"session A and B must derive different channel bindings")
			isBindingReject(reply, sendErr)
		})

		// NEGATIVE secondary (cheap): a Phase-1 transcript signed over a 32-byte
		// constant that is not any live session exporter.
		ginkgo.It("rejects an invite transcript signed for a non-session bind", func() {
			t := ginkgo.GinkgoTB()
			leader := startInviteLeader(t, inviteJoinClusterKey)
			_, _ = bootstrapAdminViaUDS(t, leader.dataDir)

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			ginkgo.DeferCleanup(cancel)

			forged := bytes.Repeat([]byte{0}, transport.JoinBindingLen)
			reply, sendErr := dialPhase1WithForgedBind(ctx, t, leader, forged)
			isBindingReject(reply, sendErr)
		})
	})

	ginkgo.Context("RestartNoOp", func() {
		// A fully-joined voter restarted with the (now-consumed) bundle env still
		// set must classify as inviteNormalBoot (artifacts complete + acked) and
		// boot normally — NOT re-redeem the spent invite. It must stay the same
		// voter and keep serving. (Mid-Phase-1 crash injection is intentionally not
		// attempted here: it is timing-flaky over the wire; the resume classifier
		// itself is unit-covered in serveruntime/invite_join_boot_test.go.)
		ginkgo.It("an already-joined node restarts with stale bundle env as a no-op and stays a voter", func() {
			t := ginkgo.GinkgoTB()
			leader := startInviteLeader(t, inviteJoinClusterKey)
			admin, err := bootstrapAdminResultViaUDSForTestMain(leader.dataDir, 30*time.Second)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "bootstrap admin SA")
			ak, sk, saID := admin.AccessKey, admin.SecretKey, admin.SAID

			bundle := mintInvite(t, leader.dataDir)

			joinerDir := shortTempDir(t)
			joiner := startInviteJoiner(t, "joiner", joinerDir, bundle)
			waitForVoter(t, leader.httpURL, "joiner", 90*time.Second)
			waitForPort(t, joiner.httpPort, 60*time.Second)

			// Seed an object through the joiner so we can prove durable state
			// survives the restart.
			bucket := "invite-join-resume"
			gomega.Expect(adminCreateBucketWithPolicyAttachAny(
				[]string{leader.dataDir}, saID, bucket, 60*time.Second)).To(gomega.Succeed())
			joinerCli := s3ClientFor(joiner.httpURL, ak, sk)
			gomega.Expect(waitForIAMReady(joinerCli, 60*time.Second)).To(gomega.Succeed())

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			ginkgo.DeferCleanup(cancel)
			body := []byte("resume payload")
			key := "resume.txt"
			gomega.Eventually(func() error {
				return tryPutObject(ctx, joinerCli, bucket, key, body)
			}, 30*time.Second, 500*time.Millisecond).Should(gomega.Succeed())

			// Restart the already-joined node with the stale bundle env still set.
			restartInviteJoiner(t, joiner, bundle)
			waitForPort(t, joiner.httpPort, 60*time.Second)

			// It must remain the SAME voter (no re-join as a new node) — assert
			// consistently, not just eventually.
			gomega.Consistently(func() bool {
				s := getStatusJSON(t, leader.httpURL)
				return containsString(stringList(s["peers"]), "joiner")
			}, 3*time.Second, 500*time.Millisecond).Should(gomega.BeTrue(),
				"restarted joiner must stay a voter as a normal no-op boot")

			// And it still serves: a fresh PutObject through the restarted joiner
			// succeeds (IAM SA still recognized, DEK ready, forward-to-leader intact).
			joinerCli2 := s3ClientFor(joiner.httpURL, ak, sk)
			gomega.Expect(waitForIAMReady(joinerCli2, 60*time.Second)).To(gomega.Succeed())
			gomega.Eventually(func() error {
				return tryPutObject(ctx, joinerCli2, bucket, "resume-after.txt", body)
			}, 30*time.Second, 500*time.Millisecond).Should(gomega.Succeed(),
				"restarted joiner must still serve writes")
		})
	})
})
