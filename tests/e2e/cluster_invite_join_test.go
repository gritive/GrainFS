//go:build integration

package e2e

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
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

// startInviteLeader boots a genesis leader: a pre-staged keys.d/current.key +
// stable --raft-addr + explicit --join-listen-addr. clusterMode is always true,
// so the Zero-CA join listener starts even on a single node, letting it mint
// invites. The transport PSK is staged to disk (deterministic-key path) rather
// than passed via the removed cluster-key flag.
func startInviteLeader(t testing.TB, clusterKey string) *inviteJoinNode {
	n := &inviteJoinNode{
		nodeID:   "leader",
		dataDir:  shortTempDir(t),
		httpPort: freePort(),
		raftPort: freePort(),
		joinPort: freePort(),
	}
	n.httpURL = fmt.Sprintf("http://127.0.0.1:%d", n.httpPort)
	// Stage the cluster transport PSK on disk before boot (replaces the removed
	// cluster-key flag); resolveOrSeedClusterKey reads keys.d/current.key.
	gomega.Expect(transport.NewKeystore(n.dataDir).WriteCurrent(clusterKey)).To(gomega.Succeed())
	args := []string{
		"serve",
		"--data", n.dataDir,
		"--port", fmt.Sprintf("%d", n.httpPort),
		"--raft-addr", fmt.Sprintf("127.0.0.1:%d", n.raftPort),
		"--join-listen-addr", fmt.Sprintf("127.0.0.1:%d", n.joinPort),
		"--node-id", n.nodeID,
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
// no staged cluster key. extraEnv lets the resume test reuse
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

// dataGroupHasVoter reports whether nodeID appears in ANY data-group's REAL raft
// voter set (raft_voters), aggregated across every supplied admin socket.
// raft_voters is admin-UDS-only (the `cluster health` route), so we shell out to
// the CLI per node (mirrors runRevokeNode). Aggregation is required because a
// node's health only carries REAL config for groups where it has an instantiated
// raft member; a group led purely by joiners reads empty on the leader.
//
// We use .Output() (stdout only): the daemon logs to stderr, and merging it would
// corrupt the JSON and silently turn this into a constant false.
func dataGroupHasVoter(t testing.TB, adminSocks []string, nodeID string) bool {
	t.Helper()
	type dgHealth struct {
		DataGroups *struct {
			Groups []struct {
				RaftVoters []string `json:"raft_voters"`
			} `json:"groups"`
		} `json:"data_groups"`
	}
	for _, sock := range adminSocks {
		out, err := exec.Command(getBinary(), "cluster", "--endpoint", sock, "--format", "json", "health").Output()
		if err != nil {
			continue // a node may be momentarily unreachable; another may report
		}
		var h dgHealth
		if json.Unmarshal(out, &h) != nil || h.DataGroups == nil {
			continue
		}
		for _, g := range h.DataGroups.Groups {
			if containsString(g.RaftVoters, nodeID) {
				return true
			}
		}
	}
	return false
}

// dumpDataGroupHealth returns each socket's `cluster --format json health` output,
// labelled, so a placement-driven precondition miss is diagnosable in one run.
func dumpDataGroupHealth(adminSocks []string) string {
	var b strings.Builder
	for _, sock := range adminSocks {
		out, err := exec.Command(getBinary(), "cluster", "--endpoint", sock, "--format", "json", "health").Output()
		fmt.Fprintf(&b, "=== %s (err=%v) ===\n%s\n", sock, err, string(out))
	}
	return b.String()
}

// dgRow is the aggregated REAL-config view of one data group: its elected
// leader (node id) and voter set (node ids).
type dgRow struct {
	groupID  string
	leaderID string
	voters   []string
}

// dataGroupRows aggregates each data group's REAL raft config across every admin
// socket, keyed by group id. Only the group LEADER's health row carries an
// authoritative leader_id + full voter set, so for each group we keep the row
// that names a leader (preferring the one with the most voters). raft_voters is
// normalized to node ids at the surface; leader_id is the data-group raft
// LeaderID, which is itself a node id (data-group Server.ID IS the node id).
func dataGroupRows(t testing.TB, adminSocks []string) map[string]dgRow {
	t.Helper()
	type wire struct {
		GroupID    string   `json:"group_id"`
		RaftVoters []string `json:"raft_voters"`
		LeaderID   string   `json:"leader_id"`
	}
	type dgHealth struct {
		DataGroups *struct {
			Groups []wire `json:"groups"`
		} `json:"data_groups"`
	}
	best := map[string]dgRow{}
	for _, sock := range adminSocks {
		out, err := exec.Command(getBinary(), "cluster", "--endpoint", sock, "--format", "json", "health").Output()
		if err != nil {
			continue
		}
		var h dgHealth
		if json.Unmarshal(out, &h) != nil || h.DataGroups == nil {
			continue
		}
		for _, g := range h.DataGroups.Groups {
			cand := dgRow{groupID: g.GroupID, leaderID: g.LeaderID, voters: g.RaftVoters}
			cur, ok := best[g.GroupID]
			switch {
			case !ok:
			case cand.leaderID != "" && cur.leaderID == "":
			case cand.leaderID == "" && cur.leaderID != "":
				continue // never downgrade a leader-bearing row to a leaderless one
			case len(cand.voters) <= len(cur.voters):
				continue
			}
			best[g.GroupID] = cand
		}
	}
	return best
}

// logHasLine reports whether the daemon log file at path contains substr. Used
// to confirm from a node's OWN log that a specific evacuation code path fired.
func logHasLine(path, substr string) bool {
	b, err := os.ReadFile(path)
	return err == nil && strings.Contains(string(b), substr)
}

// shardGroupPeersContain reports whether nodeID appears in any shard-group's
// peer_ids mirror, read from the public /api/cluster/status of the given node.
func shardGroupPeersContain(t testing.TB, base, nodeID string) bool {
	t.Helper()
	s := getStatusJSON(t, base)
	groups, _ := s["shard_groups"].([]any)
	for _, g := range groups {
		gm, ok := g.(map[string]any)
		if !ok {
			continue
		}
		if containsString(stringList(gm["peer_ids"]), nodeID) {
			return true
		}
	}
	return false
}

// parseBundleToken extracts the bundle token printed by RunInviteCreate: the
// non-empty line following "Set this on the joining node as ...".

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

		// Proves the data-group evacuation feature (Tasks 1-7): revoking a node
		// EVICTS it from the REAL data-group raft voter sets (raft_voters), not just
		// the peer_ids mirror. raft_voters is admin-UDS-only, so dataGroupHasVoter
		// shells out to `cluster --format json health` on each node's admin socket.
		ginkgo.It("evicts a revoked node from data-group voter sets", func() {
			t := ginkgo.GinkgoTB()
			leader := startInviteLeader(t, inviteJoinClusterKey)
			admin, err := bootstrapAdminResultViaUDSForTestMain(leader.dataDir, 30*time.Second)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "bootstrap admin SA")
			ak, sk, saID := admin.AccessKey, admin.SecretKey, admin.SAID

			// Full socket set (incl. the revoke target) for the precondition; the
			// survivor set excludes the revoked node, whose own local view of its
			// former group stays stale/unauthoritative after removal and would keep
			// reporting itself a voter forever (permanent false-positive flake).
			leaderSock := filepath.Join(leader.dataDir, "admin.sock")
			adminSocks := []string{leaderSock}
			var revokedSock string
			for _, id := range []string{"evac-joiner-1", "evac-joiner-2", "evac-spare"} {
				b := mintInvite(t, leader.dataDir)
				jd := shortTempDir(t)
				j := startInviteJoiner(t, id, jd, b)
				waitForVoter(t, leader.httpURL, id, 90*time.Second)
				waitForPort(t, j.httpPort, 60*time.Second)
				sock := filepath.Join(jd, "admin.sock")
				adminSocks = append(adminSocks, sock)
				if id == "evac-joiner-1" {
					revokedSock = sock
				}
			}
			survivorSocks := make([]string, 0, len(adminSocks))
			for _, s := range adminSocks {
				if s != revokedSock {
					survivorSocks = append(survivorSocks, s)
				}
			}

			// PUT an object so the bucket's data group gains real raft membership.
			bucket := "evac-bucket"
			gomega.Expect(adminCreateBucketWithPolicyAttachAny(
				[]string{leader.dataDir}, saID, bucket, 60*time.Second)).To(gomega.Succeed())
			cli := s3ClientFor(leader.httpURL, ak, sk)
			gomega.Expect(waitForIAMReady(cli, 60*time.Second)).To(gomega.Succeed())
			gomega.Eventually(func() error {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				return tryPutObject(ctx, cli, bucket, "evac.txt", []byte("evac payload"))
			}, 60*time.Second, time.Second).Should(gomega.Succeed())

			// LOAD-BEARING PRECONDITION: evac-joiner-1 IS a REAL data-group voter
			// before revoke (else the post-revoke BeFalse is vacuous). On a miss,
			// dump every socket's health so a placement skew is diagnosable in one run.
			gomega.Eventually(func() bool {
				return dataGroupHasVoter(t, adminSocks, "evac-joiner-1")
			}, 90*time.Second, time.Second).Should(gomega.BeTrue(),
				"PRECONDITION: evac-joiner-1 must be a REAL data-group voter before revoke; health dump:\n"+dumpDataGroupHealth(adminSocks))

			runRevokeNode(t, leader.dataDir, "evac-joiner-1")

			// THE FIX: evac-joiner-1 disappears from every REAL data-group voter set
			// across the survivors (its own stale local view is excluded).
			gomega.Eventually(func() bool {
				return dataGroupHasVoter(t, survivorSocks, "evac-joiner-1")
			}, 120*time.Second, time.Second).Should(gomega.BeFalse(),
				"revoked node must be evicted from all data-group REAL voter sets; survivor health dump:\n"+dumpDataGroupHealth(survivorSocks))

			// And the persisted peer_ids mirror converges (consequence of the real
			// ChangeMembership the evacuation controller drives).
			gomega.Eventually(func() bool {
				return shardGroupPeersContain(t, leader.httpURL, "evac-joiner-1")
			}, 120*time.Second, time.Second).Should(gomega.BeFalse())
		})

		// Regression guard for the revoked-data-group-LEADER self-eviction path.
		// The eviction test above revokes a FOLLOWER, so it never exercises
		// EvacuateVoter's `revokedID == localNodeID` branch. That branch calls
		// node.TransferLeadership() DIRECTLY: v2 PeerMatchIndex is unobservable
		// ((0,false)), so the old pre-wait would time out and strand a revoked
		// leader in raft_voters forever (the bug #652 fixed). Here we read the
		// seeded group's elected leader and revoke THAT node, deterministically
		// forcing the self-transfer path regardless of who won the election.
		ginkgo.It("evicts a revoked node that LEADS its data group (leadership self-transfer)", func() {
			t := ginkgo.GinkgoTB()
			leader := startInviteLeader(t, inviteJoinClusterKey)
			admin, err := bootstrapAdminResultViaUDSForTestMain(leader.dataDir, 30*time.Second)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "bootstrap admin SA")
			ak, sk, saID := admin.AccessKey, admin.SecretKey, admin.SAID

			// nodeID -> node so we can reach each node's admin socket and its
			// daemon log (to confirm the self-transfer path from the leader's log).
			nodeByID := map[string]*inviteJoinNode{"leader": leader}
			adminSocks := []string{filepath.Join(leader.dataDir, "admin.sock")}
			// Three joiners → the seeded group can reach RF≥3, so revoking its
			// leader exercises leadership-transfer, NOT the 2-voter quorum-strand
			// limitation ([P3], a different failure mode).
			for _, id := range []string{"evac-joiner-1", "evac-joiner-2", "evac-spare"} {
				b := mintInvite(t, leader.dataDir)
				jd := shortTempDir(t)
				j := startInviteJoiner(t, id, jd, b)
				waitForVoter(t, leader.httpURL, id, 90*time.Second)
				waitForPort(t, j.httpPort, 60*time.Second)
				nodeByID[id] = j
				adminSocks = append(adminSocks, filepath.Join(jd, "admin.sock"))
			}

			// PUT an object so the bucket's data group gains real raft membership.
			bucket := "evac-leader-bucket"
			gomega.Expect(adminCreateBucketWithPolicyAttachAny(
				[]string{leader.dataDir}, saID, bucket, 60*time.Second)).To(gomega.Succeed())
			cli := s3ClientFor(leader.httpURL, ak, sk)
			gomega.Expect(waitForIAMReady(cli, 60*time.Second)).To(gomega.Succeed())
			gomega.Eventually(func() error {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				return tryPutObject(ctx, cli, bucket, "evac.txt", []byte("evac payload"))
			}, 60*time.Second, time.Second).Should(gomega.Succeed())

			// PRECONDITION: the seeded group has an elected leader and ≥3 voters.
			// We target whichever node is the leader (read, don't force).
			var groupID, leaderID string
			gomega.Eventually(func() bool {
				for gid, row := range dataGroupRows(t, adminSocks) {
					if row.leaderID != "" && len(row.voters) >= 3 &&
						containsString(row.voters, "evac-joiner-1") {
						groupID, leaderID = gid, row.leaderID
						return true
					}
				}
				return false
			}, 90*time.Second, time.Second).Should(gomega.BeTrue(),
				"PRECONDITION: seeded data group needs an elected leader + ≥3 voters; health:\n"+dumpDataGroupHealth(adminSocks))
			ginkgo.GinkgoWriter.Printf("seeded group=%s elected leader=%s\n", groupID, leaderID)
			gomega.Expect(nodeByID).To(gomega.HaveKey(leaderID),
				"leader_id %q must be a known node id (not an unresolved address)", leaderID)
			revokedNode := nodeByID[leaderID]

			// revoke-node removes the target from meta-Raft (RemoveVoter), which is
			// leader-only, so it MUST be issued against the meta-Raft leader — the
			// genesis node here. The data-group leader we target is usually genesis
			// itself; a Raft leader can remove itself (commit a config excluding
			// self, then step down), so this is uniform whether the target is the
			// genesis node or a joiner.
			issuerDir := leader.dataDir
			revokedSock := filepath.Join(revokedNode.dataDir, "admin.sock")
			survivorSocks := make([]string, 0, len(adminSocks))
			for _, s := range adminSocks {
				if s != revokedSock {
					survivorSocks = append(survivorSocks, s)
				}
			}

			runRevokeNode(t, issuerDir, leaderID)

			// THE FIX: the revoked leader steps down via direct TransferLeadership,
			// a survivor takes over and removes it. Assert it leaves every
			// survivor's REAL voter set. (Reverting the fix strands it here.)
			gomega.Eventually(func() bool {
				return dataGroupHasVoter(t, survivorSocks, leaderID)
			}, 120*time.Second, time.Second).Should(gomega.BeFalse(),
				"revoked data-group LEADER must be evicted from all survivor voter sets; health:\n"+dumpDataGroupHealth(survivorSocks))

			// And the group is not left leaderless: a new, non-revoked leader
			// emerges (intent: leadership actually transferred).
			gomega.Eventually(func() bool {
				row, ok := dataGroupRows(t, survivorSocks)[groupID]
				return ok && row.leaderID != "" && row.leaderID != leaderID
			}, 120*time.Second, time.Second).Should(gomega.BeTrue(),
				"seeded group must elect a new non-revoked leader; health:\n"+dumpDataGroupHealth(survivorSocks))

			// Direct path evidence: the revoked node's OWN daemon log shows its
			// evacuator hit the self-eviction branch (revokedID == localNodeID →
			// TransferLeadership). This pins the test to the exact code path, not
			// just its black-box outcome above.
			gomega.Eventually(func() bool {
				return logHasLine(revokedNode.logPath, "evacuator: leadership transferred")
			}, 120*time.Second, time.Second).Should(gomega.BeTrue(),
				"revoked leader's evacuator must log the self-transfer (revokedID==localNodeID branch)")
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
