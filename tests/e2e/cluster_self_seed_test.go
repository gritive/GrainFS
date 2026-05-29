//go:build integration

package e2e

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// Genesis cluster-key self-seed e2e (docs/superpowers/specs/2026-05-29-genesis-
// cluster-key-self-seed.md): a keyless genesis on a fresh data dir generates and
// persists its own cluster transport key instead of erroring.

// selfSeedLeaderArgs is startInviteLeader's arg set MINUS --cluster-key. It keeps
// --join-listen-addr so the self-seeded leader can still mint invites.
func selfSeedLeaderArgs(n *inviteJoinNode) []string {
	return []string{
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
}

func startSelfSeedLeader(t testing.TB, nodeID string) *inviteJoinNode {
	n := &inviteJoinNode{
		nodeID:   nodeID,
		dataDir:  shortTempDir(t),
		httpPort: freePort(),
		raftPort: freePort(),
		joinPort: freePort(),
	}
	n.httpURL = fmt.Sprintf("http://127.0.0.1:%d", n.httpPort)
	startInviteProc(t, n, selfSeedLeaderArgs(n), nil) // nil env → NO GRAINFS_INVITE_BUNDLE
	waitForPort(t, n.httpPort, 60*time.Second)
	return n
}

var _ = ginkgo.Describe("Genesis cluster-key self-seed", func() {
	ginkgo.It("a keyless genesis self-seeds, persists current.key, and serves S3", func() {
		t := ginkgo.GinkgoTB()
		leader := startSelfSeedLeader(t, "selfseed-leader")
		admin, err := bootstrapAdminResultViaUDSForTestMain(leader.dataDir, 30*time.Second)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "bootstrap admin SA")

		gomega.Expect(filepath.Join(leader.dataDir, "keys.d", "current.key")).To(gomega.BeAnExistingFile())
		_, statErr := os.Stat(filepath.Join(leader.dataDir, "encryption.key"))
		gomega.Expect(os.IsNotExist(statErr)).To(gomega.BeTrue(), "no legacy encryption.key")

		bucket := "selfseed-bucket"
		gomega.Expect(adminCreateBucketWithPolicyAttachAny(
			[]string{leader.dataDir}, admin.SAID, bucket, 60*time.Second)).To(gomega.Succeed())
		cli := s3ClientFor(leader.httpURL, admin.AccessKey, admin.SecretKey)
		gomega.Expect(waitForIAMReady(cli, 60*time.Second)).To(gomega.Succeed())
		gomega.Eventually(func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			return tryPutObject(ctx, cli, bucket, "k.txt", []byte("v"))
		}, 60*time.Second, time.Second).Should(gomega.Succeed())
	})

	ginkgo.It("a secret-less node invite-joins a self-seeded leader and becomes a voter", func() {
		t := ginkgo.GinkgoTB()
		leader := startSelfSeedLeader(t, "selfseed-leader")
		_, err := bootstrapAdminResultViaUDSForTestMain(leader.dataDir, 30*time.Second)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "bootstrap admin SA")

		bundle := mintInvite(t, leader.dataDir)
		startInviteJoiner(t, "ss-joiner", shortTempDir(t), bundle)
		waitForVoter(t, leader.httpURL, "ss-joiner", 90*time.Second)
	})

	// NOTE: self-seed RESTART semantics (restart loads keys.d/current.key, does NOT
	// re-seed) are covered by the unit test TestBootValidateConfigSelfSeeds. A
	// full-process e2e restart of a SOLO leader is intentionally omitted: it fails
	// `WaitDEKReady: context deadline exceeded` for BOTH a --cluster-key leader and a
	// self-seeded one (verified with a control), i.e. a PRE-EXISTING solo-leader-
	// restart limitation in the KEK/DEK path, unrelated to this cluster-key change.
	// Captured in TODOS for separate investigation.

	ginkgo.It("two keyless genesis nodes form two distinct clusters (no silent merge)", func() {
		t := ginkgo.GinkgoTB()
		a := startSelfSeedLeader(t, "selfseed-a")
		b := startSelfSeedLeader(t, "selfseed-b")
		idA, err := os.ReadFile(filepath.Join(a.dataDir, "cluster.id"))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		idB, err := os.ReadFile(filepath.Join(b.dataDir, "cluster.id"))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(idA).NotTo(gomega.Equal(idB), "independent self-seeds must not share a cluster.id")
	})
})
