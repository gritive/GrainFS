//go:build integration

package e2e

// THROWAWAY feasibility spike (Zero-CA network-path de-risk).
// Guarded entirely by GRAINFS_ZEROCA_SPIKE=1. NOT production code.
//
// Journey proven here:
//  1. genesis leader boots with full secrets + spike listener,
//  2. leader mints an invite (Ed25519) and writes an InviteBundle token,
//  3. a SECOND node with NO --cluster-key, NO --encryption-key-file, empty
//     data dir, the invite bundle, and GRAINFS_ZEROCA_SPIKE=1 boots to a
//     running server WITHOUT any manually-staged cluster secrets — it pulls
//     encryption.key + KEK + cluster.id + PSK over the wire via SealToPeer,
//  4. an S3 PutObject/GetObject round-trip on the joined node succeeds
//     (proves it actually received a usable encryption.key).

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Zero-CA network-path spike", func() {
	ginkgo.It("boots a secret-less node from an invite + SealToPeer and serves S3", func() {
		t := ginkgo.GinkgoTB()

		leaderData := shortTempDir(t)
		joinerData := shortTempDir(t)
		encKeyFile := makeSharedEncryptionKeyFile(t)
		clusterKey := strings.Repeat("a", 64)

		leaderHTTP := freePort()
		leaderRaft := freePort()
		spikePort := freePort()
		joinerHTTP := freePort()

		spikeAddr := fmt.Sprintf("127.0.0.1:%d", spikePort)

		// --- 1. genesis leader with full secrets + spike listener ---
		leaderCtx, leaderCancel := context.WithCancel(context.Background())
		ginkgo.DeferCleanup(leaderCancel)

		leaderArgs := []string{
			"serve",
			"--data", leaderData,
			"--port", fmt.Sprintf("%d", leaderHTTP),
			"--raft-addr", fmt.Sprintf("127.0.0.1:%d", leaderRaft),
			"--node-id", "spike-leader",
			"--cluster-key", clusterKey,
			"--nfs4-port", "0",
			"--nbd-port", "0",
			"--encryption-key-file", encKeyFile,
			"--scrub-interval", "0",
			"--lifecycle-interval", "0",
		}
		leader := exec.CommandContext(leaderCtx, getBinary(), leaderArgs...)
		leader.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		leader.Env = append(os.Environ(),
			"GRAINFS_ZEROCA_SPIKE=1",
			"GRAINFS_ZEROCA_SPIKE_ADDR="+spikeAddr,
		)
		leaderLog := newSpikeLog(t, "spike-leader")
		leader.Stdout = leaderLog
		leader.Stderr = leaderLog
		gomega.Expect(leader.Start()).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() { terminateProcess(leader) })

		waitForPort(t, leaderHTTP, 30*time.Second)

		// --- 2. read the invite bundle token the leader minted ---
		tokenPath := filepath.Join(leaderData, "spike-invite.token")
		token := waitForFileContent(t, tokenPath, 15*time.Second)
		gomega.Expect(token).NotTo(gomega.BeEmpty())

		// --- 3. secret-less joiner: NO --cluster-key, NO --encryption-key-file ---
		joinerCtx, joinerCancel := context.WithCancel(context.Background())
		ginkgo.DeferCleanup(joinerCancel)

		joinerArgs := []string{
			"serve",
			"--data", joinerData,
			"--port", fmt.Sprintf("%d", joinerHTTP),
			"--node-id", "spike-joiner",
			"--nfs4-port", "0",
			"--nbd-port", "0",
			"--scrub-interval", "0",
			"--lifecycle-interval", "0",
		}
		joiner := exec.CommandContext(joinerCtx, getBinary(), joinerArgs...)
		joiner.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		joiner.Env = append(os.Environ(),
			"GRAINFS_ZEROCA_SPIKE=1",
			"GRAINFS_ZEROCA_SPIKE_LEADER_ADDR="+spikeAddr,
			"GRAINFS_ZEROCA_SPIKE_INVITE="+token,
			"GRAINFS_ZEROCA_SPIKE_NODE_ID=spike-joiner",
		)
		joinerLog := newSpikeLog(t, "spike-joiner")
		joiner.Stdout = joinerLog
		joiner.Stderr = joinerLog
		gomega.Expect(joiner.Start()).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() { terminateProcess(joiner) })

		// --- 4. assert the secret-less joiner came up WITHOUT staged secrets ---
		waitForPort(t, joinerHTTP, 30*time.Second)

		// Sanity: the joiner never had a manually-staged encryption.key /
		// keys/ before boot; it must exist now (delivered via SealToPeer).
		gomega.Expect(filepath.Join(joinerData, "encryption.key")).To(beAnExistingFile())

		// --- 4b. PROOF the QUIC accept-set bypass was exercised ---
		// The joiner's SPKI is in NObody's accept-set, yet the QUIC handshake +
		// stream reached the leader's spike handler. The leader logs the
		// unknown-SPKI peer reaching the handler over QUIC (it never consulted
		// any accept-set). Grep the leader log for that marker.
		gomega.Eventually(func() string {
			b, _ := os.ReadFile(leaderLog.Name())
			return string(b)
		}, 30*time.Second, 200*time.Millisecond).Should(
			gomega.ContainSubstring("handler reached by unknown-SPKI peer over QUIC"),
			"leader spike handler must be reached by the unknown-SPKI joiner over QUIC")

		// --- 5. S3 round-trip on the joined node proves the key works ---
		joinerURL := fmt.Sprintf("http://127.0.0.1:%d", joinerHTTP)
		bootstrap, _ := bootstrapAdminViaUDSAnyResult(t, []string{joinerData}, 30*time.Second)
		cli := s3ClientFor(joinerURL, bootstrap.AccessKey, bootstrap.SecretKey)
		gomega.Expect(waitForIAMReady(cli, 30*time.Second)).To(gomega.Succeed())

		bucket := "spike-bucket"
		createBucketWithAdminPolicyAttachViaUDSAny(t, []string{joinerData}, bootstrap.SAID, bucket, cli)

		payload := []byte("zero-ca-spike-roundtrip")
		putCtx, putCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer putCancel()
		_, err := cli.PutObject(putCtx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("obj"),
			Body:   bytes.NewReader(payload),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "PutObject on joined node")

		getCtx, getCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer getCancel()
		out, err := cli.GetObject(getCtx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("obj"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "GetObject on joined node")
		got := make([]byte, len(payload))
		_, _ = out.Body.Read(got)
		_ = out.Body.Close()
		gomega.Expect(got).To(gomega.Equal(payload))
	})
})

func newSpikeLog(t testing.TB, name string) *os.File {
	f, err := os.CreateTemp("", name+"-*.log")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.DeferCleanup(func() {
		if t.Failed() {
			if b, err := os.ReadFile(f.Name()); err == nil {
				t.Logf("%s log:\n%s", name, b)
			}
		}
		_ = os.Remove(f.Name())
	})
	return f
}

func waitForFileContent(t testing.TB, path string, timeout time.Duration) string {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if b, err := os.ReadFile(path); err == nil && len(strings.TrimSpace(string(b))) > 0 {
			return strings.TrimSpace(string(b))
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("file %s did not get content within %s", path, timeout)
	return ""
}

func beAnExistingFile() gomega.OmegaMatcher { return gomega.BeAnExistingFile() }
