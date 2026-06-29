package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// configSetViaCLI execs `grainfs config set <key> <value> --endpoint <admin.sock>`
// — the REAL operator surface for data-DEK rotation (admin UDS PUT /v1/config/<key>
// → ProposeConfigPut), NOT the typed `grainfs cluster config set` allowlist (which
// cannot carry encryption.rotate-dek).
func configSetViaCLI(dataDir, key, value string) (string, error) {
	args := []string{"config", "set", key, value, "--endpoint", filepath.Join(dataDir, "admin.sock")}
	cmd := exec.Command(getBinary(), args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return stdout.String(), fmt.Errorf("%w: stdout=%s stderr=%s", err, stdout.String(), stderr.String())
	}
	return stdout.String(), nil
}

// End-to-end coverage for S5: enabling data-DEK rotation. Drives the real
// operator path — `grainfs config set encryption.rotate-dek now` on the leader's
// admin socket → ConfigPut → post-commit dispatcher → ProposeDEKRotate → raft →
// every node advances the active DEK generation. Then proves the data plane:
// objects sealed before the rotation stay readable (old gen retained) and new
// writes succeed under the new gen, across the EC-shard and object lanes.
var _ = ginkgo.Describe("data-DEK rotation (encryption.rotate-dek)", func() {
	ginkgo.It("advances the active DEK generation cluster-wide and keeps pre/post-rotation objects readable", func() {
		t := ginkgo.GinkgoTB()
		c := startE2ECluster(t, e2eClusterOptions{
			Nodes:     3,
			Mode:      ClusterModeDynamicJoin,
			LogPrefix: "dek-rotate-cluster",
		})
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		ginkgo.DeferCleanup(cancel)

		bucket := "dek-rotation"
		leaderIdx, err := c.EnsureBucketWritable(ctx, bucket, 60*time.Second)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		leaderDir := c.dataDirs[leaderIdx]

		// PUT a multi-MB object (multi-chunk EC shards) BEFORE the rotation.
		preKey := "pre-rotation.bin"
		preBody := make([]byte, 5<<20)
		_, err = rand.Read(preBody)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(tryPutObject(ctx, c.S3Client(leaderIdx), bucket, preKey, preBody)).To(gomega.Succeed())

		before := kekStatusViaSocket(t, leaderDir).ActiveDEKGeneration
		want := before + 1

		// Trigger the rotation via the operator surface.
		out, err := configSetViaCLI(leaderDir, "encryption.rotate-dek", "now")
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "config set encryption.rotate-dek now: %s", out)

		// The active DEK generation must advance on the leader AND replicate to a
		// follower (the dispatcher proposes asynchronously; raft fans it out).
		gomega.Eventually(func() uint32 {
			return kekStatusViaSocket(t, leaderDir).ActiveDEKGeneration
		}, 20*time.Second, 100*time.Millisecond).Should(gomega.Equal(want),
			"leader node %d (%s) should reach active_dek_generation=%d", leaderIdx, c.nodeID(leaderIdx), want)

		followerIdx := -1
		for i := range c.dataDirs {
			if i != leaderIdx {
				followerIdx = i
				break
			}
		}
		gomega.Expect(followerIdx).NotTo(gomega.Equal(-1), "expected at least one follower")
		gomega.Eventually(func() uint32 {
			return kekStatusViaSocket(t, c.dataDirs[followerIdx]).ActiveDEKGeneration
		}, 20*time.Second, 100*time.Millisecond).Should(gomega.Equal(want),
			"follower node %d (%s) should replicate active_dek_generation=%d", followerIdx, c.nodeID(followerIdx), want)

		// PUT a new object AFTER the rotation — sealed under the new gen.
		postKey := "post-rotation.bin"
		postBody := make([]byte, 5<<20)
		_, err = rand.Read(postBody)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(tryPutObject(ctx, c.S3Client(leaderIdx), bucket, postKey, postBody)).To(gomega.Succeed())

		// Both objects must be byte-identical on read: the pre-rotation object
		// decrypts under its retained old gen; the new object under the active gen.
		gomega.Eventually(func() ([]byte, error) {
			return getObjectBytes(ctx, c.S3Client(leaderIdx), bucket, preKey)
		}, 20*time.Second, 200*time.Millisecond).Should(gomega.Equal(preBody),
			"pre-rotation object must still decrypt under its retained generation")

		got, err := getObjectBytes(ctx, c.S3Client(leaderIdx), bucket, postKey)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(got).To(gomega.Equal(postBody), "post-rotation object must decrypt under the new generation")
	})
})
