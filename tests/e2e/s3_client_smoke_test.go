package e2e

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// S3ClientSmoke exercises host-safe external S3-compatible clients
// against both single-node and cluster fixtures. FUSE-backed clients
// run from Linux in tests/fuse_s3_colima because macOS host FUSE availability
// is not a reliable e2e prerequisite.
var _ = ginkgo.Describe("S3 client smoke", ginkgo.Label("s3"), func() {
	describeS3ClientSmokeContext("SingleNode", func() s3Target {
		return newSingleNodeS3Target()
	})
	describeS3ClientSmokeContext("Cluster4Node", func() s3Target {
		return newSharedClusterS3Target(ginkgo.GinkgoTB())
	})
})

func describeS3ClientSmokeContext(name string, factory func() s3Target) {
	ginkgo.Context(name, ginkgo.Ordered, func() {
		var tgt s3Target

		ginkgo.BeforeAll(func() {
			tgt = factory()
		})

		runS3ClientSmokeCases(func() s3Target { return tgt })
	})
}

func runS3ClientSmokeCases(getTgt func() s3Target) {
	ginkgo.It("round-trips an object through MinIO mc", func(ctx context.Context) {
		testS3ClientSmokeMinIOMC(getTgt())
	}, ginkgo.NodeTimeout(60*time.Second))
}

func testS3ClientSmokeMinIOMC(tgt s3Target) {
	ginkgo.GinkgoHelper()
	_, err := exec.LookPath("mc")
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "mc is required for S3ClientSmoke; install MinIO Client or run the Colima-specific FUSE smoke target separately")

	bucket := createSpecBucket(tgt, "mc")
	endpoint := tgt.endpoint(0)
	tmpDir := ginkgo.GinkgoTB().TempDir()
	configDir := filepath.Join(tmpDir, "mc-config")
	gomega.Expect(os.MkdirAll(configDir, 0o700)).To(gomega.Succeed())
	srcPath := filepath.Join(tmpDir, "src.txt")
	gomega.Expect(os.WriteFile(srcPath, []byte("mc smoke"), 0o600)).To(gomega.Succeed())

	runClientCommand(nil, "mc", "--config-dir", configDir, "alias", "set", "grainfs", endpoint, tgt.accessKey, tgt.secretKey, "--api", "S3v4", "--path", "on")
	runClientCommand(nil, "mc", "--config-dir", configDir, "cp", srcPath, "grainfs/"+bucket+"/smoke.txt")
	out := runClientCommand(nil, "mc", "--config-dir", configDir, "cat", "grainfs/"+bucket+"/smoke.txt")
	gomega.Expect(string(out)).To(gomega.Equal("mc smoke"))
	out = runClientCommand(nil, "mc", "--config-dir", configDir, "ls", "grainfs/"+bucket)
	gomega.Expect(string(out)).To(gomega.ContainSubstring("smoke.txt"))
	runClientCommand(nil, "mc", "--config-dir", configDir, "rm", "grainfs/"+bucket+"/smoke.txt")
	requireObjectDeleted(tgt, bucket, "smoke.txt")
}

func requireObjectDeleted(tgt s3Target, bucket, key string) {
	ginkgo.GinkgoHelper()
	cli := tgt.pickNode(0)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	gomega.Eventually(func() bool {
		_, err := cli.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		return err != nil
	}, 10*time.Second, 200*time.Millisecond).Should(gomega.BeTrue())
}

func runClientCommand(env []string, name string, args ...string) []byte {
	ginkgo.GinkgoHelper()
	out, err := runClientCommandAllowError(env, name, args...)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "%s %s\n%s", name, strings.Join(args, " "), out)
	return out
}

func runClientCommandAllowError(env []string, name string, args ...string) ([]byte, error) {
	ginkgo.GinkgoHelper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = append(os.Environ(), env...)
	out, err := cmd.CombinedOutput()
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return out, fmt.Errorf("%s timed out", name)
	}
	return out, err
}
