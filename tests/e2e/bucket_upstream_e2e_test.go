package e2e

import (
	"context"
	"fmt"
	"os/exec"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Bucket upstream", ginkgo.Label("bucket"), func() {
	describeBucketUpstreamContext("SingleNode", func() s3Target {
		return newSingleNodeS3Target()
	})
	describeBucketUpstreamContext("Cluster4Node", func() s3Target {
		return newSharedClusterS3Target(ginkgo.GinkgoTB())
	})
})

func describeBucketUpstreamContext(name string, factory func() s3Target) {
	ginkgo.Context(name, func() {
		var (
			tgt    s3Target
			binary string
			sock   string
		)

		ginkgo.BeforeEach(func() {
			tgt = factory()
			binary = getBinary()
			sock = tgt.adminSockPath()
		})

		runBucketUpstreamCases(
			func() s3Target { return tgt },
			func() string { return binary },
			func() string { return sock },
		)
	})
}

func runBucketUpstreamCases(getTgt func() s3Target, getBinaryPath func() string, getSock func() string) {
	// LegacyCLI_Removed first — fixture-independent, no state mutation.
	ginkgo.It("removes the legacy IAM CLI path (LegacyCLI_Removed)", func() {
		binary := getBinaryPath()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		out, _ := exec.CommandContext(ctx, binary, "iam", "--help").CombinedOutput()
		gomega.Expect(string(out)).NotTo(gomega.ContainSubstring("bucket-upstream"),
			"expected 'bucket-upstream' to be absent from `grainfs iam --help`")

		ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel2()
		legacy := exec.CommandContext(ctx2, binary, "iam", "bucket-upstream", "set", "xb",
			"--endpoint", "/tmp/nonexistent.sock",
			"--endpoint-url", "http://x:1",
			"--access-key", "x",
			"--secret-key", "x",
		)
		out, err := legacy.CombinedOutput()
		gomega.Expect(err).To(gomega.HaveOccurred(), "legacy `iam bucket-upstream set ...` must exit non-zero; got output: %s", out)
	})

	ginkgo.It("round-trips put/get/list/delete through the bucket CLI (CLIRoundtrip)", func() {
		tgt := getTgt()
		binary := getBinaryPath()
		sock := getSock()
		name := fmt.Sprintf("up-%s-%d", tgt.name, time.Now().UnixNano())

		// PUT
		{
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			putCmd := exec.CommandContext(ctx, binary, "bucket", "upstream", "put", name,
				"--endpoint", sock,
				"--endpoint-url", "http://upstream.example:9000",
				"--access-key", "AKUP",
				"--secret-key", "upstream-secret-plain",
			)
			out, err := putCmd.CombinedOutput()
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "put: %s", string(out))
			ginkgo.DeferCleanup(func(ctx context.Context) {
				_, _ = exec.CommandContext(ctx, binary, "bucket", "upstream", "delete", name, "--endpoint", sock).CombinedOutput()
			}, ginkgo.NodeTimeout(10*time.Second))
		}

		// GET — record present, secret_key absent
		{
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			out, err := exec.CommandContext(ctx, binary, "bucket", "--json", "upstream", "get", name,
				"--endpoint", sock,
			).CombinedOutput()
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "get: %s", string(out))
			body := string(out)
			gomega.Expect(body).To(gomega.ContainSubstring(`"upstream_url":"http://upstream.example:9000"`), "wire JSON must use upstream_url")
			gomega.Expect(body).To(gomega.ContainSubstring(`"access_key":"AKUP"`))
			gomega.Expect(body).NotTo(gomega.ContainSubstring("upstream-secret-plain"), "GET response must not leak plaintext secret")
			gomega.Expect(body).NotTo(gomega.ContainSubstring(`"secret_key"`), "GET response must not include secret_key field")
		}

		// LIST — name in array
		{
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			out, err := exec.CommandContext(ctx, binary, "bucket", "--json", "upstream", "list",
				"--endpoint", sock,
			).CombinedOutput()
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "list: %s", string(out))
			body := string(out)
			gomega.Expect(body).To(gomega.ContainSubstring(fmt.Sprintf(`"bucket":%q`, name)), "list must include the registered bucket")
			gomega.Expect(body).NotTo(gomega.ContainSubstring("upstream-secret-plain"), "list must not leak plaintext")
		}

		// DELETE
		{
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			out, err := exec.CommandContext(ctx, binary, "bucket", "upstream", "delete", name,
				"--endpoint", sock,
			).CombinedOutput()
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "delete: %s", string(out))
		}

		// GET after delete → 404
		{
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			out, err := exec.CommandContext(ctx, binary, "bucket", "upstream", "get", name,
				"--endpoint", sock,
			).CombinedOutput()
			gomega.Expect(err).To(gomega.HaveOccurred(), "GET after delete must fail; output: %s", string(out))
			gomega.Expect(string(out)).To(gomega.ContainSubstring("not found"), "post-delete GET must surface not found in output")
		}
	})
}
