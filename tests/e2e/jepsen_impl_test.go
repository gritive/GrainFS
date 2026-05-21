package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Jepsen raft cluster", func() {
	ginkgo.It("keeps concurrent writes linearizable", func() {
		t := ginkgo.GinkgoTB()
		// Skip in short mode

		dir, err := os.MkdirTemp("", "grainfs-jepsen-*")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(os.RemoveAll, dir)

		binary := getBinary()
		port := freePort()

		// Start no-peers server
		cmd := exec.Command(binary, "serve",
			"--data", dir,
			"--port", fmt.Sprintf("%d", port),
			"--nfs4-port", fmt.Sprintf("%d", freePort()),
			"--nbd-port", fmt.Sprintf("%d", freePort()),
			"--scrub-interval", "0",
			"--lifecycle-interval", "0",
			"--cluster-key", "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899",
		)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		gomega.Expect(cmd.Start()).To(gomega.Succeed())
		ginkgo.DeferCleanup(terminateProcess, cmd)

		endpoint := fmt.Sprintf("http://127.0.0.1:%d", port)
		waitForPort(t, port, 30*time.Second)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		ginkgo.DeferCleanup(cancel)

		bootstrap, _ := bootstrapAdminViaUDSAnyResult(t, []string{dir}, 30*time.Second)
		ak, sk := bootstrap.AccessKey, bootstrap.SecretKey

		// Create test bucket
		client := s3ClientFor(endpoint, ak, sk)
		createBucketWithAdminPolicyAttachViaUDSAny(t, []string{dir}, bootstrap.SAID, "jepsen-test", client)

		// Run Jepsen test: 10 clients, 100 ops each
		t.Log("Starting concurrent writes with 10 clients...")
		runner := NewJepsenTestRunner(endpoint, ak, sk, 10, 100)
		errors := runner.RunConcurrentPuts(ctx, "jepsen-test", "conflict-key")

		// All operations should succeed
		for i, err := range errors {
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "client %d put should succeed", i)
		}

		t.Log("✓ All concurrent writes succeeded")

		// Verify linearizability: all clients see same value
		t.Log("Verifying linearizability...")
		runner.VerifyLinearizable(ctx, t, "jepsen-test", "conflict-key")

		t.Log("✅ Jepsen test passed - linearizability verified")
	})
})
