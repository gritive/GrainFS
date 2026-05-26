package e2e

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// TestMigrationInjector_CopiesFromSourceToDest verifies the `grainfs migrate inject`
// command copies all objects from a source GrainFS to a destination GrainFS.
var _ = ginkgo.Describe("Migration injector", func() {
	ginkgo.Context("SingleNode", func() {
		ginkgo.It("copies objects from source to destination", func() {
			t := ginkgo.GinkgoTB()
			binary := getBinary()
			ctx := context.Background()

			// --- Source GrainFS ---
			srcDir, err := os.MkdirTemp("", "grainfs-migrate-src-*")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.DeferCleanup(removeE2EDir, srcDir)

			srcPort := freePort()
			srcCmd := exec.Command(binary, "serve",
				"--data", srcDir,
				"--port", fmt.Sprintf("%d", srcPort),
				"--nfs4-port", fmt.Sprintf("%d", freePort()),
				"--nbd-port", fmt.Sprintf("%d", freePort()),
				"--scrub-interval", "0",
				"--lifecycle-interval", "0",
				"--cluster-key", "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899",
			)
			srcCmd.Stdout = os.Stdout
			srcCmd.Stderr = os.Stderr
			gomega.Expect(srcCmd.Start()).To(gomega.Succeed())
			ginkgo.DeferCleanup(terminateProcess, srcCmd)
			waitForPort(t, srcPort, 30*time.Second)

			srcEndpoint := fmt.Sprintf("http://127.0.0.1:%d", srcPort)
			srcBootstrap, _ := bootstrapAdminViaUDSAnyResult(t, []string{srcDir}, 30*time.Second)
			srcAK, srcSK := srcBootstrap.AccessKey, srcBootstrap.SecretKey
			srcClient := s3ClientFor(srcEndpoint, srcAK, srcSK)

			createBucketWithAdminPolicyAttachViaUDSAny(t, []string{srcDir}, srcBootstrap.SAID, "data", srcClient)
			_, err = srcClient.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String("data"),
				Key:    aws.String("hello.txt"),
				Body:   strings.NewReader("hello from source"),
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			_, err = srcClient.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String("data"),
				Key:    aws.String("world.txt"),
				Body:   strings.NewReader("world from source"),
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// --- Destination GrainFS ---
			dstDir, err := os.MkdirTemp("", "grainfs-migrate-dst-*")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.DeferCleanup(removeE2EDir, dstDir)

			dstPort := freePort()
			dstCmd := exec.Command(binary, "serve",
				"--data", dstDir,
				"--port", fmt.Sprintf("%d", dstPort),
				"--nfs4-port", fmt.Sprintf("%d", freePort()),
				"--nbd-port", fmt.Sprintf("%d", freePort()),
				"--scrub-interval", "0",
				"--lifecycle-interval", "0",
				"--cluster-key", "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899",
			)
			dstCmd.Stdout = os.Stdout
			dstCmd.Stderr = os.Stderr
			gomega.Expect(dstCmd.Start()).To(gomega.Succeed())
			ginkgo.DeferCleanup(terminateProcess, dstCmd)
			waitForPort(t, dstPort, 30*time.Second)

			dstEndpoint := fmt.Sprintf("http://127.0.0.1:%d", dstPort)
			dstBootstrap, _ := bootstrapAdminViaUDSAnyResult(t, []string{dstDir}, 30*time.Second)
			dstAK, dstSK := dstBootstrap.AccessKey, dstBootstrap.SecretKey
			dstClient := s3ClientFor(dstEndpoint, dstAK, dstSK)
			createBucketWithAdminPolicyAttachViaUDSAny(t, []string{dstDir}, dstBootstrap.SAID, "data", dstClient)

			// --- Run migration injector ---
			injectCmd := exec.Command(binary, "migrate", "inject",
				"--src", srcEndpoint,
				"--src-access-key", srcAK,
				"--src-secret-key", srcSK,
				"--dst", dstEndpoint,
				"--dst-access-key", dstAK,
				"--dst-secret-key", dstSK,
			)
			injectCmd.Stdout = os.Stdout
			injectCmd.Stderr = os.Stderr
			gomega.Expect(injectCmd.Run()).To(gomega.Succeed(), "migrate inject must succeed")

			// Verify objects are in destination
			for _, key := range []string{"hello.txt", "world.txt"} {
				getResp, err := dstClient.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String("data"),
					Key:    aws.String(key),
				})
				gomega.Expect(err).NotTo(gomega.HaveOccurred(), "object %s must exist in destination after migration", key)
				body, _ := io.ReadAll(getResp.Body)
				getResp.Body.Close()
				gomega.Expect(string(body)).To(gomega.ContainSubstring("source"), "content must match source")
			}
		})
	})
})
