package e2e

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// scrapeRewrapCounter fetches /metrics from endpoint and returns the sum of all
// label combinations for the named counter. Returns 0.0 if the counter is absent.
func scrapeRewrapCounter(t testing.TB, endpoint, metricName string) float64 {
	t.Helper()
	resp, err := http.Get(endpoint + "/metrics") //nolint:noctx
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0
	}
	var total float64
	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, "#") || !strings.HasPrefix(line, metricName+"{") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		var v float64
		if _, err := fmt.Sscanf(fields[len(fields)-1], "%g", &v); err == nil {
			total += v
		}
	}
	return total
}

// startRewrapServer boots a single-node grainfs server with at-rest encryption
// enabled and packed-blob storage active (small pack threshold). No --node-id
// or --raft-addr so the binary runs as a true single-node with packblob enabled.
// Returns (s3 client, dataDir, saID, httpEndpoint). All cleanup is registered
// via DeferCleanup.
func startRewrapServer(t testing.TB, packThreshold int) (*s3.Client, string, string, string) {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "grainfs-rewrap-e2e-*")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.DeferCleanup(removeE2EDir, dir)

	port := freePort()

	args := []string{
		"serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", port),
		"--nfs4-port", "0",
		"--nbd-port", "0",
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
		"--pack-threshold", fmt.Sprintf("%d", packThreshold),
	}
	cmd := exec.Command(getBinary(), args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdout = ginkgo.GinkgoWriter
	cmd.Stderr = ginkgo.GinkgoWriter
	gomega.Expect(cmd.Start()).To(gomega.Succeed())
	ginkgo.DeferCleanup(func() {
		terminateProcess(cmd)
	})

	endpoint := fmt.Sprintf("http://127.0.0.1:%d", port)
	waitForPort(t, port, 30*time.Second)
	waitSocketReady(t, filepath.Join(dir, "admin.sock"), 15*time.Second)

	bootstrap, _ := bootstrapAdminViaUDSAnyResult(t, []string{dir}, 30*time.Second)
	cli := s3ClientFor(endpoint, bootstrap.AccessKey, bootstrap.SecretKey)
	gomega.Expect(waitForIAMReady(cli, 30*time.Second)).To(gomega.Succeed())

	return cli, dir, bootstrap.SAID, endpoint
}

// DEK rewrap e2e: after a data-DEK rotation both the EC-shard and packblob
// rewrap lanes must advance their counters, and all objects must remain
// byte-identical after rewrap.
//
// Small objects (below packThreshold) land in the packblob index; large
// objects (above packThreshold) are stored as EC shards via the distributed
// backend. Both lanes run on every rotation kick.
//
// This test drove two S6b integration fixes that the fake-backed unit tests
// could not: (1) lanes must be registered after the backends are built
// (wireRewrapLanes runs post-bootBackendWrap, not in the early wireDEKKeeper);
// (2) the EC lane must sweep every data group's own stored objects, because an
// object lives in the placement group recorded in its metadata, which can
// differ from the bucket's current router assignment after a reassignment.
var _ = ginkgo.Describe("DEK rewrap lanes (DEK rotation S6b)", func() {
	ginkgo.It("increments EC + packblob rewrap counters after rotate-dek and preserves object content", func() {
		t := ginkgo.GinkgoTB()

		// packThreshold=1024: objects < 1024 bytes → packblob; larger → EC shards.
		const packThreshold = 1024

		cli, dataDir, saID, endpoint := startRewrapServer(t, packThreshold)
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		ginkgo.DeferCleanup(cancel)

		// Create a bucket for the test.
		const bucket = "rewrap-test"
		createBucketWithAdminPolicyAttachViaUDSAny(t, []string{dataDir}, saID, bucket, cli)

		// PUT a small object (below packThreshold) → lands in packblob index.
		smallKey := "small.bin"
		smallBody := bytes.Repeat([]byte{0x42}, 256) // 256 bytes, well below 1024
		_, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(smallKey),
			Body:   bytes.NewReader(smallBody),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "PUT small object (packblob)")

		// PUT a large object (above packThreshold) → stored as EC shards.
		largeKey := "large.bin"
		largeBody := bytes.Repeat([]byte{0x7F}, 2<<20) // 2 MiB, well above 1024
		_, err = cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(largeKey),
			Body:   bytes.NewReader(largeBody),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "PUT large object (EC shards)")

		// Capture baseline counter values before rotation (may be 0 or absent).
		ecBaseline := scrapeRewrapCounter(t, endpoint, "grainfs_rewrap_ec_shards_total")
		packBaseline := scrapeRewrapCounter(t, endpoint, "grainfs_rewrap_packblob_entries_total")

		// Capture DEK generation before rotation for the intermediate assertion.
		beforeGen := kekStatusViaSocket(t, dataDir).ActiveDEKGeneration
		wantGen := beforeGen + 1

		// Trigger data-DEK rotation via the operator config surface.
		out, err := configSetViaCLI(dataDir, "encryption.rotate-dek", "now")
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			"config set encryption.rotate-dek now: %s", out)

		// Wait for the active DEK generation to advance — this confirms the
		// rotation committed before we check the rewrap counters.
		gomega.Eventually(func() uint32 {
			return kekStatusViaSocket(t, dataDir).ActiveDEKGeneration
		}, 20*time.Second, 100*time.Millisecond).Should(gomega.Equal(wantGen),
			"active DEK generation must advance to %d after rotate-dek", wantGen)

		// Wait for BOTH rewrap counters to rise above baseline. The scrubberKick
		// runs asynchronously after the DEK rotation commits; allow up to 30s.
		gomega.Eventually(func() float64 {
			return scrapeRewrapCounter(t, endpoint, "grainfs_rewrap_ec_shards_total")
		}, 30*time.Second, 500*time.Millisecond).Should(gomega.BeNumerically(">=", ecBaseline+1),
			"grainfs_rewrap_ec_shards_total must increment after DEK rotation (baseline=%.0f)", ecBaseline)

		gomega.Eventually(func() float64 {
			return scrapeRewrapCounter(t, endpoint, "grainfs_rewrap_packblob_entries_total")
		}, 30*time.Second, 500*time.Millisecond).Should(gomega.BeNumerically(">=", packBaseline+1),
			"grainfs_rewrap_packblob_entries_total must increment after DEK rotation (baseline=%.0f)", packBaseline)

		// Both objects must still GET and return byte-identical content after rewrap.
		getSmall, err := cli.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(smallKey),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "GET small object after rewrap")
		defer getSmall.Body.Close()
		gotSmall, err := io.ReadAll(getSmall.Body)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(gotSmall).To(gomega.Equal(smallBody),
			"small (packblob) object must be byte-identical after DEK rewrap")

		getLarge, err := cli.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(largeKey),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "GET large object after rewrap")
		defer getLarge.Body.Close()
		gotLarge, err := io.ReadAll(getLarge.Body)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(gotLarge).To(gomega.Equal(largeBody),
			"large (EC shards) object must be byte-identical after DEK rewrap")
	})
})
