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

// scrapeMetricWithLabel fetches /metrics from endpoint and returns the value
// for a specific metric line that contains the given substring (e.g. `epoch="1"`).
// Returns 0.0 if no matching line is found.
func scrapeMetricWithLabel(t testing.TB, endpoint, metricName, labelSubstr string) float64 {
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
		if strings.HasPrefix(line, "#") {
			continue
		}
		if !strings.HasPrefix(line, metricName+"{") {
			continue
		}
		if !strings.Contains(line, labelSubstr) {
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

// DEK FSM-value rewrap ledger e2e (S7-1a-2): after a data-DEK rotation and FSM-value
// drain, the grainfs_rewrap_progress_reports_total{epoch="1"} counter must increment,
// proving the marker-driven re-Kick ran all lanes (EC+packblob+FSMvalue-check) and
// proposed a DEKRewrapProgress entry with epoch=1 to the meta-raft ledger. This is
// the observable proxy for IsGenFullyRewrapped(..., requiredEpoch=1) — the prune
// precondition gated by S7-final.
//
// Contrast with the S6b/S7-1a tests: before this slice, the same scenario increments
// rewrap lane counters but the progress report carries epoch=0 (no FSM-value check),
// and epoch=1 is never reported.
var _ = ginkgo.Describe("DEK FSM-value rewrap ledger epoch (DEK rotation S7-1a-2)", func() {
	ginkgo.It("reports epoch=1 progress to ledger after FSM-value drain following rotate-dek", func() {
		t := ginkgo.GinkgoTB()

		// packThreshold=1024: small objects → packblob, objects > 1024 → EC.
		const packThreshold = 1024
		cli, dataDir, saID, endpoint := startRewrapServer(t, packThreshold)
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		ginkgo.DeferCleanup(cancel)

		// Create a bucket and PUT an object — writes policy: and obj: FSM-values sealed
		// at gen 0. These will be stale after the rotation and must be drained before
		// the check-lane passes and the progress report can carry epoch=1.
		const bucket = "ledger-epoch-test"
		createBucketWithAdminPolicyAttachViaUDSAny(t, []string{dataDir}, saID, bucket, cli)

		objKey := "seed.bin"
		objBody := bytes.Repeat([]byte{0xAB}, 256)
		_, putErr := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(objKey),
			Body:   bytes.NewReader(objBody),
		})
		gomega.Expect(putErr).NotTo(gomega.HaveOccurred(), "PUT seed object to create stale FSM-value")

		// Capture all baselines BEFORE rotation so we compare against a stable snapshot.
		// Baseline: epoch=1 reports before rotation must be 0 (no rotation yet).
		epoch1Baseline := scrapeMetricWithLabel(t, endpoint, "grainfs_rewrap_progress_reports_total", `epoch="1"`)
		// FSM-value drain baseline: captured before rotation so the Eventually can detect
		// the increment even if the drain completes quickly after the rotation commits.
		fsmBaseline := scrapeRewrapCounter(t, endpoint, "grainfs_rewrap_fsm_values_total")

		// Record DEK generation before rotation.
		beforeGen := kekStatusViaSocket(t, dataDir).ActiveDEKGeneration
		wantGen := beforeGen + 1

		// Trigger data-DEK rotation.
		out, err := configSetViaCLI(dataDir, "encryption.rotate-dek", "now")
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			"config set encryption.rotate-dek now: %s", out)

		// Wait for the active DEK generation to advance (rotation committed).
		gomega.Eventually(func() uint32 {
			return kekStatusViaSocket(t, dataDir).ActiveDEKGeneration
		}, 20*time.Second, 100*time.Millisecond).Should(gomega.Equal(wantGen),
			"active DEK generation must advance to %d after rotate-dek", wantGen)

		// Wait for the FSM-value drain to complete — a prerequisite for the check-lane to pass.
		// The drain trigger runs asynchronously; the drain itself runs on the leader.
		gomega.Eventually(func() float64 {
			return scrapeRewrapCounter(t, endpoint, "grainfs_rewrap_fsm_values_total")
		}, 30*time.Second, 500*time.Millisecond).Should(gomega.BeNumerically(">", fsmBaseline),
			"grainfs_rewrap_fsm_values_total must increment (FSM drain must converge) before epoch-1 report")

		// After the marker is applied and the re-Kick runs all lanes clean,
		// grainfs_rewrap_progress_reports_total{epoch="1"} must increment.
		// This proves the S7-1a-2 path: CmdFSMValueResealDone → re-Kick →
		// FSMValueCheckLane returns nil → ProposeDEKRewrapProgress(epoch=1).
		// Allow up to 60s: the drain runs async, then the marker propose waits
		// for raft apply, then the re-Kick sweeps all lanes and reports.
		gomega.Eventually(func() float64 {
			return scrapeMetricWithLabel(t, endpoint, "grainfs_rewrap_progress_reports_total", `epoch="1"`)
		}, 60*time.Second, 500*time.Millisecond).Should(gomega.BeNumerically(">", epoch1Baseline),
			"grainfs_rewrap_progress_reports_total{epoch=\"1\"} must increment after FSM-value drain + marker re-Kick (baseline=%.0f)", epoch1Baseline)

		// The object must still be readable after the reseal.
		getResp, err := cli.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(objKey),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "GET object must succeed after epoch-1 report")
		defer getResp.Body.Close()
		gotBody, err := io.ReadAll(getResp.Body)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(gotBody).To(gomega.Equal(objBody), "object must be byte-identical after DEK rewrap")
	})
})

// DEK FSM-value rewrap e2e (S7-1a): after a data-DEK rotation, the
// grainfs_rewrap_fsm_values_total counter must increment (the bucket-policy
// value sealed at gen N must be resealed at gen N+1). Multipart state (mpu:)
// must NOT be rewrapped (D4 census-only).
var _ = ginkgo.Describe("DEK FSM-value rewrap lane (DEK rotation S7-1a)", func() {
	ginkgo.It("increments fsm_values rewrap counter after rotate-dek and preserves bucket-policy content", func() {
		t := ginkgo.GinkgoTB()

		// packThreshold=1024: use default pack settings; object data isn't the focus here.
		const packThreshold = 1024
		cli, dataDir, saID, endpoint := startRewrapServer(t, packThreshold)
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		ginkgo.DeferCleanup(cancel)

		// Create a bucket and PUT an object — this writes an obj: FSM-value in the
		// data group, sealed at the current DEK generation (will be stale after rotation).
		const bucket = "fsm-rewrap-test"
		createBucketWithAdminPolicyAttachViaUDSAny(t, []string{dataDir}, saID, bucket, cli)

		objKey := "seed.bin"
		objBody := bytes.Repeat([]byte{0x5A}, 256)
		_, putErr := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(objKey),
			Body:   bytes.NewReader(objBody),
		})
		gomega.Expect(putErr).NotTo(gomega.HaveOccurred(), "PUT seed object to create stale obj: FSM value")

		// Baseline counter before rotation.
		fsmBaseline := scrapeRewrapCounter(t, endpoint, "grainfs_rewrap_fsm_values_total")

		// Record the DEK generation before rotation.
		beforeGen := kekStatusViaSocket(t, dataDir).ActiveDEKGeneration
		wantGen := beforeGen + 1

		// Trigger data-DEK rotation.
		out, err := configSetViaCLI(dataDir, "encryption.rotate-dek", "now")
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			"config set encryption.rotate-dek now: %s", out)

		// Wait for the active DEK generation to advance.
		gomega.Eventually(func() uint32 {
			return kekStatusViaSocket(t, dataDir).ActiveDEKGeneration
		}, 20*time.Second, 100*time.Millisecond).Should(gomega.Equal(wantGen),
			"active DEK generation must advance to %d after rotate-dek", wantGen)

		// Wait for the FSM-values rewrap counter to rise above baseline.
		// The trigger runs asynchronously after the rotation commits; allow up to 30s.
		gomega.Eventually(func() float64 {
			return scrapeRewrapCounter(t, endpoint, "grainfs_rewrap_fsm_values_total")
		}, 30*time.Second, 500*time.Millisecond).Should(gomega.BeNumerically(">", fsmBaseline),
			"grainfs_rewrap_fsm_values_total must increment after DEK rotation (baseline=%.0f)", fsmBaseline)

		// The bucket must still be accessible after the reseal.
		_, err = cli.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "bucket must remain accessible after FSM-value reseal")
	})
})
