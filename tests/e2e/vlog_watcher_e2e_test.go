//go:build !race
// +build !race

package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// scrapeMetric returns the first sample value matching name + label substring,
// or -1 if not found. Tests that need the full label-set should parse via
// expfmt; substring matching is sufficient here.
func scrapeMetric(t testing.TB, endpoint, name, labelSubstr string) float64 {
	t.Helper()
	resp, err := http.Get(endpoint + "/metrics")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	for _, line := range strings.Split(string(body), "\n") {
		if !strings.HasPrefix(line, name) {
			continue
		}
		if labelSubstr != "" && !strings.Contains(line, labelSubstr) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		var v float64
		if _, err := fmt.Sscanf(fields[len(fields)-1], "%g", &v); err == nil {
			return v
		}
	}
	return -1
}

// vlogWatcherArgs is the union of flags every VlogWatcher sub-test needs.
// The combined set still satisfies each individual sub-test because their
// assertions are orthogonal: MetricsLive watches gauges, SustainedWrite
// watches GC counters, FiresOnLeak watches incident emission.
var vlogWatcherArgs = []string{
	"--vlog-poll-interval=500ms",
	"--vlog-eta-window=2s",
	"--vlog-recovery-window=2s",
	"--vlog-smoke-defer=2s",
	"--badger-gc-interval=500ms",
	"--badger-value-threshold=64",
	"--vlog-warn-ratio=0.0000001",
	"--vlog-critical-ratio=0.5",
}

// Vlog watcher exercises the predictive vlog watcher's three observable
// surfaces: live metrics, GC counter advance under sustained write, and
// incident emission once observed vlog usage crosses --vlog-warn-ratio.
// Each branch boots one dedicated fixture with the union of all watcher
// flags; sub-tests run sequentially on it.
var _ = ginkgo.Describe("Vlog watcher", ginkgo.Ordered, func() {
	registerWatcherTarget := func(name string, newTarget func(testing.TB, []string) s3Target) {
		ginkgo.Context("Predictive watcher "+name, func() {
			var tgt s3Target
			ginkgo.BeforeAll(func() {
				tgt = newTarget(ginkgo.GinkgoTB(), vlogWatcherArgs)
			})

			ginkgo.It("publishes live metrics", func() {
				runVlogWatcherMetricsLive(ginkgo.GinkgoTB(), tgt)
			})
			ginkgo.It("advances GC counters under sustained writes", func() {
				runVlogWatcherSustainedWriteNoStarvation(ginkgo.GinkgoTB(), tgt)
			})
			ginkgo.It("emits an incident on vlog pressure", func() {
				runVlogWatcherFiresOnLeak(ginkgo.GinkgoTB(), tgt)
			})
		})
	}

	registerGCTickerTarget := func(name string, newTarget func(testing.TB, []string) s3Target) {
		args := []string{
			"--badger-gc-interval=500ms",
			"--vlog-smoke-defer=2s",
		}
		ginkgo.Context("GC ticker "+name, func() {
			var tgt s3Target
			ginkgo.BeforeEach(func() {
				tgt = newTarget(ginkgo.GinkgoTB(), args)
			})

			ginkgo.It("recovers after deletion", func() {
				runGCTickerRecoversAfterDeletion(ginkgo.GinkgoTB(), tgt)
			})
		})
	}

	registerStrictTarget := func(name string, newTarget func(testing.TB, []string) s3Target) {
		args := []string{
			"--strict-vlog-registry=true",
			"--vlog-smoke-defer=3s",
		}
		ginkgo.Context("Strict registry "+name, func() {
			var tgt s3Target
			ginkgo.BeforeEach(func() {
				tgt = newTarget(ginkgo.GinkgoTB(), args)
			})

			ginkgo.It("fatally exits or emits an incident for a planted file", func() {
				runVlogStrictRegistryPlantedFileTriggersFatalOrIncident(ginkgo.GinkgoTB(), tgt)
			})
		})
	}

	single := func(t testing.TB, args []string) s3Target {
		return newDedicatedSingleNodeS3Target(t, args)
	}
	cluster := func(t testing.TB, args []string) s3Target {
		return newClusterS3TargetWithExtraArgs(t, 4, args)
	}

	registerWatcherTarget("SingleNode", single)
	registerWatcherTarget("Cluster4Node", cluster)
	registerGCTickerTarget("SingleNode", single)
	registerGCTickerTarget("Cluster4Node", cluster)
	registerStrictTarget("SingleNode", single)
	registerStrictTarget("Cluster4Node", cluster)
})

func runVlogWatcherMetricsLive(t testing.TB, tgt s3Target) {
	t.Helper()
	endpoint := tgt.endpoint(0)

	gomega.Eventually(func() bool {
		return scrapeMetric(t, endpoint, "grainfs_vlog_limit_bytes", "") > 0
	}).WithTimeout(8*time.Second).WithPolling(200*time.Millisecond).Should(gomega.BeTrue(),
		"vlog_limit_bytes must be > 0 (proves statfs Snapshot() ran)")

	gomega.Expect(scrapeMetric(t, endpoint, "grainfs_vlog_used_ratio", "")).To(gomega.BeNumerically(">=", 0.0),
		"vlog_used_ratio gauge must be present (recorder running)")

	gomega.Eventually(func() bool {
		return scrapeMetric(t, endpoint, "grainfs_vlog_bytes_by_category", `category="meta"`) >= 0
	}).WithTimeout(8*time.Second).WithPolling(200*time.Millisecond).Should(gomega.BeTrue(),
		"meta category must have a vlog_bytes_by_category sample")
}

func runVlogWatcherSustainedWriteNoStarvation(t testing.TB, tgt s3Target) {
	t.Helper()
	endpoint := tgt.endpoint(0)
	cli := tgt.pickNode(0)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		bucket := tgt.uniqueBucket(t, fmt.Sprintf("vlogns%d", i))
		for j := 0; j < 6; j++ {
			payload := make([]byte, 1024)
			_, _ = rand.Read(payload)
			_, err := cli.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(fmt.Sprintf("k-%d", j)),
				Body:   bytes.NewReader(payload),
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	}

	categories := []string{"meta"}
	for _, cat := range categories {
		labelSubstr := fmt.Sprintf(`category=%q`, cat)
		gomega.Eventually(func() bool {
			v := scrapeMetric(t, endpoint, "grainfs_badger_gc_runs_total", labelSubstr)
			return v > 0
		}).WithTimeout(10*time.Second).WithPolling(200*time.Millisecond).Should(gomega.BeTrue(),
			"GC runs counter must advance for category=%s within 10s", cat)
	}
}

func runVlogWatcherFiresOnLeak(t testing.TB, tgt s3Target) {
	t.Helper()
	endpoint := tgt.endpoint(0)
	cli := tgt.pickNode(0)
	ctx := context.Background()

	gomega.Eventually(func() bool {
		return scrapeMetric(t, endpoint, "grainfs_vlog_limit_bytes", "") > 0
	}).WithTimeout(8*time.Second).WithPolling(200*time.Millisecond).Should(gomega.BeTrue(), "watcher must be running")

	bucket := tgt.uniqueBucket(t, "vlogleak")
	payload := make([]byte, 4096)
	_, _ = rand.Read(payload)
	for i := 0; i < 50; i++ {
		_, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(fmt.Sprintf("k-%d", i)),
			Body:   bytes.NewReader(payload),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	// Badger's db.Size() metric is refreshed by an internal 1-minute ticker.
	gomega.Eventually(func() bool {
		for _, inc := range fetchIncidentsSafe(t, endpoint) {
			if inc.Cause == "vlog_pressure" {
				return true
			}
		}
		return false
	}).WithTimeout(90*time.Second).WithPolling(1*time.Second).Should(gomega.BeTrue(),
		"watcher must record a vlog_pressure incident once vlog crosses warn ratio")
}

// GC ticker boots a fixture with a fast GC ticker and asserts that
// grainfs_badger_gc_runs_total advances within several ticks. The metric
// is wired by gcMetricsRecorder; this regression catches the case where the
// counter declaration drifts away from the increment site.
func runGCTickerRecoversAfterDeletion(t testing.TB, tgt s3Target) {
	t.Helper()
	endpoint := tgt.endpoint(0)
	cli := tgt.pickNode(0)
	ctx := context.Background()

	bucket := tgt.uniqueBucket(t, "gctick")
	for i := 0; i < 4; i++ {
		_, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(fmt.Sprintf("k-%d", i)),
			Body:   bytes.NewReader([]byte("payload")),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	gomega.Eventually(func() bool {
		v := scrapeMetric(t, endpoint, "grainfs_badger_gc_runs_total", `category="meta"`)
		return v > 0
	}).WithTimeout(10*time.Second).WithPolling(200*time.Millisecond).Should(gomega.BeTrue(), "meta category GC counter must advance")
}

// Strict registry plants an unregistered .vlog file before the
// smoke deferral elapses, then asserts that strict mode either fatally exits
// the process OR emits a registry_under_populated incident. Destructive:
// the fixture may exit non-zero — single sub-test per branch, no other
// sub-tests can share this fixture.
func runVlogStrictRegistryPlantedFileTriggersFatalOrIncident(t testing.TB, tgt s3Target) {
	t.Helper()
	endpoint := tgt.endpoint(0)
	dataDir := filepath.Dir(tgt.adminSockPath())

	// Plant an unregistered vlog directory before the 3s smoke defer expires.
	plantDir := filepath.Join(dataDir, "fake-rogue-db")
	gomega.Expect(os.MkdirAll(plantDir, 0o755)).To(gomega.Succeed())
	plantFile := filepath.Join(plantDir, "000001.vlog")
	gomega.Expect(os.WriteFile(plantFile, []byte("rogue"), 0o644)).To(gomega.Succeed())

	// Either path is acceptable: process exits non-zero, or incident is
	// recorded. We probe by hitting the data-plane HTTP server: when
	// grainfs has exited the listener is gone and Get returns
	// "connection refused" within milliseconds.
	deadline := time.Now().Add(12 * time.Second)
	exited := false
	incidentRaised := false
	probe := &http.Client{Timeout: 500 * time.Millisecond}
	for time.Now().Before(deadline) {
		resp, err := probe.Get(endpoint + "/metrics")
		if err != nil {
			exited = true
			break
		}
		_ = resp.Body.Close()
		incidents := fetchIncidentsSafe(t, endpoint)
		for _, inc := range incidents {
			if inc.Cause == "registry_under_populated" {
				incidentRaised = true
				break
			}
		}
		if incidentRaised {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	gomega.Expect(exited || incidentRaised).To(gomega.BeTrue(),
		"strict mode must fatally exit or emit registry_under_populated incident")
}

// fetchIncidentsSafe is fetchIncidents that returns nil instead of failing
// the test when the endpoint is briefly unreachable (e.g. process is exiting).
func fetchIncidentsSafe(t testing.TB, endpoint string) []incidentState {
	t.Helper()
	resp, err := http.Get(endpoint + "/api/incidents?limit=50")
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	var out []incidentState
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil
	}
	return out
}
