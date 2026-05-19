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
	"github.com/stretchr/testify/require"
)

// scrapeMetric returns the first sample value matching name + label substring,
// or -1 if not found. Tests that need the full label-set should parse via
// expfmt; substring matching is sufficient here.
func scrapeMetric(t *testing.T, endpoint, name, labelSubstr string) float64 {
	t.Helper()
	resp, err := http.Get(endpoint + "/metrics")
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
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

// TestVlogWatcherE2E exercises the predictive vlog watcher's three observable
// surfaces: live metrics, GC counter advance under sustained write, and
// incident emission once observed vlog usage crosses --vlog-warn-ratio.
// Each branch boots one dedicated fixture with the union of all watcher
// flags; sub-tests run sequentially on it.
func TestVlogWatcherE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runVlogWatcherCases(t, newDedicatedSingleNodeS3Target(t, vlogWatcherArgs))
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		runVlogWatcherCases(t, newClusterS3TargetWithExtraArgs(t, 4, vlogWatcherArgs))
	})
}

func runVlogWatcherCases(t *testing.T, tgt s3Target) {
	t.Helper()
	endpoint := tgt.endpoint(0)
	cli := tgt.pickNode(0)
	ctx := context.Background()

	t.Run("MetricsLive", func(t *testing.T) {
		require.Eventually(t, func() bool {
			return scrapeMetric(t, endpoint, "grainfs_vlog_limit_bytes", "") > 0
		}, 8*time.Second, 200*time.Millisecond,
			"vlog_limit_bytes must be > 0 (proves statfs Snapshot() ran)")

		require.GreaterOrEqual(t,
			scrapeMetric(t, endpoint, "grainfs_vlog_used_ratio", ""), 0.0,
			"vlog_used_ratio gauge must be present (recorder running)")

		require.Eventually(t, func() bool {
			return scrapeMetric(t, endpoint, "grainfs_vlog_bytes_by_category", `category="meta"`) >= 0
		}, 8*time.Second, 200*time.Millisecond,
			"meta category must have a vlog_bytes_by_category sample")
	})

	t.Run("SustainedWriteNoStarvation", func(t *testing.T) {
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
				require.NoError(t, err)
			}
		}

		categories := []string{"meta"}
		for _, cat := range categories {
			labelSubstr := fmt.Sprintf(`category=%q`, cat)
			require.Eventually(t, func() bool {
				v := scrapeMetric(t, endpoint, "grainfs_badger_gc_runs_total", labelSubstr)
				return v > 0
			}, 10*time.Second, 200*time.Millisecond,
				"GC runs counter must advance for category=%s within 10s", cat)
		}
	})

	t.Run("FiresOnLeak", func(t *testing.T) {
		require.Eventually(t, func() bool {
			return scrapeMetric(t, endpoint, "grainfs_vlog_limit_bytes", "") > 0
		}, 8*time.Second, 200*time.Millisecond, "watcher must be running")

		bucket := tgt.uniqueBucket(t, "vlogleak")
		payload := make([]byte, 4096)
		_, _ = rand.Read(payload)
		for i := 0; i < 50; i++ {
			_, err := cli.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(fmt.Sprintf("k-%d", i)),
				Body:   bytes.NewReader(payload),
			})
			require.NoError(t, err)
		}

		// Badger's db.Size() metric is refreshed by an internal 1-minute ticker.
		require.Eventually(t, func() bool {
			for _, inc := range fetchIncidentsSafe(t, endpoint) {
				if inc.Cause == "vlog_pressure" {
					return true
				}
			}
			return false
		}, 90*time.Second, 1*time.Second,
			"watcher must record a vlog_pressure incident once vlog crosses warn ratio")
	})
}

// TestE2E_GCTicker_RecoversAfterDeletion boots one node with a fast GC ticker
// and asserts that grainfs_badger_gc_runs_total advances within several ticks.
// The metric is wired by gcMetricsRecorder; this regression catches the case
// where the counter declaration drifts away from the increment site.
func TestE2E_GCTicker_RecoversAfterDeletion(t *testing.T) {
	c := startE2ECluster(t, e2eClusterOptions{
		Nodes:      1,
		LogPrefix:  "vlog-gc",
		DisableNFS: true,
		DisableNBD: true,
		ExtraArgs: []string{
			"--badger-gc-interval=500ms",
			"--vlog-smoke-defer=2s",
		},
	})

	// Touch every category we expect to see GC activity on.
	cli := c.S3Client(0)
	ctx := context.Background()
	require.NoError(t, tryCreateBucket(ctx, cli, "vlog-gc"))
	for i := 0; i < 4; i++ {
		require.NoError(t, tryPutObject(ctx, cli, "vlog-gc", fmt.Sprintf("k-%d", i), []byte("payload")))
	}

	require.Eventually(t, func() bool {
		v := scrapeMetric(t, c.httpURLs[0], "grainfs_badger_gc_runs_total", `category="meta"`)
		return v > 0
	}, 10*time.Second, 200*time.Millisecond, "meta category GC counter must advance")
}

// TestE2E_StrictVlogRegistry_FatalOnMissing plants an unregistered .vlog file
// before the smoke deferral elapses, then asserts that strict mode either
// fatally exits the process OR emits a registry_under_populated incident.
func TestE2E_StrictVlogRegistry_FatalOnMissing(t *testing.T) {
	c := startE2ECluster(t, e2eClusterOptions{
		Nodes:      1,
		LogPrefix:  "vlog-strict",
		DisableNFS: true,
		DisableNBD: true,
		ExtraArgs: []string{
			"--strict-vlog-registry=true",
			"--vlog-smoke-defer=3s",
		},
	})

	// Plant an unregistered vlog directory before the 3s smoke defer expires.
	plantDir := filepath.Join(c.dataDirs[0], "fake-rogue-db")
	require.NoError(t, os.MkdirAll(plantDir, 0o755))
	plantFile := filepath.Join(plantDir, "000001.vlog")
	require.NoError(t, os.WriteFile(plantFile, []byte("rogue"), 0o644))

	// Either path is acceptable per spec: process exits non-zero, or incident
	// is recorded. Wait up to 12s (3s defer + buffer + drain).
	//
	// log.Fatal() in the child goroutine triggers os.Exit(1) but the harness
	// has not Wait()'d on the process yet — ProcessState stays nil and the
	// zombie still answers Signal(0). We probe by hitting the data-plane HTTP
	// server: when grainfs has exited, the listener is gone and Get returns
	// "connection refused" within milliseconds.
	deadline := time.Now().Add(12 * time.Second)
	exited := false
	incidentRaised := false
	probe := &http.Client{Timeout: 500 * time.Millisecond}
	for time.Now().Before(deadline) {
		resp, err := probe.Get(c.httpURLs[0] + "/metrics")
		if err != nil {
			exited = true
			break
		}
		_ = resp.Body.Close()
		incidents := fetchIncidentsSafe(t, c.httpURLs[0])
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
	require.True(t, exited || incidentRaised,
		"strict mode must fatally exit or emit registry_under_populated incident")
}


// fetchIncidentsSafe is fetchIncidents that returns nil instead of failing
// the test when the endpoint is briefly unreachable (e.g. process is exiting).
func fetchIncidentsSafe(t *testing.T, endpoint string) []incidentState {
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
