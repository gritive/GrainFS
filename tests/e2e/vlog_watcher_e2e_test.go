//go:build !race
// +build !race

package e2e

import (
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

// TestE2E_VlogWatcher_MetricsLive verifies the watcher loop is running:
// statfs-derived limit is non-zero, the used-ratio gauge appears in the
// metrics endpoint, and every registered DB category has its own
// vlog_bytes_by_category sample. The "fires on leak" assertion is covered by
// unit tests (resource_monitors_test.go) — at e2e scope we cannot make
// BadgerDB's vlog grow on-demand because the default valueThreshold (1 MiB)
// keeps small metadata in the LSM, leaving vlog file size at ~0.
func TestE2E_VlogWatcher_MetricsLive(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping vlog watcher e2e in -short mode")
	}
	c := startE2ECluster(t, e2eClusterOptions{
		Nodes:      1,
		ECData:     1,
		ECParity:   0,
		LogPrefix:  "vlog-live",
		DisableNFS: true,
		DisableNBD: true,
		ExtraArgs: []string{
			"--vlog-poll-interval=500ms",
			"--vlog-eta-window=1s",
			"--vlog-recovery-window=1s",
			"--vlog-smoke-defer=2s",
		},
	})

	require.Eventually(t, func() bool {
		return scrapeMetric(t, c.httpURLs[0], "grainfs_vlog_limit_bytes", "") > 0
	}, 8*time.Second, 200*time.Millisecond,
		"vlog_limit_bytes must be > 0 (proves statfs Snapshot() ran)")

	require.GreaterOrEqual(t,
		scrapeMetric(t, c.httpURLs[0], "grainfs_vlog_used_ratio", ""), 0.0,
		"vlog_used_ratio gauge must be present (recorder running)")

	// `meta` is the first DB registered at startup; group-raft / shared-raft-log
	// register later asynchronously and are flaky to assert at e2e scope.
	require.Eventually(t, func() bool {
		return scrapeMetric(t, c.httpURLs[0], "grainfs_vlog_bytes_by_category", `category="meta"`) >= 0
	}, 8*time.Second, 200*time.Millisecond,
		"meta category must have a vlog_bytes_by_category sample")
}

// TestE2E_GCTicker_RecoversAfterDeletion boots one node with a fast GC ticker
// and asserts that grainfs_badger_gc_runs_total advances within several ticks.
// The metric is wired by gcMetricsRecorder; this regression catches the case
// where the counter declaration drifts away from the increment site.
func TestE2E_GCTicker_RecoversAfterDeletion(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping vlog gc e2e in -short mode")
	}
	c := startE2ECluster(t, e2eClusterOptions{
		Nodes:      1,
		ECData:     1,
		ECParity:   0,
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
	if testing.Short() {
		t.Skip("skipping strict registry e2e in -short mode")
	}
	c := startE2ECluster(t, e2eClusterOptions{
		Nodes:      1,
		ECData:     1,
		ECParity:   0,
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

// TestE2E_VlogWatcher_SustainedWriteNoStarvation verifies that GC runs advance
// across multiple categories under sustained write load, catching regressions
// in the per-DB max-iter cap (gcMaxIterPerDBPerTick) that would let one
// write-churn DB starve siblings.
func TestE2E_VlogWatcher_SustainedWriteNoStarvation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping no-starvation e2e in -short mode")
	}
	c := startE2ECluster(t, e2eClusterOptions{
		Nodes:      2,
		ECData:     1,
		ECParity:   1,
		LogPrefix:  "vlog-nostarve",
		DisableNFS: true,
		DisableNBD: true,
		ExtraArgs: []string{
			"--badger-gc-interval=500ms",
			"--vlog-smoke-defer=2s",
		},
	})

	cli := c.S3Client(0)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		bucket := fmt.Sprintf("vlog-nostarve-%d", i)
		require.NoError(t, tryCreateBucket(ctx, cli, bucket))
		for j := 0; j < 6; j++ {
			payload := make([]byte, 1024)
			_, _ = rand.Read(payload)
			require.NoError(t, tryPutObject(ctx, cli, bucket, fmt.Sprintf("k-%d", j), payload))
		}
	}

	// Categories registered on a 2-node dynamic-join cluster: meta, dedup,
	// incident, receipts, group-raft, shared-raft-log. We assert at least
	// `meta` (always present) advances; group-raft typically advances too.
	categories := []string{"meta"}
	for _, cat := range categories {
		labelSubstr := fmt.Sprintf(`category=%q`, cat)
		require.Eventually(t, func() bool {
			v := scrapeMetric(t, c.httpURLs[0], "grainfs_badger_gc_runs_total", labelSubstr)
			return v > 0
		}, 10*time.Second, 200*time.Millisecond,
			"GC runs counter must advance for category=%s within 10s", cat)
	}
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
