package e2e

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRestartRecovery_SweepsOrphanArtifacts verifies the Phase 16 Week 3
// startup recovery path:
//
//  1. Plant orphan .tmp + parts/<uploadID>/ artifacts in a data dir.
//  2. Boot grainfs against that dir.
//  3. Confirm both artifacts are gone post-boot.
//  4. Confirm startup-phase HealEvents were persisted to the eventstore so
//     a dashboard reload after boot can still show what was cleaned.
//
// We query the eventstore (/api/eventlog?type=heal) instead of subscribing
// to the SSE stream because SSE clients can only join AFTER boot, while
// startup HealEvents fire DURING boot — the persisted store is the only
// source-of-truth a freshly-loaded dashboard can read.
func TestRestartRecovery_SweepsOrphanArtifacts(t *testing.T) {
	binary := getBinary()
	dir, err := os.MkdirTemp("", "grainfs-restart-recovery-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// Plant a stale .tmp file (backdated past the 5-min in-flight guard).
	staleTmp := filepath.Join(dir, "shards", "b", "k", "0.tmp")
	require.NoError(t, os.MkdirAll(filepath.Dir(staleTmp), 0o755))
	require.NoError(t, os.WriteFile(staleTmp, []byte("partial"), 0o644))
	past := time.Now().Add(-30 * time.Minute)
	require.NoError(t, os.Chtimes(staleTmp, past, past))

	// Plant an abandoned multipart upload (older than 24h).
	staleUpload := filepath.Join(dir, "parts", "abandoned-id")
	require.NoError(t, os.MkdirAll(staleUpload, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(staleUpload, "00001"), []byte("p"), 0o644))
	veryPast := time.Now().Add(-25 * time.Hour)
	require.NoError(t, os.Chtimes(staleUpload, veryPast, veryPast))

	port := freePort()
	cmd := exec.Command(binary, "serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", port),
		"--nfs-port", "0",
		"--nfs4-port", "0",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	defer func() { _ = cmd.Process.Kill() }()

	waitForPort(port, 5*time.Second)

	// Sanity: orphan artifacts must be gone now that startup recovery ran.
	_, statErr := os.Stat(staleTmp)
	assert.True(t, os.IsNotExist(statErr), "stale .tmp should have been removed by startup recovery")
	_, statErr = os.Stat(staleUpload)
	assert.True(t, os.IsNotExist(statErr), "abandoned multipart dir should have been removed")

	// Eventstore writes are async (bounded queue + worker) so retry briefly.
	endpoint := fmt.Sprintf("http://127.0.0.1:%d", port)
	var (
		healEvents     []map[string]any
		fetchErr       error
		hasOrphanTmp   bool
		hasOrphanMulti bool
	)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		healEvents, fetchErr = fetchHealEvents(endpoint)
		if fetchErr == nil {
			hasOrphanTmp, hasOrphanMulti = scanForRestartCleanups(healEvents)
			if hasOrphanTmp && hasOrphanMulti {
				break
			}
		}
		time.Sleep(150 * time.Millisecond)
	}

	require.NoError(t, fetchErr)
	assert.True(t, hasOrphanTmp, "expected an orphan_tmp startup HealEvent in eventstore, got: %v", healEvents)
	assert.True(t, hasOrphanMulti, "expected an orphan_multipart startup HealEvent in eventstore, got: %v", healEvents)
}

func fetchHealEvents(endpoint string) ([]map[string]any, error) {
	resp, err := http.Get(endpoint + "/api/eventlog?type=heal&since=3600&limit=200")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("eventlog status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var events []map[string]any
	if err := json.Unmarshal(body, &events); err != nil {
		return nil, err
	}
	return events, nil
}

func scanForRestartCleanups(events []map[string]any) (orphanTmp, orphanMulti bool) {
	for _, e := range events {
		meta, _ := e["metadata"].(map[string]any)
		if meta == nil {
			continue
		}
		if meta["phase"] != "startup" {
			continue
		}
		switch meta["err_code"] {
		case "orphan_tmp":
			orphanTmp = true
		case "orphan_multipart":
			orphanMulti = true
		}
	}
	return
}
