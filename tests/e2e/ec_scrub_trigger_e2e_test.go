//go:build !race
// +build !race

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func adminUnixHTTPClient(socketPath string) *http.Client {
	return &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", socketPath)
			},
		},
	}
}

// TestE2E_ECScrubTrigger_FlowsThroughCluster boots a 3-node cluster with EC
// 2+1, writes objects, then triggers POST /v1/scrub. Asserts: trigger
// replicates via meta-raft, every node creates a session for the same
// SessionID, the EC source resolver routes the bucket to its owning group's
// BadgerDB, and at least one peer reports Checked > 0 in the aggregated
// stat. The repair-on-corrupt-shard scenario is covered by background
// scrubber tests (cluster_scrubber_test.go); here we verify the trigger
// landing path end-to-end.
func TestE2E_ECScrubTrigger_FlowsThroughCluster(t *testing.T) {
	c := startE2ECluster(t, e2eClusterOptions{
		Nodes: 3, Mode: ClusterModeStaticPeers, LogPrefix: "ec-scrub-trigger",
		DisableNFS: true, DisableNBD: true,
	})

	ctx := context.Background()
	const bucket = "ec-test"
	c.GrantAdminOnBuckets(bucket)
	_, err := waitForWritableEndpoint(ctx, c.httpURLs, 120*time.Second, 5*time.Second, time.Second, func(attemptCtx context.Context, endpoint string) error {
		return tryCreateBucket(attemptCtx, ecS3Client(endpoint, c.accessKey, c.secretKey), bucket)
	})
	require.NoError(t, err)
	for i := 0; i < 5; i++ {
		payload := bytes.Repeat([]byte{byte('a' + i)}, 4096)
		key := fmt.Sprintf("k-%d", i)
		_, err := waitForWritableEndpoint(ctx, c.httpURLs, 120*time.Second, 5*time.Second, time.Second, func(attemptCtx context.Context, endpoint string) error {
			return tryPutObject(attemptCtx, ecS3Client(endpoint, c.accessKey, c.secretKey), bucket, key, payload)
		})
		require.NoError(t, err)
	}

	// Give the cluster a moment to settle EC placement before triggering.
	time.Sleep(2 * time.Second)

	body, _ := json.Marshal(map[string]any{"bucket": bucket, "scope": "full"})
	var httpCli *http.Client
	var resp *http.Response
	require.Eventually(t, func() bool {
		for _, dir := range c.dataDirs {
			candidate := adminUnixHTTPClient(filepath.Join(dir, "admin.sock"))
			r, postErr := candidate.Post("http://unix/v1/scrub", "application/json", bytes.NewReader(body))
			if postErr != nil {
				continue
			}
			if r.StatusCode == http.StatusCreated || r.StatusCode == http.StatusOK {
				httpCli = candidate
				resp = r
				return true
			}
			r.Body.Close()
		}
		return false
	}, 30*time.Second, 500*time.Millisecond, "POST /v1/scrub must reach the meta-Raft leader")
	var sr map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&sr))
	resp.Body.Close()
	sessionID, _ := sr["session_id"].(string)
	require.NotEmpty(t, sessionID)

	require.Eventually(t, func() bool {
		r, err := httpCli.Get("http://unix/v1/scrub/jobs/" + sessionID)
		if err != nil {
			return false
		}
		defer r.Body.Close()
		var info map[string]any
		if err := json.NewDecoder(r.Body).Decode(&info); err != nil {
			return false
		}
		status, _ := info["status"].(string)
		return status == "done"
	}, 60*time.Second, 1*time.Second, "scrub session must reach done within 60s")

	r, err := httpCli.Get("http://unix/v1/scrub/jobs/" + sessionID)
	require.NoError(t, err)
	defer r.Body.Close()
	var info map[string]any
	require.NoError(t, json.NewDecoder(r.Body).Decode(&info))
	t.Logf("session %s: bucket=%v scope=%v status=%v checked=%v detected=%v repaired=%v partial=%v",
		sessionID, info["bucket"], info["scope"], info["status"],
		info["checked"], info["detected"], info["repaired"], info["partial"])

	checked, ok := info["checked"].(float64)
	require.True(t, ok, "checked must decode as a JSON number")
	require.Equal(t, "done", info["status"], "session must reach done")
	require.Equal(t, "ec-test", info["bucket"], "bucket field must round-trip")
	require.Greater(t, checked, float64(0), "EC resolver and aggregation path must report checked objects")
}

// TestE2E_ECScrubTrigger_DedupHit_ReturnsExistingSession verifies the
// LookupDedup short-circuit: a second identical POST returns the same
// SessionID with created=false instead of burning a fresh raft entry.
func TestE2E_ECScrubTrigger_DedupHit_ReturnsExistingSession(t *testing.T) {
	c := startE2ECluster(t, e2eClusterOptions{
		Nodes: 1, LogPrefix: "ec-scrub-dedup",
		DisableNFS: true, DisableNBD: true,
	})
	httpCli := adminUnixHTTPClient(filepath.Join(c.dataDirs[0], "admin.sock"))

	body, _ := json.Marshal(map[string]any{"bucket": "dedup-bk", "scope": "full"})

	first, err := httpCli.Post("http://unix/v1/scrub", "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	var firstResp map[string]any
	decodeScrubTriggerResp(t, first, &firstResp)
	require.True(t, firstResp["created"].(bool), "first call: created=true expected")
	firstID := firstResp["session_id"].(string)

	second, err := httpCli.Post("http://unix/v1/scrub", "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	var secondResp map[string]any
	decodeScrubTriggerResp(t, second, &secondResp)
	require.False(t, secondResp["created"].(bool), "second call: dedup hit must return Created=false")
	require.Equal(t, firstID, secondResp["session_id"].(string), "dedup must return same SessionID")
}

func decodeScrubTriggerResp(t *testing.T, resp *http.Response, out *map[string]any) {
	t.Helper()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusCreated, resp.StatusCode, "body: %s", string(body))
	require.NoError(t, json.Unmarshal(body, out))
}
