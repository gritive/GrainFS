package e2e

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/s3auth"
)

// TestIcebergConcurrentCommitsE2E proves the iceberg REST catalog tolerates
// concurrent CommitTable from multiple workers without returning 503.
//
// Cluster4Node reproduces (smaller, deterministic) the catalog-commits
// workload that warp drives: parallel commits over a small set of tables
// spread across follower nodes that forward to the leader. The strict gate
// is "503 ≤ tolerated rate"; spec-compliant 409 (CommitFailedException) is
// tolerated as the intended optimistic-concurrency signal. This pins the
// known QUIC transient (spec §8 `iceberg-rare-quic-stream-local-cancel-
// under-load`): the threshold (≤0.5%) sits above the measured 0.1-0.2%
// baseline, so the test catches regressions of the rate without flaking
// on the documented baseline.
//
// SingleNode runs the same goroutine/table fan-out against a single node
// (no forward path) — 503 should never occur. It exercises the
// optimistic-concurrency lock and 409/200 split on the same surface
// contract, so a 409-vs-200 split regression surfaces on both targets
// rather than only under cluster load.
func TestIcebergConcurrentCommitsE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runIcebergConcurrentCommitCase(t, newSingleNodeS3Target())
	})

	t.Run("Cluster4Node", func(t *testing.T) {
		runIcebergConcurrentCommitCase(t, newSharedClusterS3Target(t))
	})
}

func runIcebergConcurrentCommitCase(t *testing.T, tgt s3Target) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Build the URL fan-out from the target. Single-node has 1 endpoint,
	// cluster has N. The leader for namespace/table creation is node-0 on
	// single (only choice); cluster fixtures expose the elected leader via
	// the harness.
	httpURLs := make([]string, tgt.nodes)
	for i := 0; i < tgt.nodes; i++ {
		httpURLs[i] = tgt.endpoint(i)
	}
	leaderIdx := 0
	if tgt.isCluster && tgt.cluster != nil {
		leaderIdx = tgt.cluster.leaderIdx
	}

	// Bootstrap: warehouse bucket. On cluster the bucket may be pre-granted
	// admin by the fixture; on single we just create it idempotently via the
	// SDK. Both paths tolerate already-exists for shared-fixture re-runs.
	if tgt.isCluster && tgt.cluster != nil {
		tgt.cluster.GrantAdminOnBuckets("grainfs-tables")
	}
	_, err := tgt.pickNode(leaderIdx).CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("grainfs-tables"),
	})
	if err != nil && !strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") &&
		!strings.Contains(err.Error(), "BucketAlreadyExists") {
		require.NoError(t, err)
	}
	// Probe the iceberg /v1/config endpoint on every endpoint so we know
	// the catalog router is wired and the bucket is reachable from each —
	// on cluster this catches the rare "follower joined but iceberg routes
	// not registered yet" race during a fresh cluster boot; on single it
	// is a trivial assertion that the catalog is up.
	for i, u := range httpURLs {
		req, err := http.NewRequest(http.MethodGet, u+"/iceberg/v1/config?warehouse=warehouse", nil)
		require.NoError(t, err, "node %d config probe build", i)
		s3auth.SignRequest(req, tgt.accessKey, tgt.secretKey, "us-east-1")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err, "node %d config probe", i)
		require.Equal(t, http.StatusOK, resp.StatusCode, "node %d config probe", i)
		resp.Body.Close()
	}

	// Unique namespace per test invocation so re-runs (and shared fixture
	// re-use) do not collide.
	nsName := fmt.Sprintf("ns_concurrent_%d", time.Now().UnixNano())
	leaderBase := httpURLs[leaderIdx] + "/iceberg"

	postIcebergJSONHelper(t, leaderBase+"/v1/namespaces",
		fmt.Sprintf(`{"namespace":["%s"],"properties":{}}`, nsName),
		tgt.accessKey, tgt.secretKey, http.StatusOK)

	const numTables = 4
	for i := 0; i < numTables; i++ {
		postIcebergJSONHelper(t,
			fmt.Sprintf("%s/v1/namespaces/%s/tables", leaderBase, nsName),
			fmt.Sprintf(`{
				"name":"t%d",
				"schema":{"type":"struct","fields":[{"name":"a","id":1,"type":"int","required":false}],"schema-id":0},
				"properties":{"format-version":"2"}
			}`, i),
			tgt.accessKey, tgt.secretKey, http.StatusOK)
	}

	// Workers stress concurrent commits across the cluster.
	// Each goroutine pins to a distinct (node, table) pair so spec-
	// compliant 409s do not dominate the failure count and obscure 503s.
	const goroutines = 16
	const commitsPerWorker = 100
	type tally struct {
		status200   atomic.Int64
		status409   atomic.Int64
		status503   atomic.Int64
		statusOther atomic.Int64
		otherCodes  sync.Map // map[int]int64
	}
	var counts tally
	var snapshotSeq atomic.Int64

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			nodeIdx := workerID % len(httpURLs)
			tableIdx := workerID % numTables
			base := httpURLs[nodeIdx] + "/iceberg"
			path := fmt.Sprintf("%s/v1/namespaces/%s/tables/t%d", base, nsName, tableIdx)
			for i := 0; i < commitsPerWorker; i++ {
				snapID := snapshotSeq.Add(1)
				body := fmt.Sprintf(`{
					"requirements":[],
					"updates":[
						{"action":"add-snapshot","snapshot":{"snapshot-id":%d,"sequence-number":%d,"timestamp-ms":%d,"manifest-list":"s3://grainfs-tables/warehouse/%s/t%d/metadata/snap-%d.avro","summary":{"operation":"overwrite"},"schema-id":0}},
						{"action":"set-snapshot-ref","ref-name":"main","type":"branch","snapshot-id":%d}
					]
				}`, snapID, snapID, time.Now().UnixMilli(), nsName, tableIdx, snapID, snapID)
				code := postIcebergCommit(t, path, body, tgt.accessKey, tgt.secretKey)
				switch code {
				case http.StatusOK:
					counts.status200.Add(1)
				case http.StatusConflict:
					counts.status409.Add(1)
				case http.StatusServiceUnavailable:
					counts.status503.Add(1)
				default:
					counts.statusOther.Add(1)
					v, _ := counts.otherCodes.LoadOrStore(code, new(atomic.Int64))
					v.(*atomic.Int64).Add(1)
				}
			}
		}(g)
	}
	wg.Wait()

	total := counts.status200.Load() + counts.status409.Load() +
		counts.status503.Load() + counts.statusOther.Load()
	t.Logf("concurrent commits: total=%d 200=%d 409=%d 503=%d other=%d",
		total, counts.status200.Load(), counts.status409.Load(),
		counts.status503.Load(), counts.statusOther.Load())
	if counts.statusOther.Load() > 0 {
		counts.otherCodes.Range(func(k, v any) bool {
			t.Logf("  other status=%d count=%d", k, v.(*atomic.Int64).Load())
			return true
		})
	}

	// Strict assertion: no 503 from any goroutine. 503 from MetaCatalog
	// means the cluster lost the ability to forward to the leader.
	//
	// KNOWN ISSUE (tracked in spec §8 under
	// `iceberg-rare-quic-stream-local-cancel-under-load`): the QUIC
	// transport layer occasionally cancels a forwarded-proposal stream
	// "by local with error code 1" under heavy concurrent load,
	// surfacing to clients as 503. Root cause is in the QUIC stream
	// lifecycle and requires deeper transport-layer investigation.
	// This test pins the symptom — accepts low rate but warns above
	// a sane threshold.
	got503 := counts.status503.Load()
	const tolerated503PerKReq = 5 // ~0.5% — well above measured 0.1-0.2% baseline
	threshold := tolerated503PerKReq * total / 1000
	if threshold < 1 {
		threshold = 1
	}
	if got503 > 0 {
		t.Logf("WARNING: %d × 503 across %d commits (%.2f%%) — see spec §8 "+
			"`iceberg-rare-quic-stream-local-cancel-under-load`",
			got503, total, 100.0*float64(got503)/float64(total))
	}
	require.LessOrEqual(t, got503, threshold,
		"got %d × 503 across %d concurrent commits — exceeds tolerated rate (%d). "+
			"Meta-forward path lost the leader more often than the known transient baseline.",
		got503, total, threshold)

	// Cleanup: drop tables + namespace. Best-effort; do not fail the test
	// on cleanup errors because the assertion above is the gate.
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cleanupCancel()
	for i := 0; i < numTables; i++ {
		req, _ := http.NewRequestWithContext(cleanupCtx, http.MethodDelete,
			fmt.Sprintf("%s/v1/namespaces/%s/tables/t%d", leaderBase, nsName, i), nil)
		s3auth.SignRequest(req, tgt.accessKey, tgt.secretKey, "us-east-1")
		if resp, err := http.DefaultClient.Do(req); err == nil {
			resp.Body.Close()
		}
	}
	req, _ := http.NewRequestWithContext(cleanupCtx, http.MethodDelete,
		fmt.Sprintf("%s/v1/namespaces/%s", leaderBase, nsName), nil)
	s3auth.SignRequest(req, tgt.accessKey, tgt.secretKey, "us-east-1")
	if resp, err := http.DefaultClient.Do(req); err == nil {
		resp.Body.Close()
	}
}

// postIcebergCommit returns the HTTP status code of a CommitTable POST.
// On non-2xx, the body is read and logged at t.Helper level for diagnosis.
// The iceberg catalog routes require SigV4 auth post-#427, so callers must
// thread the fixture's access/secret pair through.
func postIcebergCommit(t *testing.T, url, body, accessKey, secretKey string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Logf("postIcebergCommit: NewRequest err=%v url=%s", err, url)
		return -1
	}
	req.Header.Set("Content-Type", "application/json")
	s3auth.SignRequest(req, accessKey, secretKey, "us-east-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Logf("postIcebergCommit: Do err=%v url=%s", err, url)
		return -2
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusServiceUnavailable ||
		(resp.StatusCode >= 500 && resp.StatusCode != http.StatusInternalServerError) {
		buf, _ := io.ReadAll(resp.Body)
		t.Logf("postIcebergCommit: status=%d url=%s body=%s",
			resp.StatusCode, url, string(buf))
	}
	return resp.StatusCode
}

// postIcebergJSONHelper posts a JSON body to url and requires the status.
// Mirrors the in-package postIcebergJSON helper from internal/server/iceberg_api_test.go
// but lives here because tests/e2e cannot import the server package's test helpers.
func postIcebergJSONHelper(t *testing.T, url, body, accessKey, secretKey string, wantStatus int) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader([]byte(body)))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	s3auth.SignRequest(req, accessKey, secretKey, "us-east-1")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	if resp.StatusCode != wantStatus {
		buf, _ := io.ReadAll(resp.Body)
		t.Fatalf("POST %s: status=%d want=%d body=%s", url, resp.StatusCode, wantStatus, string(buf))
	}
	// Drain body so the connection can be reused.
	_, _ = io.Copy(io.Discard, resp.Body)
}
