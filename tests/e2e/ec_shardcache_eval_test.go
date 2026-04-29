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
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

// TestE2E_ECShardCacheEval is the multi-node baseline that decides whether
// to add a real EC shard cache. We spin up a 3-node cluster (the smallest
// size where Phase 18 Cluster EC activates), enable --measure-read-amp on
// every node, run several access patterns hand-picked to expose shard-
// level locality, and read each node's /metrics to compute the simulator
// hit rate per cache size (16 / 64 / 256 MB equivalent).
//
// The hit-rate curve answers the question we cannot answer from a single
// node: when CachedBackend is bypassed (large object) or evicted, does
// the same shard get re-requested often enough that an in-memory shard
// cache would catch the duplicate fetches.
//
//	flat low across all sizes  → no shard locality, EC shard cache has
//	                              no production benefit; defer.
//	rising with size           → working set fits in a reachable budget;
//	                              build the cache, size it from the curve.
//	saturated even at 16 MB    → every workload a tiny cache catches;
//	                              good return on a small implementation.
//
// Workloads (each measured with a fresh tracker reset per node):
//
//   - large_repeat: one 16 MB object, GET it 10 times. CachedBackend's
//     4 MB-per-object limit forces every GET to bypass cache and re-run
//     getObjectEC, which reads K shards each time. If the shard cache
//     would help anywhere, it is here.
//
//   - small_repeat: one 1 MB object, GET it 10 times. CachedBackend
//     should absorb after the first GET, so the simulator should NOT
//     see repeats. Validates we are not double-counting.
//
//   - many_unique: 20 unique 8 MB objects, GET each once. Worst case:
//     all cold. Simulator must report 0% hit at every cache size.
func TestE2E_ECShardCacheEval(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node EC measurement is too slow for -short mode")
	}
	binary := getBinary()
	if _, err := os.Stat(binary); err != nil {
		t.Skipf("grainfs binary not found at %s — run `make build` first", binary)
	}

	const (
		clusterKey = "E2E-EC-SHARDCACHE-EVAL"
		accessKey  = "ec-eval-ak"
		secretKey  = "ec-eval-sk"
		bucketName = "ec-shardcache-eval"
		numNodes   = 3
		ecData     = 2
		ecParity   = 1
	)

	httpPorts := make([]int, numNodes)
	raftPorts := make([]int, numNodes)
	for i := range httpPorts {
		httpPorts[i] = freePort()
		raftPorts[i] = freePort()
	}
	raftAddr := func(i int) string { return fmt.Sprintf("127.0.0.1:%d", raftPorts[i]) }
	httpURL := func(i int) string { return fmt.Sprintf("http://127.0.0.1:%d", httpPorts[i]) }
	peersFor := func(i int) string {
		var out []string
		for j := range raftPorts {
			if j == i {
				continue
			}
			out = append(out, raftAddr(j))
		}
		return strings.Join(out, ",")
	}

	dataDirs := make([]string, numNodes)
	for i := range dataDirs {
		d, err := os.MkdirTemp("", fmt.Sprintf("grainfs-ec-shcache-%d-*", i))
		require.NoError(t, err)
		dataDirs[i] = d
		t.Cleanup(func() { _ = os.RemoveAll(d) })
	}

	startNode := func(i int) *exec.Cmd {
		cmd := exec.Command(binary, "serve",
			"--data", dataDirs[i],
			"--port", fmt.Sprintf("%d", httpPorts[i]),
			"--node-id", fmt.Sprintf("ec-cache-eval-%d", i),
			"--raft-addr", raftAddr(i),
			"--peers", peersFor(i),
			"--cluster-key", clusterKey,
			"--access-key", accessKey,
			"--secret-key", secretKey,
			fmt.Sprintf("--ec-data=%d", ecData),
			fmt.Sprintf("--ec-parity=%d", ecParity),
			"--measure-read-amp", // ← the whole point of this test
			"--block-cache-size=0",
			"--shard-cache-size=0", // simulator-only baseline; real cache off
			"--nfs4-port", "0",
			"--nbd-port", "0",
			"--snapshot-interval", "0",
			"--scrub-interval", "0",
			"--lifecycle-interval", "0",
			"--no-encryption",
		)
		require.NoError(t, cmd.Start(), "start node %d", i)
		return cmd
	}

	procs := make([]*exec.Cmd, numNodes)
	t.Cleanup(func() {
		for _, p := range procs {
			if p != nil && p.Process != nil {
				_ = p.Process.Kill()
				_, _ = p.Process.Wait()
			}
		}
	})
	for i := 0; i < numNodes; i++ {
		procs[i] = startNode(i)
	}
	for i := 0; i < numNodes; i++ {
		waitForPort(t, httpPorts[i], 60*time.Second)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	var client *s3.Client
	var leaderURL string
	require.Eventually(t, func() bool {
		for i := 0; i < numNodes; i++ {
			c := ecS3Client(httpURL(i), accessKey, secretKey)
			_, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketName)})
			if err == nil {
				client = c
				leaderURL = httpURL(i)
				return true
			}
		}
		return false
	}, 120*time.Second, 2*time.Second, "no leader found")
	t.Logf("leader: %s", leaderURL)

	// One 16 MB object — bypasses CachedBackend (4 MB per-obj cap).
	largeKey := "large-16mb"
	largeData := make([]byte, 16*1024*1024)
	if _, err := rand.Read(largeData); err != nil {
		t.Fatalf("rand: %v", err)
	}
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(largeKey),
		Body:   bytes.NewReader(largeData),
	}); err != nil {
		t.Fatalf("put large: %v", err)
	}

	// One 1 MB object — fits CachedBackend, repeated GETs should NOT
	// reach getObjectEC after the first miss.
	smallKey := "small-1mb"
	smallData := make([]byte, 1024*1024)
	if _, err := rand.Read(smallData); err != nil {
		t.Fatalf("rand: %v", err)
	}
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(smallKey),
		Body:   bytes.NewReader(smallData),
	}); err != nil {
		t.Fatalf("put small: %v", err)
	}

	// 20 unique 8 MB objects — drains the cache every time.
	uniqueKeys := make([]string, 20)
	uniqueData := make([]byte, 8*1024*1024)
	if _, err := rand.Read(uniqueData); err != nil {
		t.Fatalf("rand: %v", err)
	}
	for i := range uniqueKeys {
		uniqueKeys[i] = fmt.Sprintf("uniq-%02d-8mb", i)
		if _, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(uniqueKeys[i]),
			Body:   bytes.NewReader(uniqueData),
		}); err != nil {
			t.Fatalf("put %s: %v", uniqueKeys[i], err)
		}
	}

	// We measure each workload independently against a fresh baseline
	// snapshot. Counters are per-process; each node holds its own.
	type nodeBaseline struct {
		hits, misses [3]uint64 // [16MB, 64MB, 256MB]
	}
	urls := make([]string, numNodes)
	for i := range urls {
		urls[i] = httpURL(i)
	}

	getOnce := func(t *testing.T, key string) {
		t.Helper()
		out, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
		})
		require.NoError(t, err, "GET %s", key)
		_, _ = io.Copy(io.Discard, out.Body)
		_ = out.Body.Close()
	}

	// scrape returns (hits, misses) per simulator size for one node.
	scrape := func(t *testing.T, url string) [3][2]uint64 {
		t.Helper()
		resp, err := http.Get(url + "/metrics")
		require.NoError(t, err, "scrape %s", url)
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		text := string(body)
		read := func(metric, tracker string) uint64 {
			pattern := fmt.Sprintf(`grainfs_readamp_%s_total\{tracker="%s"\}\s+(\S+)`, metric, tracker)
			re := regexp.MustCompile(pattern)
			m := re.FindStringSubmatch(text)
			if len(m) < 2 {
				return 0
			}
			f, _ := strconv.ParseFloat(m[1], 64)
			return uint64(f)
		}
		var out [3][2]uint64
		sizes := []string{"ec_shard_16mb", "ec_shard_64mb", "ec_shard_256mb"}
		for i, s := range sizes {
			out[i][0] = read("hits", s)
			out[i][1] = read("misses", s)
		}
		return out
	}
	snapshotAll := func(t *testing.T) []nodeBaseline {
		out := make([]nodeBaseline, numNodes)
		for i := 0; i < numNodes; i++ {
			r := scrape(t, urls[i])
			for j := 0; j < 3; j++ {
				out[i].hits[j] = r[j][0]
				out[i].misses[j] = r[j][1]
			}
		}
		return out
	}
	report := func(t *testing.T, label string, before []nodeBaseline) {
		t.Helper()
		after := snapshotAll(t)
		// Sum deltas across all nodes — simulator records per-process,
		// and EC shards land on different nodes, so cluster-wide hit
		// rate is what we actually want.
		var totalHits, totalMisses [3]uint64
		for n := 0; n < numNodes; n++ {
			for j := 0; j < 3; j++ {
				if after[n].hits[j] >= before[n].hits[j] {
					totalHits[j] += after[n].hits[j] - before[n].hits[j]
				}
				if after[n].misses[j] >= before[n].misses[j] {
					totalMisses[j] += after[n].misses[j] - before[n].misses[j]
				}
			}
		}
		labels := []string{"16MB", "64MB", "256MB"}
		for j := 0; j < 3; j++ {
			total := totalHits[j] + totalMisses[j]
			rate := 0.0
			if total > 0 {
				rate = 100 * float64(totalHits[j]) / float64(total)
			}
			t.Logf("%-30s %5s: %5.1f%% hit (cluster total: %d hit / %d miss)",
				label, labels[j], rate, totalHits[j], totalMisses[j])
		}
	}

	// Workload A: large object, repeated GET.
	t.Run("large_repeat_16mb_x10", func(t *testing.T) {
		base := snapshotAll(t)
		for i := 0; i < 10; i++ {
			getOnce(t, largeKey)
		}
		report(t, "large_16mb × 10 GETs", base)
	})

	// Workload B: small object, repeated GET — CachedBackend absorbs.
	t.Run("small_repeat_1mb_x10", func(t *testing.T) {
		base := snapshotAll(t)
		for i := 0; i < 10; i++ {
			getOnce(t, smallKey)
		}
		report(t, "small_1mb × 10 GETs", base)
	})

	// Workload C: many unique large objects — no recurrence.
	t.Run("many_unique_8mb_x20", func(t *testing.T) {
		base := snapshotAll(t)
		for _, k := range uniqueKeys {
			getOnce(t, k)
		}
		report(t, "20 unique 8mb GETs", base)
	})
}

// TestE2E_ECShardCacheActive turns the real EC shard cache ON and
// verifies the production cache delivers the hit rate the simulator
// predicted. Companion to TestE2E_ECShardCacheEval, which measures the
// same access patterns with the cache OFF.
//
// Workload: 16 MB object, repeated GET ×10. CachedBackend bypasses the
// object (4 MB-per-object cap) so every iteration goes through
// getObjectEC. The first GET fetches K shards and populates the cache;
// the next 9 should hit on the per-shard pre-pass and short-circuit
// reconstruction without any ReadShard / ReadLocalShard call.
//
// Expectation: ≥80% real cache hit rate cluster-wide. The 90% number
// the simulator measured on PR #71 is the upper bound; we leave 10%
// margin for cold misses and cross-node placement details.
func TestE2E_ECShardCacheActive(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node EC active-cache test is too slow for -short mode")
	}
	binary := getBinary()
	if _, err := os.Stat(binary); err != nil {
		t.Skipf("grainfs binary not found at %s — run `make build` first", binary)
	}

	const (
		clusterKey = "E2E-EC-SHARDCACHE-ACTIVE"
		accessKey  = "ec-active-ak"
		secretKey  = "ec-active-sk"
		bucketName = "ec-shardcache-active"
		numNodes   = 3
		ecData     = 2
		ecParity   = 1
	)

	httpPorts := make([]int, numNodes)
	raftPorts := make([]int, numNodes)
	for i := range httpPorts {
		httpPorts[i] = freePort()
		raftPorts[i] = freePort()
	}
	raftAddr := func(i int) string { return fmt.Sprintf("127.0.0.1:%d", raftPorts[i]) }
	httpURL := func(i int) string { return fmt.Sprintf("http://127.0.0.1:%d", httpPorts[i]) }
	peersFor := func(i int) string {
		var out []string
		for j := range raftPorts {
			if j == i {
				continue
			}
			out = append(out, raftAddr(j))
		}
		return strings.Join(out, ",")
	}

	dataDirs := make([]string, numNodes)
	for i := range dataDirs {
		d, err := os.MkdirTemp("", fmt.Sprintf("grainfs-ec-shcache-active-%d-*", i))
		require.NoError(t, err)
		dataDirs[i] = d
		t.Cleanup(func() { _ = os.RemoveAll(d) })
	}

	startNode := func(i int) *exec.Cmd {
		cmd := exec.Command(binary, "serve",
			"--data", dataDirs[i],
			"--port", fmt.Sprintf("%d", httpPorts[i]),
			"--node-id", fmt.Sprintf("ec-cache-active-%d", i),
			"--raft-addr", raftAddr(i),
			"--peers", peersFor(i),
			"--cluster-key", clusterKey,
			"--access-key", accessKey,
			"--secret-key", secretKey,
			fmt.Sprintf("--ec-data=%d", ecData),
			fmt.Sprintf("--ec-parity=%d", ecParity),
			"--block-cache-size=0", // isolate: only EC shard cache active
			// 256 MB total → 16 MB per-shard budget. Each EC shard for
			// a 16 MB object at k=2 is ≈8 MB, which exceeds a 4 MB slot
			// (= 64 MB / 16 shards) and silently drops at Put. 256 MB is
			// the production default precisely because real EC shards
			// are MB-sized, not KB-sized like volume blocks.
			"--shard-cache-size=268435456",
			"--nfs4-port", "0",
			"--nbd-port", "0",
			"--snapshot-interval", "0",
			"--scrub-interval", "0",
			"--lifecycle-interval", "0",
			"--no-encryption",
		)
		require.NoError(t, cmd.Start(), "start node %d", i)
		return cmd
	}

	procs := make([]*exec.Cmd, numNodes)
	t.Cleanup(func() {
		for _, p := range procs {
			if p != nil && p.Process != nil {
				_ = p.Process.Kill()
				_, _ = p.Process.Wait()
			}
		}
	})
	for i := 0; i < numNodes; i++ {
		procs[i] = startNode(i)
	}
	for i := 0; i < numNodes; i++ {
		waitForPort(t, httpPorts[i], 60*time.Second)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	var client *s3.Client
	var leaderURL string
	require.Eventually(t, func() bool {
		for i := 0; i < numNodes; i++ {
			c := ecS3Client(httpURL(i), accessKey, secretKey)
			_, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketName)})
			if err == nil {
				client = c
				leaderURL = httpURL(i)
				return true
			}
		}
		return false
	}, 120*time.Second, 2*time.Second, "no leader found")
	t.Logf("leader: %s", leaderURL)

	largeKey := "large-16mb"
	largeData := make([]byte, 16*1024*1024)
	if _, err := rand.Read(largeData); err != nil {
		t.Fatalf("rand: %v", err)
	}
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(largeKey),
		Body:   bytes.NewReader(largeData),
	}); err != nil {
		t.Fatalf("put large: %v", err)
	}

	// Repeated GET ×10 to drive cache hits.
	for i := 0; i < 10; i++ {
		out, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(largeKey),
		})
		require.NoError(t, err, "GET iteration %d", i)
		_, _ = io.Copy(io.Discard, out.Body)
		_ = out.Body.Close()
	}

	// Sum cluster-wide cache stats. Shards land on different nodes, so
	// per-process numbers each capture a slice of the work.
	type cacheStatus struct {
		ShardCache struct {
			Enabled       bool    `json:"enabled"`
			Hits          uint64  `json:"hits"`
			Misses        uint64  `json:"misses"`
			ResidentBytes int64   `json:"resident_bytes"`
			HitRatePct    float64 `json:"hit_rate_pct"`
		} `json:"shard_cache"`
	}
	var totalHits, totalMisses uint64
	for i := 0; i < numNodes; i++ {
		resp, err := http.Get(httpURL(i) + "/api/cache/status")
		require.NoError(t, err, "fetch cache status from node %d", i)
		var st cacheStatus
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&st))
		_ = resp.Body.Close()
		require.True(t, st.ShardCache.Enabled, "node %d shard_cache must be enabled", i)
		t.Logf("node %d: hits=%d misses=%d resident=%d hit_rate=%.1f%%",
			i, st.ShardCache.Hits, st.ShardCache.Misses, st.ShardCache.ResidentBytes, st.ShardCache.HitRatePct)
		totalHits += st.ShardCache.Hits
		totalMisses += st.ShardCache.Misses
	}
	total := totalHits + totalMisses
	require.Greater(t, total, uint64(0), "shard cache recorded zero accesses — wiring broken")
	hitRate := 100 * float64(totalHits) / float64(total)
	t.Logf("cluster-wide shard cache: %d hits / %d misses → %.1f%% hit rate", totalHits, totalMisses, hitRate)

	// First GET is necessarily a miss for every shard; the next 9 should
	// be served from cache. With k=2, m=1 we issue 3 read intents per
	// GET (2 needed for reconstruction). 10 GETs × 3 shards = 30 lookups
	// per cache. Best case ≈90% (3 misses out of 30); we accept ≥80%.
	if hitRate < 80 {
		t.Fatalf("real shard cache hit rate %.1f%% is below the 80%% floor — getObjectEC is not consulting the cache as expected", hitRate)
	}
}
