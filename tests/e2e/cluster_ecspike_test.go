package e2e

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gritive/GrainFS/internal/cluster/ecspike"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ecspikeNode is one of the 6 grainfs local processes used by the spike.
type ecspikeNode struct {
	port     int
	endpoint string
	dataDir  string
	cmd      *exec.Cmd
}

func (n *ecspikeNode) kill() {
	if n.cmd != nil && n.cmd.Process != nil {
		_ = n.cmd.Process.Kill()
		_, _ = n.cmd.Process.Wait()
	}
}

// startEcspikeCluster spawns 6 independent grainfs serve processes in local mode
// on loopback. Returns the node list and a cleanup func. Uses existing test
// harness helpers (freePort, waitForPort, newS3Client).
func startEcspikeCluster(t *testing.T) ([]*ecspikeNode, func()) {
	return startEcspikeClusterOpts(t, false)
}

// startEcspikeClusterNoEC spawns 6 nodes with --ec=false --no-encryption to measure
// raw shard p95 without nested solo EC overhead (CONDITIONAL GO prerequisite).
func startEcspikeClusterNoEC(t *testing.T) ([]*ecspikeNode, func()) {
	return startEcspikeClusterOpts(t, true)
}

func startEcspikeClusterOpts(t *testing.T, noEC bool) ([]*ecspikeNode, func()) {
	t.Helper()
	binary := getBinary()

	const n = 6
	nodes := make([]*ecspikeNode, n)

	cleanup := func() {
		for _, node := range nodes {
			if node != nil {
				node.kill()
				_ = os.RemoveAll(node.dataDir)
			}
		}
	}

	for i := 0; i < n; i++ {
		dir, err := os.MkdirTemp("", fmt.Sprintf("ecspike-node%d-*", i))
		if err != nil {
			cleanup()
			require.NoErrorf(t, err, "mkdtemp node %d", i)
		}
		port := freePort()
		args := []string{
			"serve",
			"--data", dir,
			"--port", fmt.Sprintf("%d", port),
			"--nfs-port", "0",
			"--nfs4-port", "0",
			"--nbd-port", "0",
		}
		if noEC {
			args = append(args, "--ec=false", "--no-encryption")
		}
		cmd := exec.Command(binary, args...)
		if err := cmd.Start(); err != nil {
			cleanup()
			require.NoErrorf(t, err, "start node %d", i)
		}
		nodes[i] = &ecspikeNode{
			port:     port,
			endpoint: fmt.Sprintf("http://127.0.0.1:%d", port),
			dataDir:  dir,
			cmd:      cmd,
		}
		waitForPort(port, 10*time.Second)
	}
	return nodes, cleanup
}

// TestECSpike_KillOneNodeStillReadable is the single Stage 2 go/no-go E2E test.
//
// Goal: prove that a 4+2 Reed-Solomon cluster survives a single-node failure
// and returns identical bytes after reconstruction, plus measure client-side
// p95 latency for 16MB PUT to drive the go/no-go decision.
func TestECSpike_KillOneNodeStillReadable(t *testing.T) {
	if testing.Short() {
		t.Skip("ecspike: requires full cluster bootstrap, skipped in -short")
	}

	nodes, cleanup := startEcspikeCluster(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Prepare config using the package's S3 helper for each node.
	endpoints := make([]string, len(nodes))
	for i, n := range nodes {
		endpoints[i] = n.endpoint
	}
	clients := make(map[string]*s3.Client, len(endpoints))
	for _, ep := range endpoints {
		clients[ep] = newS3Client(ep)
	}
	cfg := &ecspike.Config{
		Nodes:   endpoints,
		DataK:   4,
		ParityM: 2,
		Bucket:  "ecspike",
		S3Client: func(ep string) *s3.Client {
			return clients[ep]
		},
	}

	// Create the ecspike bucket on every node.
	for _, ep := range endpoints {
		_, err := clients[ep].CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(cfg.Bucket),
		})
		require.NoErrorf(t, err, "create bucket on %s", ep)
	}

	// Correctness: 10 × 16MB random objects, record SHA256.
	const objCount = 10
	const objSize = 16 * 1024 * 1024
	originals := make(map[string][32]byte, objCount)

	t.Logf("ecspike: writing %d × %d-byte objects", objCount, objSize)
	for i := 0; i < objCount; i++ {
		data := make([]byte, objSize)
		_, err := rand.Read(data)
		require.NoError(t, err, "rand data")
		key := fmt.Sprintf("obj-%02d", i)
		require.NoErrorf(t, ecspike.Put(ctx, cfg, key, data), "Put %s", key)
		originals[key] = sha256.Sum256(data)
	}

	// Kill node 0 and give the process a moment to release the port.
	t.Logf("ecspike: killing node 0 at %s", nodes[0].endpoint)
	nodes[0].kill()
	time.Sleep(500 * time.Millisecond)

	// Reconstruct every object and verify SHA256.
	for key, wantSum := range originals {
		got, err := ecspike.Get(ctx, cfg, key)
		if !assert.NoErrorf(t, err, "Get %s after node kill", key) {
			continue
		}
		gotSum := sha256.Sum256(got)
		assert.Equalf(t, wantSum, gotSum, "Get %s: sha256 mismatch (len=%d)", key, len(got))
	}

	if t.Failed() {
		return
	}
	t.Logf("ecspike: correctness PASS (%d/%d objects reconstructed)", objCount, objCount)

	// Latency measurement: 100 × 16MB PUT on a fresh cluster so node 0 is alive.
	// Reuse current cluster sans node 0 → skip this block; instead spawn a fresh
	// cluster for honest baseline.
	measureECSpikeP95(t)
}

// TestECSpike_RawShardP95 is the CONDITIONAL GO prerequisite remeasurement.
// Nodes run with --ec=false --no-encryption to eliminate nested solo EC overhead.
// Result determines whether Phase 18 Stage 3 is safe to start.
//
// go/no-go: p95 < 500ms → Phase 18 진입, p95 >= 500ms → Phase 18 보류.
func TestECSpike_RawShardP95(t *testing.T) {
	if testing.Short() {
		t.Skip("ecspike: requires full cluster bootstrap, skipped in -short")
	}
	measureECSpikeP95WithOpts(t, true)
}

// measureECSpikeP95 spawns a fresh 6-node cluster, does 100 × 16MB PUTs, and
// reports p50/p95/p99. Separate cluster so kills don't skew timing.
func measureECSpikeP95(t *testing.T) {
	measureECSpikeP95WithOpts(t, false)
}

func measureECSpikeP95WithOpts(t *testing.T, noEC bool) {
	t.Helper()
	var nodes []*ecspikeNode
	var cleanup func()
	if noEC {
		nodes, cleanup = startEcspikeClusterNoEC(t)
	} else {
		nodes, cleanup = startEcspikeCluster(t)
	}
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	endpoints := make([]string, len(nodes))
	for i, n := range nodes {
		endpoints[i] = n.endpoint
	}
	clients := make(map[string]*s3.Client, len(endpoints))
	for _, ep := range endpoints {
		clients[ep] = newS3Client(ep)
	}
	cfg := &ecspike.Config{
		Nodes:   endpoints,
		DataK:   4,
		ParityM: 2,
		Bucket:  "ecspike",
		S3Client: func(ep string) *s3.Client {
			return clients[ep]
		},
	}
	for _, ep := range endpoints {
		_, err := clients[ep].CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(cfg.Bucket),
		})
		require.NoErrorf(t, err, "create bucket on %s", ep)
	}

	const iter = 100
	const objSize = 16 * 1024 * 1024
	data := make([]byte, objSize)
	_, err := rand.Read(data)
	require.NoError(t, err, "rand data")

	ecLabel := "with nested EC"
	if noEC {
		ecLabel = "no EC (raw shard)"
	}
	latencies := make([]time.Duration, iter)
	t.Logf("ecspike: measuring p95 with %d × %d-byte PUTs (%s)", iter, objSize, ecLabel)
	for i := 0; i < iter; i++ {
		key := fmt.Sprintf("lat-%03d", i)
		start := time.Now()
		require.NoErrorf(t, ecspike.Put(ctx, cfg, key, data), "Put %s", key)
		latencies[i] = time.Since(start)
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p50 := latencies[iter/2]
	p95 := latencies[(iter*95)/100]
	p99 := latencies[(iter*99)/100]
	mean := time.Duration(0)
	for _, l := range latencies {
		mean += l
	}
	mean /= time.Duration(iter)

	t.Logf("ecspike p50=%s p95=%s p99=%s mean=%s (loopback, 6-process, 4+2, 16MB, %s)",
		p50, p95, p99, mean, ecLabel)

	if noEC {
		// CONDITIONAL GO threshold: p95 < 500ms for raw shard (no nested EC).
		const threshold = 500 * time.Millisecond
		if p95 >= threshold {
			t.Logf("CONDITIONAL GO: p95 %s >= %s — Phase 18 Stage 3 보류, 아키텍처 재검토 필요", p95, threshold)
		} else {
			t.Logf("CONDITIONAL GO: p95 %s < %s — Phase 18 Stage 3 진입 가능", p95, threshold)
		}
	} else {
		// Original go/no-go threshold from design: p95 < 200ms.
		const threshold = 200 * time.Millisecond
		if p95 >= threshold {
			t.Logf("ecspike go/no-go: p95 %s >= %s threshold — flag for go/no-go review", p95, threshold)
		} else {
			t.Logf("ecspike go/no-go: p95 %s < %s threshold — PASS", p95, threshold)
		}
	}
}
