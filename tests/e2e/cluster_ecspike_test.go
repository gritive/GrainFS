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
	return startEcspikeClusterOpts(t)
}

func startEcspikeClusterOpts(t *testing.T) ([]*ecspikeNode, func()) {
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
			"--nfs4-port", "0",
			"--nbd-port", "0",
		}
		cmd := exec.Command(binary, args...)
		// Assign before Start so cleanup() can remove dir even if Start fails.
		nodes[i] = &ecspikeNode{
			port:     port,
			endpoint: fmt.Sprintf("http://127.0.0.1:%d", port),
			dataDir:  dir,
			cmd:      cmd,
		}
		if err := cmd.Start(); err != nil {
			cleanup()
			require.NoErrorf(t, err, "start node %d", i)
		}
		waitForPort(t, port, 10*time.Second)
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

// measureECSpikeP95 spawns a fresh 6-node cluster, does 100 × 16MB PUTs, and
// reports p50/p95/p99. Separate cluster so kills don't skew timing.
func measureECSpikeP95(t *testing.T) {
	t.Helper()
	nodes, cleanup := startEcspikeCluster(t)
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

	latencies := make([]time.Duration, iter)
	t.Logf("ecspike: measuring p95 with %d × %d-byte PUTs", iter, objSize)
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

	t.Logf("ecspike p50=%s p95=%s p99=%s mean=%s (loopback, 6-process, 4+2, 16MB)",
		p50, p95, p99, mean)

	// Original go/no-go threshold from design: p95 < 200ms.
	const threshold = 200 * time.Millisecond
	if p95 >= threshold {
		t.Logf("ecspike go/no-go: p95 %s >= %s threshold — flag for go/no-go review", p95, threshold)
	} else {
		t.Logf("ecspike go/no-go: p95 %s < %s threshold — PASS", p95, threshold)
	}
}
