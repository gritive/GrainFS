package e2e

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gritive/GrainFS/internal/cluster/ecspike"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// ecspikeNode is one of the 6 grainfs local processes used by the spike.
type ecspikeNode struct {
	port     int
	endpoint string
	dataDir  string
	cmd      *exec.Cmd
	saID     string
	ak, sk   string
	logFile  *os.File
}

func (n *ecspikeNode) kill() {
	terminateProcess(n.cmd)
}

// startEcspikeCluster spawns 6 independent grainfs serve processes in local mode
// on loopback. Returns the node list and a cleanup func. Uses existing test
// harness helpers (freePort, waitForPort, newS3Client).
func startEcspikeCluster(t testing.TB) ([]*ecspikeNode, func()) {
	return startEcspikeClusterOpts(t)
}

func startEcspikeClusterOpts(t testing.TB) ([]*ecspikeNode, func()) {
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
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "mkdtemp node %d", i)
		}
		port := freePort()
		args := []string{
			"serve",
			"--data", dir,
			"--port", fmt.Sprintf("%d", port),
			"--nfs4-port", fmt.Sprintf("%d", freePort()),
			"--nbd-port", fmt.Sprintf("%d", freePort()),
			"--scrub-interval", "0",
			"--lifecycle-interval", "0",
			"--cluster-key", "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
		}
		cmd := exec.Command(binary, args...)
		logFile, err := os.CreateTemp("", fmt.Sprintf("ecspike-node-%d-*.log", i))
		if err != nil {
			cleanup()
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "create node %d log", i)
		}
		cmd.Stderr = logFile
		// Assign before Start so cleanup() can remove dir even if Start fails.
		nodes[i] = &ecspikeNode{
			port:     port,
			endpoint: fmt.Sprintf("http://127.0.0.1:%d", port),
			dataDir:  dir,
			cmd:      cmd,
			logFile:  logFile,
		}
		nodeIndex := i
		ginkgo.DeferCleanup(func() {
			_ = logFile.Close()
			if keepE2EArtifacts() {
				t.Logf("ECSpike node %d stderr saved to %s", nodeIndex, logFile.Name())
			}
		})
		if err := cmd.Start(); err != nil {
			cleanup()
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "start node %d", i)
		}
		gomega.Expect(waitForPortsParallelErrWithProcesses([]int{port}, []*exec.Cmd{cmd}, 30*time.Second)).
			To(gomega.Succeed(), "server did not start on port %d; stderr saved to %s", port, logFile.Name())
		bootstrap, _ := bootstrapAdminViaUDSAnyResult(t, []string{dir}, 10*time.Second)
		nodes[i].saID = bootstrap.SAID
		nodes[i].ak = bootstrap.AccessKey
		nodes[i].sk = bootstrap.SecretKey
	}
	return nodes, cleanup
}

func requireECSpikeBucketsReady(t testing.TB, ctx context.Context, cfg *ecspike.Config, nodes []*ecspikeNode, clients map[string]*s3.Client) {
	t.Helper()

	const readinessKey = "__grainfs_e2e_ready"
	byEndpoint := make(map[string]*ecspikeNode, len(nodes))
	for _, n := range nodes {
		byEndpoint[n.endpoint] = n
	}
	for _, ep := range cfg.Nodes {
		node := byEndpoint[ep]
		gomega.Expect(node).NotTo(gomega.BeNil(), "node for endpoint %s", ep)
		gomega.Expect(node.saID).NotTo(gomega.BeEmpty(), "bootstrap SA ID for %s", ep)
		sock := filepath.Join(node.dataDir, "admin.sock")
		gomega.Expect(tryAdminCreateBucketWithPolicyAttach(sock, cfg.Bucket, node.saID, "bucket-admin")).
			To(gomega.Succeed(), "admin create bucket on %s", ep)
		client := clients[ep]
		waitForS3Write(t, client, cfg.Bucket, readinessKey, 30*time.Second)
		_, _ = client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(cfg.Bucket),
			Key:    aws.String(readinessKey),
		})
	}
}

var _ = ginkgo.Describe("EC spike cluster", func() {
	ginkgo.It("survives one killed node and remains readable", func() {
		t := ginkgo.GinkgoTB()

		nodes, cleanup := startEcspikeCluster(t)
		ginkgo.DeferCleanup(cleanup)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		ginkgo.DeferCleanup(cancel)

		// Prepare config using the package's S3 helper for each node.
		endpoints := make([]string, len(nodes))
		for i, n := range nodes {
			endpoints[i] = n.endpoint
		}
		clients := make(map[string]*s3.Client, len(endpoints))
		for _, n := range nodes {
			clients[n.endpoint] = s3ClientFor(n.endpoint, n.ak, n.sk)
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

		requireECSpikeBucketsReady(t, ctx, cfg, nodes, clients)

		// Correctness: 10 × 16MB random objects, record SHA256.
		const objCount = 10
		const objSize = 16 * 1024 * 1024
		originals := make(map[string][32]byte, objCount)

		t.Logf("ecspike: writing %d × %d-byte objects", objCount, objSize)
		for i := 0; i < objCount; i++ {
			data := make([]byte, objSize)
			_, err := rand.Read(data)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "rand data")
			key := fmt.Sprintf("obj-%02d", i)
			gomega.Expect(ecspike.Put(ctx, cfg, key, data)).To(gomega.Succeed(), "Put %s", key)
			originals[key] = sha256.Sum256(data)
		}

		// Kill node 0 and give the process a moment to release the port.
		t.Logf("ecspike: killing node 0 at %s", nodes[0].endpoint)
		nodes[0].kill()
		time.Sleep(500 * time.Millisecond)

		// Reconstruct every object and verify SHA256.
		for key, wantSum := range originals {
			got, err := ecspike.Get(ctx, cfg, key)
			if err != nil {
				gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Get %s after node kill", key)
				continue
			}
			gotSum := sha256.Sum256(got)
			gomega.Expect(gotSum).To(gomega.Equal(wantSum), "Get %s: sha256 mismatch (len=%d)", key, len(got))
		}

		if t.Failed() {
			return
		}
		t.Logf("ecspike: correctness PASS (%d/%d objects reconstructed)", objCount, objCount)

		// Latency measurement: 100 × 16MB PUT on a fresh cluster so node 0 is alive.
		// Reuse current cluster sans node 0 → skip this block; instead spawn a fresh
		// cluster for honest baseline.
		measureECSpikeP95(t)
	})
})

// measureECSpikeP95 spawns a fresh 6-node cluster, does 100 × 16MB PUTs, and
// reports p50/p95/p99. Separate cluster so kills don't skew timing.
func measureECSpikeP95(t testing.TB) {
	t.Helper()
	nodes, cleanup := startEcspikeCluster(t)
	ginkgo.DeferCleanup(cleanup)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	ginkgo.DeferCleanup(cancel)

	endpoints := make([]string, len(nodes))
	for i, n := range nodes {
		endpoints[i] = n.endpoint
	}
	clients := make(map[string]*s3.Client, len(endpoints))
	for _, n := range nodes {
		clients[n.endpoint] = s3ClientFor(n.endpoint, n.ak, n.sk)
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
	requireECSpikeBucketsReady(t, ctx, cfg, nodes, clients)

	const iter = 100
	const objSize = 16 * 1024 * 1024
	data := make([]byte, objSize)
	_, err := rand.Read(data)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "rand data")

	latencies := make([]time.Duration, iter)
	t.Logf("ecspike: measuring p95 with %d × %d-byte PUTs", iter, objSize)
	for i := 0; i < iter; i++ {
		key := fmt.Sprintf("lat-%03d", i)
		start := time.Now()
		gomega.Expect(ecspike.Put(ctx, cfg, key, data)).To(gomega.Succeed(), "Put %s", key)
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
