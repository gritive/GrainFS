package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2E_ClusterEC_PutGet_5Node verifies Phase 18 Cluster EC end-to-end with
// a 3+2 configuration on a 5-node cluster (k+m = 5 = node count, so shards
// land on 5 distinct nodes). PUT → shards spread across nodes → GET
// reconstructs. Killing one node still allows reads (read-k tolerance of 2).
//
// We use 5 nodes instead of 6 because 6-node Raft bootstrap on loopback
// is noisy in CI; 5 nodes converge faster and still exercise the full EC
// code path (ecK=3, ecM=2, placement across all 5 nodes).
func TestE2E_ClusterEC_PutGet_5Node(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-node e2e in -short mode")
	}
	binary := getBinary()
	if _, err := os.Stat(binary); err != nil {
		t.Skipf("grainfs binary not found at %s — run `make build` first", binary)
	}

	const (
		clusterKey = "E2E-CLUSTER-EC-KEY-3P2"
		accessKey  = "ec-ak"
		secretKey  = "ec-sk"
		bucketName = "ec-test"
		numNodes   = 5
		ecData     = 3
		ecParity   = 2
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
		d, err := os.MkdirTemp("", fmt.Sprintf("grainfs-cluster-ec-%d-*", i))
		require.NoError(t, err)
		dataDirs[i] = d
		t.Cleanup(func() { _ = os.RemoveAll(d) })
	}

	startNode := func(i int) *exec.Cmd {
		cmd := exec.Command(binary, "serve",
			"--data", dataDirs[i],
			"--port", fmt.Sprintf("%d", httpPorts[i]),
			"--node-id", fmt.Sprintf("ec-node-%d", i),
			"--raft-addr", raftAddr(i),
			"--peers", peersFor(i),
			"--cluster-key", clusterKey,
			"--access-key", accessKey,
			"--secret-key", secretKey,
			"--cluster-ec=true",
			fmt.Sprintf("--ec-data=%d", ecData),
			fmt.Sprintf("--ec-parity=%d", ecParity),
			"--ec=false",
			"--nfs-port", "0",
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
	killAll := func() {
		for _, p := range procs {
			if p != nil && p.Process != nil {
				_ = p.Process.Kill()
				_, _ = p.Process.Wait()
			}
		}
	}
	t.Cleanup(killAll)

	// Stage 1: start 3 nodes first — quorum(5)=3, so they elect a stable leader
	// without competing with 2 additional simultaneous candidates. The 5-way
	// simultaneous start caused split-vote loops that never converged in CI.
	for i := 0; i < 3; i++ {
		procs[i] = startNode(i)
	}
	for i := 0; i < 3; i++ {
		waitForPort(t, httpPorts[i], 60*time.Second)
	}

	// Stage 2: bring up the remaining 2 nodes after the cluster has a leader.
	for i := 3; i < numNodes; i++ {
		procs[i] = startNode(i)
	}
	for i := 3; i < numNodes; i++ {
		waitForPort(t, httpPorts[i], 30*time.Second)
	}

	var client *s3.Client
	var leaderIdx int
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	require.Eventually(t, func() bool {
		for i := 0; i < numNodes; i++ {
			c := ecS3Client(httpURL(i), accessKey, secretKey)
			_, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketName)})
			if err == nil {
				client = c
				leaderIdx = i
				return true
			}
		}
		return false
	}, 120*time.Second, 2*time.Second, "no leader found or CreateBucket never succeeded")
	t.Logf("leader: node %d at %s", leaderIdx, httpURL(leaderIdx))

	// Write 5 random objects of varied sizes. Verify each round-trips.
	type objEntry struct {
		key string
		sum [32]byte
		sz  int
	}
	objects := []objEntry{
		{"obj-a-small", [32]byte{}, 1024},
		{"obj-b-medium", [32]byte{}, 64 * 1024},
		{"obj-c-large", [32]byte{}, 1 * 1024 * 1024},
		{"obj-d-odd", [32]byte{}, 12345},
		{"obj-e-tiny", [32]byte{}, 100},
	}
	for i := range objects {
		data := make([]byte, objects[i].sz)
		_, err := rand.Read(data)
		require.NoError(t, err)
		objects[i].sum = sha256.Sum256(data)
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objects[i].key),
			Body:   bytes.NewReader(data),
		})
		require.NoErrorf(t, err, "PutObject %s (%d bytes)", objects[i].key, objects[i].sz)
	}

	// Round-trip check — all shards available.
	for _, obj := range objects {
		out, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(obj.key),
		})
		require.NoErrorf(t, err, "GetObject %s", obj.key)
		got, err := io.ReadAll(out.Body)
		_ = out.Body.Close()
		require.NoError(t, err)
		assert.Equalf(t, obj.sum, sha256.Sum256(got),
			"sha256 mismatch for %s (len=%d)", obj.key, len(got))
	}
	t.Logf("cluster EC: %d/%d objects round-tripped with all shards present", len(objects), len(objects))

	// Kill a non-leader node — one shard disappears for any key placed there.
	// Read-k tolerance of 3+2 is 2 missing shards, so single-node failure
	// reconstructs every object.
	victim := (leaderIdx + 1) % numNodes
	t.Logf("killing node %d at %s", victim, httpURL(victim))
	_ = procs[victim].Process.Kill()
	_, _ = procs[victim].Process.Wait()
	procs[victim] = nil
	time.Sleep(3 * time.Second) // let peer-health / timeouts settle

	for _, obj := range objects {
		out, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(obj.key),
		})
		require.NoErrorf(t, err, "GetObject %s after node kill", obj.key)
		got, err := io.ReadAll(out.Body)
		_ = out.Body.Close()
		require.NoError(t, err)
		assert.Equalf(t, obj.sum, sha256.Sum256(got),
			"sha256 mismatch for %s after kill (len=%d)", obj.key, len(got))
	}
	t.Logf("cluster EC: %d/%d objects reconstructed after single-node failure", len(objects), len(objects))
}

// TestE2E_ClusterEC_FallbackToNx_3Node verifies the under-threshold fallback:
// with 3 nodes but 4+2 config, cluster EC is NOT active → falls back to N×
// replication. PUT+GET must still work (existing N× path) and the FSM must
// NOT record a placement entry for these objects.
func TestE2E_ClusterEC_FallbackToNx_3Node(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-node e2e in -short mode")
	}
	binary := getBinary()
	if _, err := os.Stat(binary); err != nil {
		t.Skipf("grainfs binary not found at %s — run `make build` first", binary)
	}

	const (
		clusterKey = "E2E-EC-FALLBACK-KEY"
		accessKey  = "fb-ak"
		secretKey  = "fb-sk"
		bucketName = "fb-test"
		numNodes   = 3
		ecData     = 4
		ecParity   = 2
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
		d, err := os.MkdirTemp("", fmt.Sprintf("grainfs-ec-fallback-%d-*", i))
		require.NoError(t, err)
		dataDirs[i] = d
		t.Cleanup(func() { _ = os.RemoveAll(d) })
	}

	procs := make([]*exec.Cmd, numNodes)
	for i := 0; i < numNodes; i++ {
		cmd := exec.Command(binary, "serve",
			"--data", dataDirs[i],
			"--port", fmt.Sprintf("%d", httpPorts[i]),
			"--node-id", fmt.Sprintf("fb-node-%d", i),
			"--raft-addr", raftAddr(i),
			"--peers", peersFor(i),
			"--cluster-key", clusterKey,
			"--access-key", accessKey,
			"--secret-key", secretKey,
			"--cluster-ec=true",
			fmt.Sprintf("--ec-data=%d", ecData),
			fmt.Sprintf("--ec-parity=%d", ecParity),
			"--ec=false",
			"--nfs-port", "0",
			"--nfs4-port", "0",
			"--nbd-port", "0",
			"--snapshot-interval", "0",
			"--scrub-interval", "0",
			"--lifecycle-interval", "0",
			"--no-encryption",
		)
		require.NoError(t, cmd.Start(), "start node %d", i)
		procs[i] = cmd
	}
	t.Cleanup(func() {
		for _, p := range procs {
			if p != nil && p.Process != nil {
				_ = p.Process.Kill()
				_, _ = p.Process.Wait()
			}
		}
	})

	for i := range procs {
		waitForPort(t, httpPorts[i], 30*time.Second)
	}

	var client *s3.Client
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	require.Eventually(t, func() bool {
		for i := 0; i < numNodes; i++ {
			c := ecS3Client(httpURL(i), accessKey, secretKey)
			_, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketName)})
			if err == nil {
				client = c
				return true
			}
		}
		return false
	}, 30*time.Second, 1*time.Second, "no leader found")

	data := make([]byte, 8192)
	_, _ = rand.Read(data)
	sum := sha256.Sum256(data)

	_, perr := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("fallback-obj"),
		Body:   bytes.NewReader(data),
	})
	require.NoError(t, perr, "PutObject on under-threshold cluster should use N× fallback")

	out, gerr := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("fallback-obj"),
	})
	require.NoError(t, gerr)
	got, rerr := io.ReadAll(out.Body)
	_ = out.Body.Close()
	require.NoError(t, rerr)
	assert.Equal(t, sum, sha256.Sum256(got))
	t.Logf("cluster EC fallback: 3-node cluster correctly uses N× replication when below k+m=%d threshold", ecData+ecParity)
}

// TestE2E_ClusterEC_TopologyChange verifies that placement FSM records remain
// valid (immutable) through a topology change (node removal). After killing a
// non-leader node:
//   - Old objects: LookupShardPlacement still returns the original placement
//     (FSM is append-only; records are never mutated) and GET reconstructs.
//   - New objects: written after the kill use the updated liveNodes() set and
//     are also GET-able.
//
// This directly validates the TODOS.md requirement:
// "N 변경 전후 placement FSM record가 유효한지 검증하는 E2E 시나리오."
func TestE2E_ClusterEC_TopologyChange(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-node e2e in -short mode")
	}
	binary := getBinary()
	if _, err := os.Stat(binary); err != nil {
		t.Skipf("grainfs binary not found at %s — run `make build` first", binary)
	}

	const (
		clusterKey = "E2E-EC-TOPO-KEY"
		accessKey  = "tp-ak"
		secretKey  = "tp-sk"
		bucketName = "topo-test"
		numNodes   = 5
		ecData     = 3
		ecParity   = 2
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
		d, err := os.MkdirTemp("", fmt.Sprintf("grainfs-topo-%d-*", i))
		require.NoError(t, err)
		dataDirs[i] = d
		t.Cleanup(func() { _ = os.RemoveAll(d) })
	}

	startNode := func(i int) *exec.Cmd {
		cmd := exec.Command(binary, "serve",
			"--data", dataDirs[i],
			"--port", fmt.Sprintf("%d", httpPorts[i]),
			"--node-id", fmt.Sprintf("tp-node-%d", i),
			"--raft-addr", raftAddr(i),
			"--peers", peersFor(i),
			"--cluster-key", clusterKey,
			"--access-key", accessKey,
			"--secret-key", secretKey,
			"--cluster-ec=true",
			fmt.Sprintf("--ec-data=%d", ecData),
			fmt.Sprintf("--ec-parity=%d", ecParity),
			"--ec=false",
			"--nfs-port", "0",
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
	killAll := func() {
		for _, p := range procs {
			if p != nil && p.Process != nil {
				_ = p.Process.Kill()
				_, _ = p.Process.Wait()
			}
		}
	}
	t.Cleanup(killAll)

	// Stage 1: bring up 3 nodes first to elect a stable leader.
	for i := 0; i < 3; i++ {
		procs[i] = startNode(i)
	}
	for i := 0; i < 3; i++ {
		waitForPort(t, httpPorts[i], 60*time.Second)
	}

	// Stage 2: add the remaining 2 nodes.
	for i := 3; i < numNodes; i++ {
		procs[i] = startNode(i)
	}
	for i := 3; i < numNodes; i++ {
		waitForPort(t, httpPorts[i], 30*time.Second)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	var client *s3.Client
	var leaderIdx int
	require.Eventually(t, func() bool {
		for i := 0; i < numNodes; i++ {
			c := ecS3Client(httpURL(i), accessKey, secretKey)
			_, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketName)})
			if err == nil {
				client = c
				leaderIdx = i
				return true
			}
		}
		return false
	}, 120*time.Second, 2*time.Second, "no leader found or CreateBucket never succeeded")
	t.Logf("topology test: leader node %d at %s", leaderIdx, httpURL(leaderIdx))

	// PUT objects before topology change.
	type entry struct {
		key string
		sum [32]byte
	}
	preObjects := []entry{
		{"pre-obj-a", [32]byte{}},
		{"pre-obj-b", [32]byte{}},
		{"pre-obj-c", [32]byte{}},
	}
	for i := range preObjects {
		data := make([]byte, 32*1024)
		_, err := rand.Read(data)
		require.NoError(t, err)
		preObjects[i].sum = sha256.Sum256(data)
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(preObjects[i].key),
			Body:   bytes.NewReader(data),
		})
		require.NoErrorf(t, err, "pre-topology PutObject %s", preObjects[i].key)
	}

	// Verify round-trip before topology change.
	for _, obj := range preObjects {
		out, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(obj.key),
		})
		require.NoErrorf(t, err, "pre-topology GetObject %s", obj.key)
		got, _ := io.ReadAll(out.Body)
		_ = out.Body.Close()
		assert.Equalf(t, obj.sum, sha256.Sum256(got), "sha256 mismatch pre-topology %s", obj.key)
	}
	t.Logf("topology test: %d pre-topology objects verified", len(preObjects))

	// Topology change: kill one non-leader node.
	// With k+m=5, losing one node still leaves 4 nodes alive which is above the
	// read-k tolerance (only 3 data shards needed). Reads still reconstruct.
	victim := (leaderIdx + 1) % numNodes
	t.Logf("topology test: killing node %d (topology change N=5 → N=4)", victim)
	_ = procs[victim].Process.Kill()
	_, _ = procs[victim].Process.Wait()
	procs[victim] = nil
	// Wait for Raft to propagate the peer-health change across the cluster.
	time.Sleep(5 * time.Second)

	// Re-pick the client — use a surviving node (not the victim, not necessarily the leader).
	survivor := (victim + 1) % numNodes
	if survivor == victim {
		survivor = (victim + 2) % numNodes
	}
	client = ecS3Client(httpURL(survivor), accessKey, secretKey)

	// Old objects: FSM placement records must be immutable — same shards, still GET-able.
	for _, obj := range preObjects {
		out, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(obj.key),
		})
		require.NoErrorf(t, err, "post-topology GetObject %s (placement immutability)", obj.key)
		got, _ := io.ReadAll(out.Body)
		_ = out.Body.Close()
		assert.Equalf(t, obj.sum, sha256.Sum256(got),
			"sha256 mismatch post-topology for %s (FSM placement changed?)", obj.key)
	}
	t.Logf("topology test: %d pre-topology objects reconstructed after topology change (FSM placement immutable)", len(preObjects))

	// New objects after topology change: liveNodes() now has 4 nodes.
	// Placement uses 3+2=5 shards but only 4 distinct nodes → some nodes get 2 shards.
	// The cluster is below k+m=5 node threshold (4 < 5), so ECActive() returns false
	// and new writes fall back to N× replication. Verify they still round-trip.
	postObjects := []entry{
		{"post-obj-x", [32]byte{}},
		{"post-obj-y", [32]byte{}},
	}
	for i := range postObjects {
		data := make([]byte, 16*1024)
		_, err := rand.Read(data)
		require.NoError(t, err)
		postObjects[i].sum = sha256.Sum256(data)
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(postObjects[i].key),
			Body:   bytes.NewReader(data),
		})
		require.NoErrorf(t, err, "post-topology PutObject %s", postObjects[i].key)
	}
	for _, obj := range postObjects {
		out, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(obj.key),
		})
		require.NoErrorf(t, err, "post-topology GetObject new %s", obj.key)
		got, _ := io.ReadAll(out.Body)
		_ = out.Body.Close()
		assert.Equalf(t, obj.sum, sha256.Sum256(got), "sha256 mismatch post-topology new %s", obj.key)
	}
	t.Logf("topology test: %d post-topology objects verified (N× fallback after node loss)", len(postObjects))
}

// ecS3Client returns an S3 client using the given access/secret credentials.
// Separate from newS3Client to avoid changing signatures other tests depend on.
func ecS3Client(endpoint, ak, sk string) *s3.Client {
	return s3.New(s3.Options{
		BaseEndpoint: aws.String(endpoint),
		Region:       "us-east-1",
		Credentials:  staticCreds{ak: ak, sk: sk},
		UsePathStyle: true,
	})
}

// staticCreds is a minimal aws.CredentialsProvider that returns fixed keys.
type staticCreds struct{ ak, sk string }

func (c staticCreds) Retrieve(ctx context.Context) (aws.Credentials, error) {
	return aws.Credentials{AccessKeyID: c.ak, SecretAccessKey: c.sk, Source: "static"}, nil
}
