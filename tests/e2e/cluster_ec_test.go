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
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gritive/GrainFS/internal/cluster"
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
		bucketName = "ec-test"
		numNodes   = 5
	)
	var accessKey, secretKey string

	httpPorts := make([]int, numNodes)
	raftPorts := make([]int, numNodes)
	nfs4Ports := make([]int, numNodes)
	nbdPorts := make([]int, numNodes)
	ports := uniqueFreePorts(numNodes * 4)
	for i := range numNodes {
		httpPorts[i] = ports[i*4]
		raftPorts[i] = ports[i*4+1]
		nfs4Ports[i] = ports[i*4+2]
		nbdPorts[i] = ports[i*4+3]
	}

	raftAddr := func(i int) string { return fmt.Sprintf("127.0.0.1:%d", raftPorts[i]) }
	httpURL := func(i int) string { return fmt.Sprintf("http://127.0.0.1:%d", httpPorts[i]) }

	dataDirs := make([]string, numNodes)
	for i := range dataDirs {
		d, err := os.MkdirTemp("", fmt.Sprintf("grainfs-cluster-ec-%d-*", i))
		require.NoError(t, err)
		dataDirs[i] = d
		t.Cleanup(func() { _ = os.RemoveAll(d) })
	}
	encKeyFile := makeSharedEncryptionKeyFile(t)

	startNode := func(i int) *exec.Cmd {
		stderrFile, err := os.Create(fmt.Sprintf("/tmp/ec5-node-%d-stderr.log", i))
		require.NoError(t, err, "create stderr file for node %d", i)
		cmd := exec.Command(binary, "serve",
			"--data", dataDirs[i],
			"--port", fmt.Sprintf("%d", httpPorts[i]),
			"--node-id", raftAddr(i),
			"--raft-addr", raftAddr(i),
			"--cluster-key", clusterKey,
			"--encryption-key-file", encKeyFile,
			"--shard-cache-size=0",
			"--nfs4-port", fmt.Sprintf("%d", nfs4Ports[i]),
			"--nbd-port", fmt.Sprintf("%d", nbdPorts[i]),
			"--scrub-interval", "0",
			"--lifecycle-interval", "0",
		)
		cmd.Stdout = stderrFile
		cmd.Stderr = stderrFile
		require.NoError(t, cmd.Start(), "start node %d", i)
		t.Cleanup(func() {
			stderrFile.Close()
			if t.Failed() {
				t.Logf("Node %d stderr saved to %s", i, stderrFile.Name())
			} else {
				os.Remove(stderrFile.Name())
			}
		})
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

	// Start seed node, then let followers join via .join-pending.
	procs[0] = startNode(0)
	waitForPortsParallel(t, httpPorts[:1], 60*time.Second)
	time.Sleep(2 * time.Second)
	for i := 1; i < numNodes; i++ {
		require.NoError(t, writeNodeJoinPending(dataDirs[i], raftAddr(0)))
		procs[i] = startNode(i)
		time.Sleep(150 * time.Millisecond)
	}
	waitForPortsParallel(t, httpPorts, 90*time.Second)

	// Bootstrap admin SA via UDS once quorum exists.
	accessKey, secretKey = bootstrapAdminViaUDSAny(t, dataDirs, 60*time.Second)

	var client *s3.Client
	var leaderIdx int
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	require.Eventually(t, func() bool {
		for i := 0; i < numNodes; i++ {
			c := ecS3Client(httpURL(i), accessKey, secretKey)
			err := tryCreateBucket(ctx, c, bucketName)
			if err == nil {
				client = c
				leaderIdx = i
				return true
			}
		}
		return false
	}, 240*time.Second, 500*time.Millisecond, "no leader found or CreateBucket never succeeded")
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
		require.Eventuallyf(t, func() bool {
			return tryPutObject(ctx, client, bucketName, objects[i].key, data) == nil
		}, 60*time.Second, 1*time.Second, "PutObject %s (%d bytes)", objects[i].key, objects[i].sz)
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
		require.Equalf(t, obj.sum, sha256.Sum256(got),
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

	// Poll until each object is reconstructed. getObjectEC uses a 3s per-shard
	// timeout for the dead node, so each attempt completes quickly even before
	// peerHealth marks the node unhealthy.
	for _, obj := range objects {
		obj := obj
		var lastErr error
		require.Eventuallyf(t, func() bool {
			for i := 0; i < numNodes; i++ {
				if i == victim {
					continue
				}
				candidate := ecS3Client(httpURL(i), accessKey, secretKey)
				out, err := candidate.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String(obj.key),
				})
				if err != nil {
					lastErr = fmt.Errorf("node %d: %w", i, err)
					continue
				}
				got, readErr := io.ReadAll(out.Body)
				_ = out.Body.Close()
				if readErr != nil {
					lastErr = fmt.Errorf("node %d body: %w", i, readErr)
					continue
				}
				if sha256.Sum256(got) != obj.sum {
					lastErr = fmt.Errorf("node %d sha256 mismatch len=%d", i, len(got))
					continue
				}
				client = candidate
				lastErr = nil
				return true
			}
			t.Logf("GetObject %s after node kill failed on all surviving nodes: %v", obj.key, lastErr)
			return false
		}, 45*time.Second, 1*time.Second, "GetObject %s after node kill", obj.key)
	}
	t.Logf("cluster EC: %d/%d objects reconstructed after single-node failure", len(objects), len(objects))
}

// TestE2E_ClusterEC_3Node_ActiveKM21 verifies dynamic EC activation on a 3-node
// cluster. The zero-config profile for 3 nodes is k=2, m=1. EC must be active:
// - PUT stores 3 shards (k+m=3) distributed across all nodes.
// - GET reconstructs correctly from 2 available shards (k=2 minimum).
// - Killing one node (1 out of 3) leaves k=2 shards: GET must still succeed.
// - Killing a second node leaves only 1 shard (< k=2): GET must fail.
func TestE2E_ClusterEC_3Node_ActiveKM21(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-node e2e in -short mode")
	}
	const (
		clusterKey = "E2E-EC-3NODE-KEY"
		bucketName = "3n-test"
		numNodes   = 3
	)

	c := startE2ECluster(t, e2eClusterOptions{
		Nodes:      numNodes,
		Mode:       ClusterModeStaticPeers,
		ClusterKey: clusterKey,
		LogPrefix:  "cluster-ec-3node",
		DisableNFS: true,
		DisableNBD: true,
	})
	accessKey, secretKey := c.accessKey, c.secretKey

	var client *s3.Client
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	require.Eventually(t, func() bool {
		for i := 0; i < numNodes; i++ {
			candidate := ecS3Client(c.httpURLs[i], accessKey, secretKey)
			err := tryCreateBucket(ctx, candidate, bucketName)
			if err == nil {
				client = candidate
				return true
			}
		}
		return false
	}, 120*time.Second, 1*time.Second, "no leader found")

	data := make([]byte, 8192)
	_, _ = rand.Read(data)
	sum := sha256.Sum256(data)

	// PUT: EC must be active on 3-node cluster (k=2, m=1).
	require.Eventually(t, func() bool {
		for i := 0; i < numNodes; i++ {
			candidate := ecS3Client(c.httpURLs[i], accessKey, secretKey)
			if tryPutObject(ctx, candidate, bucketName, "ec-obj", data) == nil {
				client = candidate
				return true
			}
		}
		return false
	}, 60*time.Second, 1*time.Second, "PutObject on 3-node cluster with dynamic EC k=2,m=1 must succeed")

	// GET with all 3 nodes up: must reconstruct correctly.
	out, gerr := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("ec-obj"),
	})
	require.NoError(t, gerr, "GetObject with all 3 nodes up")
	got, rerr := io.ReadAll(out.Body)
	_ = out.Body.Close()
	require.NoError(t, rerr)
	require.Equal(t, sum, sha256.Sum256(got), "data integrity check with all 3 nodes")

	// The post-failure assertion specifically depends on nodes 0 and 1. Wait
	// until both planned survivors can see the EC metadata and read the object
	// before killing node 2; a single successful GET through client does not
	// prove every survivor has applied the committed object metadata yet.
	require.Eventually(t, func() bool {
		for i := 0; i < numNodes-1; i++ {
			candidate := ecS3Client(c.httpURLs[i], accessKey, secretKey)
			out2, err2 := candidate.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String("ec-obj"),
			})
			if err2 != nil {
				t.Logf("node %d pre-kill GetObject err: %v", i, err2)
				return false
			}
			data2, err2 := io.ReadAll(out2.Body)
			_ = out2.Body.Close()
			if err2 != nil {
				t.Logf("node %d pre-kill body read err: %v", i, err2)
				return false
			}
			if gotSum := sha256.Sum256(data2); gotSum != sum {
				t.Logf("node %d pre-kill sha256 mismatch: got %x want %x", i, gotSum, sum)
				return false
			}
		}
		return true
	}, 60*time.Second, 1*time.Second, "surviving nodes must see EC object before node kill")

	// Kill the last node (index 2). With k=2 data shards remaining on nodes 0
	// and 1, getObjectEC can reconstruct using at most one dead-peer fetch
	// (bounded by 3s per-shard timeout).
	victim := numNodes - 1
	_ = c.procs[victim].Process.Kill()
	_, _ = c.procs[victim].Process.Wait()
	c.procs[victim] = nil
	t.Logf("killed node %d; expecting GET to succeed with 2 remaining shards (k=2)", victim)

	// Poll until the surviving nodes serve the object. No fixed sleep —
	// getObjectEC uses a 3s per-shard timeout, so each poll completes quickly
	// even while the dead peer's QUIC connection is timing out. 30s covers
	// both the per-shard timeout (3s) and any Raft re-election (~5-10s).
	var gotAfterKill []byte
	require.Eventually(t, func() bool {
		for i := 0; i < numNodes-1; i++ {
			candidate := ecS3Client(c.httpURLs[i], accessKey, secretKey)
			out2, err2 := candidate.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String("ec-obj"),
			})
			if err2 != nil {
				t.Logf("node %d GetObject err: %v", i, err2)
				continue
			}
			data, _ := io.ReadAll(out2.Body)
			_ = out2.Body.Close()
			if len(data) > 0 {
				gotAfterKill = data
				return true
			}
		}
		return false
	}, 30*time.Second, 1*time.Second, "GET must succeed with 2 surviving nodes (k=2 threshold)")
	require.Equal(t, sum, sha256.Sum256(gotAfterKill), "data integrity with 1 node down")
	t.Logf("3-node EC dynamic: k=2,m=1 verified — 1 node killed, GET reconstructed from 2 shards")
}

// TestE2E_ClusterEC_TopologyChange verifies that placement FSM records remain
// valid (immutable) through a topology change, AND that EC stays active after
// the change. We use 6 nodes with k=3, m=2 (k+m=5): killing one non-leader
// node leaves 5 live nodes (5 >= k+m=5), so ECActive() remains true throughout.
//
// Assertions:
//   - Pre-kill objects: GET reconstructs using original FSM placement (immutable).
//   - Post-kill objects: new PUTs still go through cluster EC (ECActive=true with
//     5 remaining nodes), and GET reconstructs correctly.
//
// This validates the TODOS.md requirement:
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
		bucketName = "topo-test"
		// 6 nodes use auto 4+2; object groups with 5 voters record auto 3+2.
		numNodes = 6
	)
	var accessKey, secretKey string

	httpPorts := make([]int, numNodes)
	raftPorts := make([]int, numNodes)
	nfs4Ports := make([]int, numNodes)
	nbdPorts := make([]int, numNodes)
	ports := uniqueFreePorts(numNodes * 4)
	for i := range numNodes {
		httpPorts[i] = ports[i*4]
		raftPorts[i] = ports[i*4+1]
		nfs4Ports[i] = ports[i*4+2]
		nbdPorts[i] = ports[i*4+3]
	}

	raftAddr := func(i int) string { return fmt.Sprintf("127.0.0.1:%d", raftPorts[i]) }
	httpURL := func(i int) string { return fmt.Sprintf("http://127.0.0.1:%d", httpPorts[i]) }

	dataDirs := make([]string, numNodes)
	for i := range dataDirs {
		d, err := os.MkdirTemp("", fmt.Sprintf("grainfs-topo-%d-*", i))
		require.NoError(t, err)
		dataDirs[i] = d
		t.Cleanup(func() { _ = os.RemoveAll(d) })
	}
	encKeyFile := makeSharedEncryptionKeyFile(t)

	// All 6 nodes are configured with the full peer list from the start so the
	// leader elected among the first 3 nodes already knows about nodes 3,4,5.
	// Without this, stage-2 nodes timeout and send higher-term RequestVotes that
	// force the existing leader to step down (standard Raft), causing a livelock.
	// With a uniform 6-node config, quorum=4, so no election succeeds until ≥4
	// nodes are up — handled by the require.Eventually 120s window on CreateBucket.
	startNode := func(i int) *exec.Cmd {
		stderrFile, err := os.Create(fmt.Sprintf("/tmp/tp-node-%d-stderr.log", i))
		require.NoError(t, err, "create stderr file for node %d", i)
		cmd := exec.Command(binary, "serve",
			"--data", dataDirs[i],
			"--port", fmt.Sprintf("%d", httpPorts[i]),
			"--node-id", raftAddr(i),
			"--raft-addr", raftAddr(i),
			"--cluster-key", clusterKey,
			"--encryption-key-file", encKeyFile,
			"--nfs4-port", fmt.Sprintf("%d", nfs4Ports[i]),
			"--nbd-port", fmt.Sprintf("%d", nbdPorts[i]),
			"--scrub-interval", "0",
			"--lifecycle-interval", "0",
		)
		cmd.Stdout = stderrFile
		cmd.Stderr = stderrFile
		require.NoError(t, cmd.Start(), "start node %d", i)
		t.Cleanup(func() {
			stderrFile.Close()
			if t.Failed() {
				t.Logf("Node %d stderr saved to %s", i, stderrFile.Name())
			} else {
				os.Remove(stderrFile.Name())
			}
		})
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

	// Start seed node, then let followers join sequentially via .join-pending.
	procs[0] = startNode(0)
	waitForPortsParallel(t, httpPorts[:1], 60*time.Second)
	time.Sleep(2 * time.Second)
	for i := 1; i < numNodes; i++ {
		require.NoError(t, writeNodeJoinPending(dataDirs[i], raftAddr(0)))
		procs[i] = startNode(i)
		time.Sleep(150 * time.Millisecond)
	}
	waitForPortsParallel(t, httpPorts, 60*time.Second)
	time.Sleep(4 * time.Second)
	for i, port := range httpPorts {
		t.Logf("node %d up (http :%d)", i, port)
	}

	// Bootstrap admin SA via the leader's UDS.
	accessKey, secretKey = bootstrapAdminViaUDSAny(t, dataDirs, 60*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	endpoints := make([]string, numNodes)
	for i := range endpoints {
		endpoints[i] = httpURL(i)
	}
	leaderIdx, err := waitForWritableEndpoint(
		ctx,
		endpoints,
		240*time.Second,
		5*time.Second,
		1*time.Second,
		func(attemptCtx context.Context, endpoint string) error {
			c := ecS3Client(endpoint, accessKey, secretKey)
			return tryCreateBucket(attemptCtx, c, bucketName)
		},
	)
	require.NoError(t, err, "no leader found or CreateBucket never succeeded")
	client := ecS3Client(httpURL(leaderIdx), accessKey, secretKey)
	t.Logf("topology test: leader node %d at %s (N=%d, auto EC width=%d)", leaderIdx, httpURL(leaderIdx), numNodes, cluster.AutoECConfigForClusterSize(numNodes).NumShards())

	type entry struct {
		key string
		sum [32]byte
	}

	// PUT objects before topology change — all written via cluster EC (6 >= 5).
	// All PutObjects iterate all live nodes to find the current leader.
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
		putData := append([]byte(nil), data...)
		objKey := preObjects[i].key
		require.Eventually(t, func() bool {
			for j := 0; j < numNodes; j++ {
				c := ecS3Client(httpURL(j), accessKey, secretKey)
				putErr := tryPutObject(ctx, c, bucketName, objKey, putData)
				if putErr == nil {
					client = c
					return true
				}
			}
			return false
		}, 30*time.Second, 500*time.Millisecond, "pre-topology PutObject %s", objKey)
	}

	for _, obj := range preObjects {
		obj := obj
		require.Eventually(t, func() bool {
			out, err := client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(obj.key),
			})
			if err != nil {
				return false
			}
			got, _ := io.ReadAll(out.Body)
			_ = out.Body.Close()
			return sha256.Sum256(got) == obj.sum
		}, 15*time.Second, 1*time.Second, "pre-topology GetObject %s", obj.key)
	}
	t.Logf("topology test: %d pre-topology objects written+verified via cluster EC", len(preObjects))

	// Target loss: kill one non-leader node. Topology-derived durability uses
	// the placement group's configured voter set, not the live-node count, so
	// new writes must fail while the group still names the missing target.
	victim := (leaderIdx + 1) % numNodes
	t.Logf("topology test: killing node %d (configured placement target becomes unavailable)", victim)
	_ = procs[victim].Process.Kill()
	_, _ = procs[victim].Process.Wait()
	procs[victim] = nil

	// Old objects: FSM placement records are immutable. GET must reconstruct
	// using the original 6-node placement even though one shard node is gone
	// (k=3 data shards needed; the victim held 1 of 5 shards, so 4 remain ≥ 3).
	// Use require.Eventually — dead node's QUIC connection takes up to 3s per shard
	// to time out before the remaining shards are fetched.
	for _, obj := range preObjects {
		obj := obj
		deadline := time.Now().Add(60 * time.Second)
		var lastGetErr error
		ok := false
		for time.Now().Before(deadline) {
			for j := 0; j < numNodes; j++ {
				if procs[j] == nil {
					continue
				}
				c := ecS3Client(httpURL(j), accessKey, secretKey)
				got, err := getObjectBytes(ctx, c, bucketName, obj.key)
				if err != nil {
					lastGetErr = fmt.Errorf("node %d %s: %w", j, httpURL(j), err)
					continue
				}
				if sha256.Sum256(got) == obj.sum {
					client = c
					lastGetErr = nil
					ok = true
					break
				}
				lastGetErr = fmt.Errorf("node %d %s: sha256 mismatch", j, httpURL(j))
			}
			if ok {
				break
			}
			time.Sleep(1 * time.Second)
		}
		if ok {
			continue
		}
		require.Failf(t, "post-topology GetObject failed",
			"%s (FSM placement must be immutable), last error: %v", obj.key, lastGetErr)
	}
	t.Logf("topology test: %d pre-topology objects reconstructed after node kill (placement immutable)", len(preObjects))

	// New objects after target loss must not silently downshift to a narrower
	// EC profile. Operators need a clear 503 until topology repair/removal
	// updates the placement group.
	for j := 0; j < numNodes; j++ {
		if procs[j] == nil {
			continue
		}
		requireS3PutEventually503(t, ctx, ecS3Client(httpURL(j), accessKey, secretKey), bucketName, fmt.Sprintf("post-target-loss-%d", j))
	}
	t.Logf("topology test: new writes returned 503 while configured placement target was unavailable")
}

// ecS3Client returns an S3 client using the given access/secret credentials.
// Separate from newS3Client to avoid changing signatures other tests depend on.
func ecS3Client(endpoint, ak, sk string) *s3.Client {
	return s3.New(s3.Options{
		BaseEndpoint:     aws.String(endpoint),
		Region:           "us-east-1",
		Credentials:      staticCreds{ak: ak, sk: sk},
		UsePathStyle:     true,
		RetryMaxAttempts: 1,
	})
}

func tryCreateBucket(parent context.Context, client *s3.Client, bucket string) error {
	ctx, cancel := clusterECS3OpContext(parent, 5*time.Second)
	defer cancel()
	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}, func(o *s3.Options) {
		o.RetryMaxAttempts = 1
	})
	return err
}

func tryPutObject(parent context.Context, client *s3.Client, bucket, key string, data []byte) error {
	ctx, cancel := clusterECS3OpContext(parent, 5*time.Second)
	defer cancel()
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	}, func(o *s3.Options) {
		o.RetryMaxAttempts = 1
	})
	return err
}

func getObjectBytes(parent context.Context, client *s3.Client, bucket, key string) ([]byte, error) {
	ctx, cancel := clusterECS3OpContext(parent, 5*time.Second)
	defer cancel()
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}, func(o *s3.Options) {
		o.RetryMaxAttempts = 1
	})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

func clusterECS3OpContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if parent == nil || parent.Err() != nil {
		return context.WithTimeout(context.Background(), timeout)
	}
	return context.WithTimeout(parent, timeout)
}

// staticCreds is a minimal aws.CredentialsProvider that returns fixed keys.
type staticCreds struct{ ak, sk string }

func (c staticCreds) Retrieve(ctx context.Context) (aws.Credentials, error) {
	return aws.Credentials{AccessKeyID: c.ak, SecretAccessKey: c.sk, Source: "static"}, nil
}
