package e2e

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/clusteradmin"
	"github.com/stretchr/testify/require"
)

// multiraft_sharding_test.go — e2e validation for live multi-raft sharding
// (v0.0.7.0). Scope:
//
//   ✓ per-group raft.Node + BadgerDB instantiation on owned voters
//   ✓ bucket→group hash assignment recorded in meta-Raft
//   ✓ group leader elections + restart recovery
//
// Out of scope (deferred to v0.0.7.1):
//   ✗ data-plane routing — PUT/GET still goes to legacy shared distBackend.
//     PerGroupPersistence and CrossNodeDispatch tests covered there.

type mrCluster struct {
	t             *testing.T
	procs         []*exec.Cmd
	dataDirs      []string
	httpPorts     []int
	raftPorts     []int
	nfs4Ports     []int
	nbdPorts      []int
	httpURLs      []string
	stopped       bool
	clusterKey    string
	encKeyFile    string
	accessKey     string
	secretKey     string
	saID          string
	wildcardAdmin bool
	leaderIdx     int      // last-known leader (set during probe)
	nodeCount     int      // number of currently running nodes (used by addNode)
	extraArgs     []string // extra serve flags from mrClusterOptions.ExtraArgs
}

type mrClusterOptions struct {
	disableNFS4   bool
	disableNBD    bool
	FastBootstrap bool     // replace time.Sleep(8s) with shard-group polling
	MaxNodes      int      // pre-allocate ports for up to MaxNodes (for addNode); 0 = numNodes
	ExtraArgs     []string // extra flags appended to each node's serve command
}

func startStaticMRCluster(t *testing.T, numNodes int) *mrCluster {
	t.Helper()
	return startStaticMRClusterWithOptions(t, numNodes, mrClusterOptions{})
}

func startStaticMRClusterWithOptions(t *testing.T, numNodes int, opts mrClusterOptions) *mrCluster {
	t.Helper()
	binary := getBinary()
	if _, err := os.Stat(binary); err != nil {
	}

	// Legacy static-peer multi-process e2e: freePort() TOCTOU + meta-Raft
	// election timing can still produce transient boot failures. Retry the
	// whole boot sequence with fresh ports up to 3x; a real defect produces a
	// deterministic failure across all attempts.
	const maxAttempts = 3
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		c, err := tryStartStaticMRCluster(t, numNodes, opts)
		if err == nil {
			return c
		}
		lastErr = err
		t.Logf("startStaticMRCluster attempt %d/%d failed: %v", attempt, maxAttempts, err)
	}
	require.Failf(t, "startStaticMRCluster failed", "failed after %d attempts: %v", maxAttempts, lastErr)
	return nil
}

// newMRCluster allocates maxNodes port slots and temp dirs for a multi-raft
// cluster. It does NOT start any processes. Callers (tryStartStaticMRCluster,
// tryStartMRCluster) do the actual node startup.
func newMRCluster(t *testing.T, maxNodes int, opts mrClusterOptions) (*mrCluster, error) {
	t.Helper()
	c := &mrCluster{
		t:          t,
		clusterKey: "E2E-MR-SHARDING-KEY",
		encKeyFile: makeSharedEncryptionKeyFile(t),
		extraArgs:  opts.ExtraArgs,
	}
	c.httpPorts = make([]int, maxNodes)
	c.raftPorts = make([]int, maxNodes)
	c.nfs4Ports = make([]int, maxNodes)
	c.nbdPorts = make([]int, maxNodes)
	c.httpURLs = make([]string, maxNodes)
	c.dataDirs = make([]string, maxNodes)
	c.procs = make([]*exec.Cmd, maxNodes)

	ports := uniqueFreePorts(maxNodes * 4)
	for i := 0; i < maxNodes; i++ {
		c.httpPorts[i] = ports[i]
		c.raftPorts[i] = ports[maxNodes+i]
		if !opts.disableNFS4 {
			c.nfs4Ports[i] = ports[2*maxNodes+i]
		}
		if !opts.disableNBD {
			c.nbdPorts[i] = ports[3*maxNodes+i]
		}
		c.httpURLs[i] = fmt.Sprintf("http://127.0.0.1:%d", c.httpPorts[i])

		d, err := os.MkdirTemp("", fmt.Sprintf("mrshard-%d-*", i))
		if err != nil {
			c.Stop()
			return nil, fmt.Errorf("mkdir tmp node %d: %w", i, err)
		}
		c.dataDirs[i] = d
	}

	t.Cleanup(c.Stop)
	return c, nil
}

func tryStartStaticMRCluster(t *testing.T, numNodes int, opts mrClusterOptions) (*mrCluster, error) {
	t.Helper()
	c, err := newMRCluster(t, numNodes, opts)
	if err != nil {
		return nil, err
	}

	// Start node 0 as seed leader, then let followers join via .join-pending.
	c.procs[0] = c.startNode(0)
	if err := waitForPortsParallelErrWithProcesses(c.httpPorts[:1], c.procs[:1], 60*time.Second); err != nil {
		c.Stop()
		return nil, err
	}
	time.Sleep(2 * time.Second)

	// Bootstrap admin SA on the seed node before followers join.
	admin, _ := bootstrapAdminViaUDSAnyResult(c.t, c.dataDirs[:1], 60*time.Second)
	c.accessKey, c.secretKey = admin.AccessKey, admin.SecretKey
	c.saID = admin.SAID
	c.wildcardAdmin = bootstrapResultHasWildcardAdmin(admin)

	seedRaftAddr := fmt.Sprintf("127.0.0.1:%d", c.raftPorts[0])
	for i := 1; i < numNodes; i++ {
		if err := writeNodeJoinPending(c.dataDirs[i], seedRaftAddr); err != nil {
			c.Stop()
			return nil, fmt.Errorf("write join-pending node %d: %w", i, err)
		}
		c.procs[i] = c.startNode(i)
		time.Sleep(150 * time.Millisecond)
	}
	if err := waitForPortsParallelErrWithProcesses(c.httpPorts, c.procs, 60*time.Second); err != nil {
		c.Stop()
		return nil, err
	}
	time.Sleep(4 * time.Second)

	probeBucket := "__mrshard-leader-probe"
	c.GrantAdminOnBuckets(probeBucket)

	// Wait for at least one node to be writable (leader elected).
	probeCtx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	leaderIdx, err := waitForWritableEndpoint(
		probeCtx,
		c.httpURLs,
		180*time.Second,
		5*time.Second,
		1*time.Second,
		func(ctx context.Context, endpoint string) error {
			cli := ecS3Client(endpoint, c.accessKey, c.secretKey)
			return tryCreateBucket(ctx, cli, probeBucket)
		},
	)
	if err != nil {
		c.Stop()
		return nil, fmt.Errorf("no leader found within timeout: %w", err)
	}
	c.leaderIdx = leaderIdx

	// Disable auto-snapshot cluster-wide for deterministic e2e behavior.
	// PATCH is Raft-replicated so calling it on the leader's UDS suffices.
	// Tests that need auto-snapshot enable it explicitly.
	patchSnapshotInterval(c.t, c.dataDirs[c.leaderIdx], "0s")

	// Allow the automatic seed loop to complete before exercising routing.
	time.Sleep(8 * time.Second)
	return c, nil
}

// tryStartMRCluster starts numNodes nodes using dynamic sequential join:
// node 0 (seed) starts first, then nodes 1..numNodes-1 each start after the
// previous one is HTTP-ready. opts.FastBootstrap replaces the fixed 8-second
// seed-loop wait with polling on shard-group count via the admin UDS.
//
// If opts.MaxNodes > numNodes, port/slice arrays are pre-allocated to MaxNodes
// so addNode can attach more nodes later without reallocating.
func tryStartMRCluster(t *testing.T, numNodes int, opts mrClusterOptions) (*mrCluster, error) {
	t.Helper()
	maxNodes := opts.MaxNodes
	if maxNodes < numNodes {
		maxNodes = numNodes
	}

	c, err := newMRCluster(t, maxNodes, opts)
	if err != nil {
		return nil, err
	}

	// Start seed node (node 0).
	c.procs[0] = c.startNode(0)
	if err := waitForPortsParallelErrWithProcesses(c.httpPorts[:1], c.procs[:1], 60*time.Second); err != nil {
		c.Stop()
		return nil, fmt.Errorf("seed node not ready: %w", err)
	}
	time.Sleep(2 * time.Second)

	// Bootstrap admin SA on the seed before any followers join.
	admin, _ := bootstrapAdminViaUDSAnyResult(c.t, c.dataDirs[:1], 60*time.Second)
	c.accessKey, c.secretKey = admin.AccessKey, admin.SecretKey
	c.saID = admin.SAID
	c.wildcardAdmin = bootstrapResultHasWildcardAdmin(admin)
	c.nodeCount = 1

	// Start followers sequentially: wait for each before starting next.
	seedRaftAddr := fmt.Sprintf("127.0.0.1:%d", c.raftPorts[0])
	for i := 1; i < numNodes; i++ {
		if err := writeNodeJoinPending(c.dataDirs[i], seedRaftAddr); err != nil {
			c.Stop()
			return nil, fmt.Errorf("write join-pending node %d: %w", i, err)
		}
		c.procs[i] = c.startNode(i)
		if err := waitForPortsParallelErrWithProcesses(c.httpPorts[i:i+1], c.procs[i:i+1], 90*time.Second); err != nil {
			c.Stop()
			return nil, fmt.Errorf("node %d not ready: %w", i, err)
		}
		c.nodeCount++
	}

	// Wait for leader.
	probeBucket := "__mrshard-dyn-leader-probe"
	c.GrantAdminOnBuckets(probeBucket)
	probeCtx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	leaderIdx, err := waitForWritableEndpoint(
		probeCtx,
		c.httpURLs[:numNodes],
		180*time.Second,
		5*time.Second,
		time.Second,
		func(ctx context.Context, endpoint string) error {
			cli := ecS3Client(endpoint, c.accessKey, c.secretKey)
			return tryCreateBucket(ctx, cli, probeBucket)
		},
	)
	if err != nil {
		c.Stop()
		return nil, fmt.Errorf("no leader found: %w", err)
	}
	c.leaderIdx = leaderIdx

	patchSnapshotInterval(c.t, c.dataDirs[c.leaderIdx], "0s")

	// Wait for seed loop: dynamic (polling) or static (sleep).
	// seedGroupCountForClusterSize(n) = max(n*4, 8).
	expectedGroups := numNodes * 4
	if expectedGroups < 8 {
		expectedGroups = 8
	}
	if opts.FastBootstrap {
		waitForShardGroupCount(t, c.dataDirs[c.leaderIdx], expectedGroups, 60*time.Second)
	} else {
		time.Sleep(8 * time.Second)
	}

	return c, nil
}

// startMRCluster retries tryStartMRCluster up to 3 times with fresh ports on
// transient port-allocation or election-timing failures.
func startMRCluster(t *testing.T, numNodes int, opts mrClusterOptions) *mrCluster {
	t.Helper()
	const maxAttempts = 3
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		c, err := tryStartMRCluster(t, numNodes, opts)
		if err == nil {
			return c
		}
		lastErr = err
		t.Logf("startMRCluster attempt %d/%d failed: %v", attempt, maxAttempts, err)
	}
	require.Failf(t, "startMRCluster failed", "failed after %d attempts: %v", maxAttempts, lastErr)
	return nil
}

func (c *mrCluster) startNode(i int) *exec.Cmd {
	t := c.t
	t.Helper()
	binary := getBinary()
	raftAddr := fmt.Sprintf("127.0.0.1:%d", c.raftPorts[i])
	logFile, err := os.CreateTemp("", fmt.Sprintf("mrshard-node-%d-*.log", i))
	require.NoError(t, err, "create multi-raft node log file")
	t.Cleanup(func() {
		_ = logFile.Close()
		if t.Failed() && keepE2EArtifacts() {
			t.Logf("multi-raft node %d log saved to %s", i, logFile.Name())
		} else {
			_ = os.Remove(logFile.Name())
		}
	})
	args := []string{
		"serve",
		"--data", c.dataDirs[i],
		"--port", fmt.Sprintf("%d", c.httpPorts[i]),
		"--node-id", raftAddr,
		"--raft-addr", raftAddr,
		"--cluster-key", c.clusterKey,
		"--encryption-key-file", c.encKeyFile,
		"--nfs4-port", fmt.Sprintf("%d", c.nfs4Ports[i]),
		"--nbd-port", fmt.Sprintf("%d", c.nbdPorts[i]),
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
	}
	args = append(args, c.extraArgs...)
	cmd := exec.Command(binary, args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	require.NoError(t, cmd.Start(), "start node %d", i)
	return cmd
}

func (c *mrCluster) Stop() {
	if c.stopped {
		return
	}
	c.stopped = true
	for _, p := range c.procs {
		if p != nil && p.Process != nil {
			_ = p.Process.Signal(syscall.SIGTERM)
		}
	}
	deadline := time.Now().Add(10 * time.Second)
	for _, p := range c.procs {
		if p == nil || p.Process == nil {
			continue
		}
		done := make(chan error, 1)
		go func(p *exec.Cmd) { done <- p.Wait() }(p)
		select {
		case <-done:
		case <-time.After(time.Until(deadline)):
			_ = p.Process.Kill()
			<-done
		}
	}
	for _, d := range c.dataDirs {
		if c.t.Failed() && keepE2EArtifacts() {
			c.t.Logf("multi-raft data dir saved to %s", d)
			continue
		}
		_ = os.RemoveAll(d)
	}
}

func (c *mrCluster) GrantAdminOnBuckets(buckets ...string) {
	c.t.Helper()
	if c.wildcardAdmin {
		return
	}
	if c.saID == "" {
		c.t.Fatalf("cannot grant bucket admin without bootstrap sa_id")
	}
	grantAdminOnBucketsViaUDSAny(c.t, c.dataDirs, c.saID, buckets, 60*time.Second)
}

// addNode starts the next pre-allocated node slot and writes .join-pending so
// it boots directly in join mode. Requires startMRCluster to have been called
// with MaxNodes > current nodeCount. Blocks until the node is HTTP-ready, then
// updates c.leaderIdx by probing the seed node's admin UDS.
func (c *mrCluster) addNode(t *testing.T) {
	t.Helper()
	i := c.nodeCount
	if i >= len(c.procs) {
		t.Fatalf("addNode: nodeCount %d exceeds pre-allocated MaxNodes %d", i, len(c.procs))
	}
	seedRaftAddr := fmt.Sprintf("127.0.0.1:%d", c.raftPorts[0])
	require.NoError(t, writeNodeJoinPending(c.dataDirs[i], seedRaftAddr),
		"addNode: write join-pending node %d", i)
	c.procs[i] = c.startNode(i)
	require.NoError(t,
		waitForPortsParallelErrWithProcesses(c.httpPorts[i:i+1], c.procs[i:i+1], 90*time.Second),
		"addNode: node %d not ready", i)
	c.nodeCount++

	// Update leaderIdx by querying the seed node's admin UDS.
	// Status.LeaderID equals the leader's raftAddr ("127.0.0.1:<raftPort>")
	// because MetaRaftConfig.RaftID = state.raftAddr (see boot_phases_raft.go).
	sock := filepath.Join(c.dataDirs[0], "admin.sock")
	cli := clusteradmin.NewClient(sock)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if status, err := cli.Status(ctx); err == nil && status.LeaderID != "" {
		t.Logf("addNode: leaderID=%q (nodeCount=%d)", status.LeaderID, c.nodeCount)
		for j := 0; j < c.nodeCount; j++ {
			if fmt.Sprintf("127.0.0.1:%d", c.raftPorts[j]) == status.LeaderID {
				c.leaderIdx = j
				break
			}
		}
	} else {
		t.Logf("addNode: Status() unavailable (err=%v), leaderIdx unchanged", err)
	}
}

// liveURLs returns the slice of HTTP URLs for nodes that are currently running.
// When nodeCount == 0 (static clusters set all nodes before setting nodeCount),
// it falls back to the full httpURLs slice.
func (c *mrCluster) liveURLs() []string {
	if c.nodeCount > 0 {
		return c.httpURLs[:c.nodeCount]
	}
	return c.httpURLs
}

// countGroupDirsAcrossNodes returns the union of group_id directories that
// exist under any node's {dataDir}/groups/. Used to verify per-group BadgerDB
// + raft were instantiated.
func countGroupDirsAcrossNodes(c *mrCluster) map[string]int {
	out := make(map[string]int) // group_id → number of nodes hosting it
	for _, d := range c.dataDirs {
		entries, err := os.ReadDir(filepath.Join(d, "groups"))
		if err != nil {
			continue // node may not be a voter for any group
		}
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			out[e.Name()]++
		}
	}
	return out
}

// waitForShardGroupCount polls GET /v1/cluster/status via the admin UDS on
// dataDir until at least minGroups shard groups are present or timeout.
func waitForShardGroupCount(t *testing.T, dataDir string, minGroups int, timeout time.Duration) {
	t.Helper()
	sock := filepath.Join(dataDir, "admin.sock")
	cli := clusteradmin.NewClient(sock)
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		status, err := cli.Status(ctx)
		if err != nil {
			return false
		}
		return len(status.ShardGroups) >= minGroups
	}, timeout, time.Second, "expected >= %d shard groups in %s", minGroups, dataDir)
}

// ----- TestMultiRaftShardingBootE2E --------------------------------------
// Verify 5-process boot with automatic seed groups results in:
//   - All processes alive
//   - Per-group directories created on voter nodes (groups/group-{N}/{badger,raft})
//   - Each group has the expected number of voters for the auto EC profile
func TestMultiRaftShardingBootE2E(t *testing.T) {
	c := startStaticMRClusterWithOptions(t, 5, mrClusterOptions{
		disableNFS4: true,
		disableNBD:  true,
	})

	groupDirs := countGroupDirsAcrossNodes(c)

	// The helper boots a seed node first, then joins the remaining nodes.
	// Join handling does not rewrite already-created shard groups, so the seed
	// groups only need to exist. Groups added after the cluster reaches five
	// nodes must use the auto EC width from the cluster size at creation time.
	for i := 1; i <= 7; i++ {
		gid := fmt.Sprintf("group-%d", i)
		require.NotZero(t, groupDirs[gid], "seed group %s must have at least one voter directory", gid)
	}
	for i := 8; i < 20; i++ {
		gid := fmt.Sprintf("group-%d", i)
		creationClusterSize := i/4 + 1
		wantVoters := cluster.AutoECConfigForClusterSize(creationClusterSize).NumShards()
		voterCount := groupDirs[gid]
		require.Equal(t, wantVoters, voterCount,
			"group %s expected %d voter dirs, got %d", gid, wantVoters, voterCount)
	}
	t.Logf("boot ok: %d distinct groups with directories across 5 nodes", len(groupDirs))
}

// ----- TestMultiRaftShardingAllNodeServicesE2E ---------------------------
// Every cluster process must expose its node-local services. S3 writes are
// cluster-wide and may forward to the current leader; NFSv4/NBD are TCP
// listeners local to each process.
func TestMultiRaftShardingAllNodeServicesE2E(t *testing.T) {
	c := startStaticMRCluster(t, 3)

	waitForPortsParallel(t, c.httpPorts, 10*time.Second)
	waitForPortsParallel(t, c.nfs4Ports, 45*time.Second)
	waitForPortsParallel(t, c.nbdPorts, 45*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	for i, endpoint := range c.httpURLs {
		client := ecS3Client(endpoint, c.accessKey, c.secretKey)
		bucket := fmt.Sprintf("all-node-s3-%d", i)
		c.GrantAdminOnBuckets(bucket)
		var lastErr error
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			lastErr = tryCreateBucket(ctx, client, bucket)
			if lastErr == nil {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		require.NoErrorf(t, lastErr, "S3 CreateBucket should work through node %d at %s", i, endpoint)
	}
}

// ----- TestMultiRaftShardingBucketAssignmentE2E ---------------------------
// Verify bucket→group hash assignment is recorded.
//
// Scope cut: Without admin endpoints we cannot directly inspect meta-FSM
// assignments. Instead we verify behavioral consequences:
//   - CreateBucket succeeds (proves CreateBucket path completed including
//     ProposeBucketAssignment)
//   - Subsequent CreateBucket on same name is idempotent (no error / 409)
//   - Spread: 32 buckets all created without error
func TestMultiRaftShardingBucketAssignmentE2E(t *testing.T) {
	// v0.0.7.1 PR-D: data-plane routing now enables auto-redirect to current leader.
	// ClusterCoordinator routes bucket-scoped ops, and CreateBucket goes through
	// the same forward path with try-each-peer reliability.

	c := startStaticMRCluster(t, 3)

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	// Use the leader index discovered during cluster probe — single-shot per
	// CreateBucket. Falls back to iterating if leader changes.
	createBucket := func(name string) error {
		c.GrantAdminOnBuckets(name)
		// Try the known leader first.
		tryNode := func(i int) error {
			cli := ecS3Client(c.httpURLs[i], c.accessKey, c.secretKey)
			cbCtx, cbCancel := context.WithTimeout(ctx, 5*time.Second)
			defer cbCancel()
			_, err := cli.CreateBucket(cbCtx, &s3.CreateBucketInput{Bucket: aws.String(name)})
			return err
		}
		if c.leaderIdx >= 0 {
			if err := tryNode(c.leaderIdx); err == nil {
				return nil
			}
		}
		// Leader may have moved — try all
		var lastErr error
		for i := 0; i < len(c.procs); i++ {
			if err := tryNode(i); err == nil {
				c.leaderIdx = i
				return nil
			} else {
				lastErr = err
			}
		}
		return lastErr
	}

	for i := 0; i < 32; i++ {
		require.NoErrorf(t, createBucket(fmt.Sprintf("bkt-%d", i)), "CreateBucket bkt-%d", i)
	}

	// Idempotency: re-create 5 buckets, expect either nil or AlreadyOwnedByYou
	for i := 0; i < 5; i++ {
		err := createBucket(fmt.Sprintf("bkt-%d", i))
		if err != nil {
			require.Contains(t, err.Error(), "BucketAlreadyOwnedByYou",
				"unexpected non-idempotent error on bkt-%d: %v", i, err)
		}
	}
	t.Logf("32 buckets created + 5 idempotent re-creates ok")
}

// ----- TestMultiRaftShardingRestartRecoveryE2E ----------------------------
// Boot, create buckets, SIGTERM all, restart with same dataDirs, verify
// per-group dirs persist + bucket recreate (idempotent) succeeds.
func TestMultiRaftShardingRestartRecoveryE2E(t *testing.T) {
	// v0.0.7.1 PR-D: data-plane routing with try-each-peer reliability fixes
	// leader-probe flakes.

	c := startStaticMRClusterWithOptions(t, 3, mrClusterOptions{
		disableNFS4: true,
		disableNBD:  true,
	}) // 3 procs, 2 groups keeps restart recovery focused and stable

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	for i := 0; i < 1; i++ {
		requireMRCreateBucketEventually(t, ctx, c, fmt.Sprintf("rb-%d", i))
	}

	// Capture the group directory layout before restart.
	beforeDirs := countGroupDirsAcrossNodes(c)
	require.NotEmpty(t, beforeDirs, "expected group dirs before restart")

	// SIGTERM all and wait for clean exit.
	for _, p := range c.procs {
		_ = p.Process.Signal(syscall.SIGTERM)
	}
	for _, p := range c.procs {
		_ = p.Wait()
	}

	// Restart with the same data dirs and fresh HTTP ports (avoid TCP TIME_WAIT).
	for i := range c.procs {
		c.httpPorts[i] = freePort()
		c.httpURLs[i] = fmt.Sprintf("http://127.0.0.1:%d", c.httpPorts[i])
		c.procs[i] = c.startNode(i)
	}

	// Wait for HTTP readiness and a leader together. A plain TCP port probe can
	// false-negative under full e2e load on macOS even when Hertz has already
	// logged its listener; the S3 write probe is the readiness signal this test
	// actually needs.
	probeBucket := fmt.Sprintf("__post-restart-probe-%d", time.Now().UnixNano())
	c.GrantAdminOnBuckets(probeBucket)
	probeCtx, probeCancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer probeCancel()
	leaderIdx, err := waitForWritableEndpoint(
		probeCtx,
		c.httpURLs,
		240*time.Second,
		5*time.Second,
		1*time.Second,
		func(ctx context.Context, endpoint string) error {
			cli := ecS3Client(endpoint, c.accessKey, c.secretKey)
			err := tryCreateBucket(ctx, cli, probeBucket)
			if err != nil && strings.Contains(fmt.Sprint(err), "BucketAlreadyOwnedByYou") {
				return nil
			}
			return err
		},
	)
	require.NoError(t, err, "no leader after restart")
	c.leaderIdx = leaderIdx

	afterDirs := countGroupDirsAcrossNodes(c)
	for gid, beforeCount := range beforeDirs {
		require.GreaterOrEqual(t, afterDirs[gid], beforeCount,
			"group %s lost voter directories after restart: before=%d after=%d",
			gid, beforeCount, afterDirs[gid])
	}
	t.Logf("restart ok: %d groups recovered", len(afterDirs))
}

// ----- TestMultiRaftShardingPerGroupPersistenceE2E ---------------------
// Verify that an object written through the multi-raft data plane survives a
// clean cluster restart and remains readable through routed S3 GETs.
func TestMultiRaftShardingPerGroupPersistenceE2E(t *testing.T) {

	// This test must route away from legacy group-0 so the object lands under a
	// per-group BadgerDB. "persist-group-1" hashes to group-1 when the active
	// groups are group-0 and group-1.
	const bucket = "persist-group-1"
	c := startStaticMRClusterWithOptions(t, 3, mrClusterOptions{
		disableNFS4: true,
		disableNBD:  true,
	})

	// Create a bucket (will be assigned to some group).
	createCtx, createCancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer createCancel()
	requireMRCreateBucketEventually(t, createCtx, c, bucket)

	// Write an object.
	const body = "per-group-persistence-test-data"
	writeCtx, writeCancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer writeCancel()
	requireMRPutObjectFromAnyNodeEventually(t, writeCtx, c, bucket, "test-key", []byte(body))

	readCtx, readCancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer readCancel()
	requireMRGetObjectFromAnyNodeEventually(t, readCtx, c, bucket, "test-key", []byte(body))

	// Stop processes without removing data dirs, then restart with the same
	// storage roots. t.Cleanup handles final shutdown and dir removal.
	for _, p := range c.procs {
		if p != nil && p.Process != nil {
			_ = p.Process.Signal(syscall.SIGTERM)
		}
	}
	stopDeadline := time.Now().Add(10 * time.Second)
	for _, p := range c.procs {
		if p == nil {
			continue
		}
		done := make(chan error, 1)
		go func(p *exec.Cmd) { done <- p.Wait() }(p)
		select {
		case <-done:
		case <-time.After(time.Until(stopDeadline)):
			_ = p.Process.Kill()
			<-done
		}
	}

	for i := range c.procs {
		c.httpPorts[i] = freePort()
		c.httpURLs[i] = fmt.Sprintf("http://127.0.0.1:%d", c.httpPorts[i])
		c.procs[i] = c.startNode(i)
	}

	probeBucket := fmt.Sprintf("__per-group-restart-probe-%d", time.Now().UnixNano())
	probeCtx, probeCancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer probeCancel()
	leaderIdx, err := waitForWritableEndpoint(
		probeCtx,
		c.httpURLs,
		240*time.Second,
		5*time.Second,
		1*time.Second,
		func(ctx context.Context, endpoint string) error {
			cli := ecS3Client(endpoint, c.accessKey, c.secretKey)
			err := tryCreateBucket(ctx, cli, probeBucket)
			if err != nil && strings.Contains(fmt.Sprint(err), "BucketAlreadyOwnedByYou") {
				return nil
			}
			return err
		},
	)
	require.NoError(t, err, "no leader after per-group persistence restart")
	c.leaderIdx = leaderIdx

	restartCtx, restartCancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer restartCancel()
	requireMRGetObjectFromAnyNodeEventually(t, restartCtx, c, bucket, "test-key", []byte(body))
	t.Log("per-group persistence ok: object survived restart and routed GET returned committed payload")
}

func requireMRPutObjectEventually(t *testing.T, ctx context.Context, client *s3.Client, bucket, key string, data []byte) {
	t.Helper()
	var lastErr error
	var got []byte
	var headSize int64 = -1
	var versionID string
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		versionID, lastErr = tryPutObjectVersioned(ctx, client, bucket, key, data)
		if lastErr != nil {
			time.Sleep(2 * time.Second)
			continue
		}
		if out, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		}); err == nil && out.ContentLength != nil {
			headSize = *out.ContentLength
		}
		got, lastErr = getObjectBytes(ctx, client, bucket, key)
		if lastErr == nil && bytes.Equal(got, data) {
			return
		}
		time.Sleep(2 * time.Second)
	}
	require.Failf(t, "PutObject never became readable",
		"PutObject %s/%s never became readable with committed body: lastErr=%v versionID=%q headSize=%d gotLen=%d got=%q",
		bucket, key, lastErr, versionID, headSize, len(got), string(got),
	)
}

func requireMRPutObjectFromAnyNodeEventually(t *testing.T, ctx context.Context, c *mrCluster, bucket, key string, data []byte) {
	t.Helper()
	var lastErr error
	var got []byte
	var headSize int64 = -1
	var versionID string
	var lastNode int
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		for i, endpoint := range c.liveURLs() {
			lastNode = i
			client := ecS3Client(endpoint, c.accessKey, c.secretKey)
			versionID, lastErr = tryPutObjectVersioned(ctx, client, bucket, key, data)
			if lastErr != nil {
				continue
			}
			if out, err := client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			}); err == nil && out.ContentLength != nil {
				headSize = *out.ContentLength
			}
			got, lastErr = getObjectBytes(ctx, client, bucket, key)
			if lastErr == nil && bytes.Equal(got, data) {
				c.leaderIdx = i
				return
			}
		}
		time.Sleep(2 * time.Second)
	}
	require.Failf(t, "PutObject never became readable",
		"PutObject %s/%s never became readable with committed body: lastNode=%d lastErr=%v versionID=%q headSize=%d gotLen=%d got=%q",
		bucket, key, lastNode, lastErr, versionID, headSize, len(got), string(got),
	)
}

func tryPutObjectVersioned(parent context.Context, client *s3.Client, bucket, key string, data []byte) (string, error) {
	ctx, cancel := clusterECS3OpContext(parent, 5*time.Second)
	defer cancel()
	out, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	}, func(o *s3.Options) {
		o.RetryMaxAttempts = 1
	})
	if err != nil {
		return "", err
	}
	if out.VersionId == nil || *out.VersionId == "" {
		return "", fmt.Errorf("PutObject returned success without version id")
	}
	return *out.VersionId, nil
}

func requireMRCreateBucketEventually(t *testing.T, ctx context.Context, c *mrCluster, bucket string) {
	t.Helper()
	c.GrantAdminOnBuckets(bucket)
	var lastErr error
	tryNode := func(i int) bool {
		client := ecS3Client(c.httpURLs[i], c.accessKey, c.secretKey)
		lastErr = tryCreateBucket(ctx, client, bucket)
		if lastErr == nil || strings.Contains(fmt.Sprint(lastErr), "BucketAlreadyOwnedByYou") {
			c.leaderIdx = i
			return true
		}
		return false
	}
	require.Eventually(t, func() bool {
		if c.leaderIdx >= 0 && tryNode(c.leaderIdx) {
			return true
		}
		for i := range c.liveURLs() {
			if tryNode(i) {
				return true
			}
		}
		return false
	}, 60*time.Second, 2*time.Second, "CreateBucket %s never became writable: %v", bucket, lastErr)
}

func requireMRGetObjectEventually(t *testing.T, ctx context.Context, client *s3.Client, bucket, key string, want []byte) {
	t.Helper()
	var lastErr error
	var got []byte
	require.Eventually(t, func() bool {
		got, lastErr = getObjectBytes(ctx, client, bucket, key)
		return lastErr == nil && bytes.Equal(got, want)
	}, 60*time.Second, 2*time.Second,
		"GetObject %s/%s never returned committed object: lastErr=%v got=%q",
		bucket, key, lastErr, string(got))
}

func requireMRGetObjectFromAnyNodeEventually(t *testing.T, ctx context.Context, c *mrCluster, bucket, key string, want []byte) {
	t.Helper()
	var lastErr error
	var got []byte
	var lastNode int
	require.Eventually(t, func() bool {
		for i, endpoint := range c.liveURLs() {
			client := ecS3Client(endpoint, c.accessKey, c.secretKey)
			got, lastErr = getObjectBytes(ctx, client, bucket, key)
			lastNode = i
			if lastErr == nil && bytes.Equal(got, want) {
				c.leaderIdx = i
				return true
			}
		}
		return false
	}, 90*time.Second, 2*time.Second,
		"GetObject %s/%s never returned committed object from any node: lastNode=%d lastErr=%v got=%q",
		bucket, key, lastNode, lastErr, string(got))
}

// ----- TestMultiRaftShardingCrossNodeDispatchE2E -------------------------
// Verify that a PUT request arriving at a non-voter node is forwarded to
// the correct group leader and persisted. Tests ClusterCoordinator's
// forward.Send → peer's ForwardReceiver → GroupBackend.PutObject path.
func TestMultiRaftShardingCrossNodeDispatchE2E(t *testing.T) {

	// Cross-node routing does not require multiple seeded groups; one RF=3 group
	// is enough to exercise follower-to-leader forwarding.
	c := startStaticMRCluster(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Create bucket via leader.
	requireMRCreateBucketEventually(t, ctx, c, "cross-node-test")

	// Write object via a non-leader node (if leaderIdx != 0, use node 0; otherwise node 1).
	writeNodeIdx := 0
	if writeNodeIdx == c.leaderIdx {
		writeNodeIdx = 1
	}
	writeCLI := ecS3Client(c.httpURLs[writeNodeIdx], c.accessKey, c.secretKey)
	const body = "cross-node-dispatch-test-data"
	requireMRPutObjectEventually(t, ctx, writeCLI, "cross-node-test", "dispatch-key", []byte(body))

	// Verify object is readable via any node (routing consistency).
	readCLI := ecS3Client(c.httpURLs[0], c.accessKey, c.secretKey)
	getOut, err := readCLI.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("cross-node-test"),
		Key:    aws.String("dispatch-key"),
	})
	require.NoError(t, err)
	defer getOut.Body.Close()
	readBody, err := io.ReadAll(getOut.Body)
	require.NoError(t, err)
	require.Equal(t, body, string(readBody))
	t.Log("cross-node dispatch ok: non-voter PUT forwarded to leader and persisted")
}

func TestTopologyDurabilityFullTargetWriteGuardE2E(t *testing.T) {

	c := startStaticMRClusterWithOptions(t, 3, mrClusterOptions{disableNFS4: true, disableNBD: true})
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	const bucket = "missing-placement-target"
	requireMRCreateBucketEventually(t, ctx, c, bucket)

	leaderClient := ecS3Client(c.httpURLs[c.leaderIdx], c.accessKey, c.secretKey)
	const existingKey = "before-target-loss"
	const existingBody = "still-readable-after-one-target-loss"
	requireMRPutObjectEventually(t, ctx, leaderClient, bucket, existingKey, []byte(existingBody))

	killIdx := 0
	if killIdx == c.leaderIdx {
		killIdx = 1
	}
	forwardIdx := 0
	for forwardIdx == c.leaderIdx || forwardIdx == killIdx {
		forwardIdx++
	}
	require.NotNil(t, c.procs[killIdx], "target process must exist")
	t.Logf("killing placement target node %d at %s", killIdx, c.httpURLs[killIdx])
	require.NoError(t, c.procs[killIdx].Process.Signal(syscall.SIGTERM))
	_ = c.procs[killIdx].Wait()
	c.procs[killIdx] = nil

	readOut, err := ecS3Client(c.httpURLs[forwardIdx], c.accessKey, c.secretKey).GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(existingKey),
	})
	require.NoError(t, err, "existing object must remain readable with k live shards")
	defer readOut.Body.Close()
	readBody, err := io.ReadAll(readOut.Body)
	require.NoError(t, err)
	require.Equal(t, existingBody, string(readBody))

	requireS3PutEventually503(t, ctx, leaderClient, bucket, "leader-after-target-loss")
	requireS3PutEventually503(t, ctx, ecS3Client(c.httpURLs[forwardIdx], c.accessKey, c.secretKey), bucket, "forwarded-after-target-loss")
}

func requireS3PutEventually503(t *testing.T, ctx context.Context, client *s3.Client, bucket, key string) {
	t.Helper()
	require.Eventually(t, func() bool {
		putCtx, cancelPut := context.WithTimeout(ctx, 5*time.Second)
		defer cancelPut()
		_, err := client.PutObject(putCtx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader([]byte("should-return-503")),
		}, func(o *s3.Options) {
			o.RetryMaxAttempts = 1
		})
		if err == nil {
			return false
		}
		errStr := err.Error()
		return strings.Contains(errStr, "503") || strings.Contains(errStr, "ServiceUnavailable")
	}, 45*time.Second, time.Second, "expected missing topology placement target to surface as S3 503")
}

// ----- TestMultiRaftShardingGroupLeaderFailoverE2E ------------------------
// Simulate leader crash (SIGTERM) for a group and verify another voter takes
// over, then PUT/GET continue to work. Validates Raft election + FSM
// continuity under the new ClusterCoordinator routing (try-each-peer
// eventually hits the new leader).
func TestMultiRaftShardingGroupLeaderFailoverE2E(t *testing.T) {

	// Failover behavior is independent of group count. Use one group to keep
	// startup focused on the failover path under test.
	c := startStaticMRCluster(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Create bucket and write object via current leader.
	requireMRCreateBucketEventually(t, ctx, c, "failover-test")
	cli := ecS3Client(c.httpURLs[c.leaderIdx], c.accessKey, c.secretKey)

	const body = "failover-test-data"
	requireMRPutObjectEventually(t, ctx, cli, "failover-test", "failover-key", []byte(body))

	// Find which node hosts the leader for the assigned group (simplification:
	// we kill the leader node process; whichever group loses leader
	// should recover quickly within 1-2 election timeouts).
	// In production, ShardService/GroupBackend.RaftNode().LeaderID() would identify
	// the exact process; for e2e we rely on RF=3 so killing any voter forces
	// an election.
	killIdx := c.leaderIdx
	t.Logf("killing leader node %d to trigger group failover", killIdx)

	// SIGTERM the leader process.
	require.NotNil(t, c.procs[killIdx], "leader process must exist")
	err := c.procs[killIdx].Process.Signal(syscall.SIGTERM)
	require.NoError(t, err)
	_ = c.procs[killIdx].Wait()

	// Wait for new leader to emerge (CreateBucket should succeed).
	newLeaderCtx, newLeaderCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer newLeaderCancel()
	newLeaderIdx := -1
	var lastProbeErr error
	c.GrantAdminOnBuckets("failover-test-probe")
	deadline := time.Now().Add(55 * time.Second)
	for time.Now().Before(deadline) && newLeaderIdx == -1 {
		for i := range c.procs {
			if i == killIdx {
				continue
			}
			testCli := ecS3Client(c.httpURLs[i], c.accessKey, c.secretKey)
			ctx2, cancel2 := context.WithTimeout(newLeaderCtx, 3*time.Second)
			_, err := testCli.CreateBucket(ctx2, &s3.CreateBucketInput{Bucket: aws.String("failover-test-probe")})
			cancel2()
			if err == nil || strings.Contains(fmt.Sprint(err), "BucketAlreadyOwnedByYou") {
				newLeaderIdx = i
				break
			}
			lastProbeErr = err
			t.Logf("probe node %d: %v", i, err)
		}
		if newLeaderIdx == -1 {
			time.Sleep(500 * time.Millisecond)
		}
	}
	require.NotEqual(t, -1, newLeaderIdx, "no new leader emerged after 55s: last err=%v", lastProbeErr)
	require.NotEqual(t, killIdx, newLeaderIdx, "new leader must be different from killed leader")
	t.Logf("new leader emerged: node %d", newLeaderIdx)

	// Original object should still be readable after re-election.
	readCtx, readCancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer readCancel()
	requireMRGetObjectFromAnyNodeEventually(t, readCtx, c, "failover-test", "failover-key", []byte(body))

	// With full-target placement, losing one of three placement targets must
	// block new writes instead of silently accepting under-replicated data.
	newCLI := ecS3Client(c.httpURLs[newLeaderIdx], c.accessKey, c.secretKey)
	requireS3PutEventually503(t, readCtx, newCLI, "failover-test", "failover-key-2")
	t.Log("group leader failover ok: new leader elected, committed data readable, new writes blocked while target is missing")
}

// ----- TestMultiRaftShardingNFSv4SmokeE2E ----------------------------
// Cross-protocol parity: verify that NFSv4 routes through ClusterCoordinator,
// so objects written via S3 are readable over NFSv4 and vice versa. On Linux
// the test mounts locally; on macOS it mounts from the Colima Linux VM.
func TestMultiRaftShardingNFSv4SmokeE2E(t *testing.T) {

	c := startStaticMRCluster(t, 3)
	waitForPortsParallel(t, []int{c.nfs4Ports[0]}, 45*time.Second)

	// Write object via S3 API.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cli := ecS3Client(c.httpURLs[0], c.accessKey, c.secretKey)
	const legacyNFS4Bucket = "__grainfs_nfs4"
	c.GrantAdminOnBuckets(legacyNFS4Bucket)
	_, err := cli.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(legacyNFS4Bucket),
	})
	require.NoError(t, err)
	runNfsExportJSONOnDataDir(t, c.dataDirs[0], "add", legacyNFS4Bucket)

	const s3Body = "written-via-s3"
	_, err = cli.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(legacyNFS4Bucket),
		Key:    aws.String("s3-file.txt"),
		Body:   bytes.NewReader([]byte(s3Body)),
	})
	require.NoError(t, err)

	const nfsBody = "written-via-nfs"
	runNFSv4SmokeClient(t, c.nfs4Ports[0], legacyNFS4Bucket, s3Body, nfsBody)

	getOut, err := cli.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(legacyNFS4Bucket),
		Key:    aws.String("nfs-file.txt"),
	})
	require.NoError(t, err)
	defer getOut.Body.Close()
	nfsReadBody, err := io.ReadAll(getOut.Body)
	require.NoError(t, err)
	require.Equal(t, nfsBody, string(nfsReadBody))

	t.Log("NFSv4 smoke ok: S3↔NFSv4 cross-protocol parity verified")
}

func runNFSv4SmokeClient(t *testing.T, nfsPort int, bucket, s3Body, nfsBody string) {
	t.Helper()

	switch runtime.GOOS {
	case "linux":
		runLocalNFSv4SmokeClient(t, nfsPort, bucket, s3Body, nfsBody)
	case "darwin":
		runColimaNFSv4SmokeClient(t, nfsPort, bucket, s3Body, nfsBody)
	default:
	}
}

func runLocalNFSv4SmokeClient(t *testing.T, nfsPort int, bucket, s3Body, nfsBody string) {
	t.Helper()

	mountDir, err := os.MkdirTemp("", "mrshard-nfs-*")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = exec.Command("umount", mountDir).Run()
		_ = os.Remove(mountDir)
	})

	mountCmd := exec.Command("mount", "-t", "nfs4",
		"-o", "rw,hard,intr,timeo=600,retrans=2",
		fmt.Sprintf("127.0.0.1:%d:/", nfsPort),
		mountDir,
	)
	out, err := mountCmd.CombinedOutput()
	if err != nil {
		t.Logf("NFSv4 mount failed (may require sudo): %v\n%s", err, string(out))
	}

	nfsFilePath := filepath.Join(mountDir, bucket, "s3-file.txt")
	require.Eventually(t, func() bool {
		data, err := os.ReadFile(nfsFilePath)
		return err == nil && string(data) == s3Body
	}, 30*time.Second, 500*time.Millisecond, "object not visible via NFSv4")

	nfsNewFilePath := filepath.Join(mountDir, bucket, "nfs-file.txt")
	require.NoError(t, os.WriteFile(nfsNewFilePath, []byte(nfsBody), 0o644))
}

func runColimaNFSv4SmokeClient(t *testing.T, nfsPort int, bucket, s3Body, nfsBody string) {
	t.Helper()

	_, _ = exec.LookPath("colima")
	_, _ = exec.Command("colima", "status").CombinedOutput()

	hostIP := os.Getenv("HOST_IP")
	if hostIP == "" {
		hostIP = "192.168.5.2"
	}

	name := strings.ReplaceAll(t.Name(), "/", "-")
	mountDir := fmt.Sprintf("/mnt/grainfs-mr-nfs-%s-%d", name, time.Now().UnixNano())
	runColimaSSH(t, "sudo", "mkdir", "-p", mountDir)
	t.Cleanup(func() {
		_ = colimaSSH("sudo", "umount", "-l", mountDir).Run()
		_ = colimaSSH("sudo", "rmdir", mountDir).Run()
	})

	_, _ = colimaSSHCombinedOutput(15*time.Second, "sudo", "mount", "-t", "nfs4",
		"-o", fmt.Sprintf("vers=4.1,port=%d,rw,hard,intr,timeo=600,retrans=2", nfsPort),
		fmt.Sprintf("%s:/", hostIP),
		mountDir,
	)

	nfsFilePath := mountDir + "/" + bucket + "/s3-file.txt"
	require.Eventually(t, func() bool {
		out, err := colimaSSHCombinedOutput(2*time.Second, "sudo", "cat", nfsFilePath)
		return err == nil && string(out) == s3Body
	}, 30*time.Second, 500*time.Millisecond, "object not visible via Colima NFSv4 mount")

	nfsNewFilePath := mountDir + "/" + bucket + "/nfs-file.txt"
	runColimaSSH(t, "sudo", "bash", "-c",
		fmt.Sprintf("printf %%s %s > %s", shellQuote(nfsBody), shellQuote(nfsNewFilePath)))
}

func colimaSSH(args ...string) *exec.Cmd {
	return exec.Command("colima", append([]string{"ssh", "--"}, args...)...)
}

func colimaSSHCombinedOutput(timeout time.Duration, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return exec.CommandContext(ctx, "colima", append([]string{"ssh", "--"}, args...)...).CombinedOutput()
}

func runColimaSSH(t *testing.T, args ...string) string {
	t.Helper()
	out, err := colimaSSH(args...).CombinedOutput()
	if err != nil {
		t.Fatalf("colima ssh %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}

func TestMultiRaftShardingNBDRoutesThroughCoordinatorE2E(t *testing.T) {

	c := startE2ECluster(t, e2eClusterOptions{
		Nodes:      3,
		Mode:       ClusterModeStaticPeers,
		DisableNFS: true,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c.GrantAdminOnBuckets("__grainfs_volumes")
	require.Eventually(t, func() bool {
		err := tryCreateBucket(ctx, c.S3Client(0), "__grainfs_volumes")
		return err == nil || strings.Contains(fmt.Sprint(err), "BucketAlreadyOwnedByYou")
	}, 30*time.Second, 500*time.Millisecond, "__grainfs_volumes bucket grant did not become writable")
	ensureE2ENBDVolume(t, ctx, c, "default", 4*1024*1024)

	client := dialE2ENBD(t, fmt.Sprintf("127.0.0.1:%d", c.nbdPorts[0]), "default")
	defer client.Close()

	body := []byte("nbd-through-cluster-coordinator")
	client.WriteAt(t, 0, body)
	client.Flush(t)
	requireNBDReadEventually(t, client, 0, body)

	out, err := c.S3Client(1).ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String("__grainfs_volumes"),
		Prefix: aws.String("__vol/default/"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.Contents)
}

func TestMultiRaftShardingIcebergCatalogPointerAndMetadataObjectSplitE2E(t *testing.T) {

	c := startE2ECluster(t, e2eClusterOptions{
		Nodes:      3,
		Mode:       ClusterModeStaticPeers,
		DisableNFS: true,
		DisableNBD: true,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	c.GrantAdminOnBuckets("grainfs-tables")
	require.Eventually(t, func() bool {
		err := tryCreateBucket(ctx, c.S3Client(0), "grainfs-tables")
		return err == nil || strings.Contains(fmt.Sprint(err), "BucketAlreadyOwnedByYou")
	}, 30*time.Second, 500*time.Millisecond, "grainfs-tables bucket grant did not become writable")

	icebergClient := newIcebergSigV4Client(t, c.accessKey, c.secretKey, "us-east-1")

	nsReq, err := http.NewRequest(http.MethodPost, c.httpURLs[1]+"/iceberg/v1/namespaces", bytes.NewReader([]byte(`{"namespace":["ns"],"properties":{}}`)))
	require.NoError(t, err)
	nsReq.Header.Set("Content-Type", "application/json")
	resp, err := icebergClient.Do(nsReq)
	require.NoError(t, err)
	require.Less(t, resp.StatusCode, 300)
	require.NoError(t, resp.Body.Close())

	createTableBody := `{
		"name":"t",
		"schema":{"type":"struct","schema-id":0,"fields":[]},
		"properties":{}
	}`
	tblReq, err := http.NewRequest(http.MethodPost, c.httpURLs[1]+"/iceberg/v1/namespaces/ns/tables", bytes.NewReader([]byte(createTableBody)))
	require.NoError(t, err)
	tblReq.Header.Set("Content-Type", "application/json")
	resp, err = icebergClient.Do(tblReq)
	require.NoError(t, err)
	require.Less(t, resp.StatusCode, 300)
	require.NoError(t, resp.Body.Close())

	loadReq, err := http.NewRequest(http.MethodGet, c.httpURLs[2]+"/iceberg/v1/namespaces/ns/tables/t", nil)
	require.NoError(t, err)
	loadResp, err := icebergClient.Do(loadReq)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, loadResp.StatusCode)
	loadBody, err := io.ReadAll(loadResp.Body)
	require.NoError(t, err)
	require.NoError(t, loadResp.Body.Close())
	require.Contains(t, string(loadBody), `"metadata-location"`)
	require.Contains(t, string(loadBody), `s3://grainfs-tables/warehouse/ns/t/metadata/00000.json`)

	var got []byte
	var readErr error
	readCtx, readCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer readCancel()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		for i := range c.httpURLs {
			got, readErr = getObjectBytes(readCtx, c.S3Client(i), "grainfs-tables", "warehouse/ns/t/metadata/00000.json")
			if readErr == nil {
				break
			}
		}
		if readErr == nil {
			break
		}
		time.Sleep(time.Second)
	}
	require.NoError(t, readErr)
	require.Contains(t, string(got), `"format-version"`)
}

// TestTwoNodeAvailabilityTrapE2E verifies the well-known 2-node quorum trap:
// with 2 voters in metaRaft (and all data groups), losing one node breaks
// quorum entirely. This is intentional — 2-node clusters are functionally
// worse than single-node for availability.
func TestTwoNodeAvailabilityTrapE2E(t *testing.T) {

	c := startMRCluster(t, 2, mrClusterOptions{
		FastBootstrap: true,
		disableNFS4:   true,
		disableNBD:    true,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const bucket = "availability-trap-test"
	requireMRCreateBucketEventually(t, ctx, c, bucket)
	cli := ecS3Client(c.httpURLs[c.leaderIdx], c.accessKey, c.secretKey)

	// Write should succeed with both nodes alive.
	requireMRPutObjectEventually(t, ctx, cli, bucket, "before-kill", []byte("alive"))

	// Kill the non-leader node to break metaRaft quorum (needs 2/2).
	followerIdx := 1 - c.leaderIdx
	t.Logf("killing follower node %d to break 2-node quorum", followerIdx)
	require.NotNil(t, c.procs[followerIdx])
	require.NotNil(t, c.procs[followerIdx].Process)
	require.NoError(t, c.procs[followerIdx].Process.Signal(syscall.SIGTERM))
	_ = c.procs[followerIdx].Wait()
	c.procs[followerIdx] = nil

	// Write must now fail — quorum is lost.
	// In a 2-node raft cluster, the surviving leader blocks waiting for
	// 2/2 acknowledgment that never arrives, so requests time out with
	// context.DeadlineExceeded rather than a fast 503. A fast 503 preflight
	// ("not enough voters") would require a separate quorum-check feature.
	// This test documents the current reality (hang = trap), not a bug.
	writeCtx, writeCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer writeCancel()
	_, writeErr := cli.PutObject(writeCtx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("after-quorum-loss"),
		Body:   bytes.NewReader([]byte("blocked")),
	}, func(o *s3.Options) { o.RetryMaxAttempts = 1 })
	require.Error(t, writeErr, "expected write to fail after 2-node quorum loss (got success — split-brain?)")
}

// TestDynamicGroupSeeding1to5E2E verifies that each dynamic node join
// triggers the seed loop to expand shard groups according to
// seedGroupCountForClusterSize(n) = max(n*4, 8):
//
//	1 node  → 8 groups
//	2 nodes → 8 groups (no change)
//	3 nodes → 12 groups
//	4 nodes → 16 groups
//	5 nodes → 20 groups
//
// After each expansion, a PUT (with internal GET round-trip) is verified to
// confirm routing works.
func TestDynamicGroupSeeding1to5E2E(t *testing.T) {

	// Start with 1 node; pre-allocate ports for 5.
	c := startMRCluster(t, 1, mrClusterOptions{
		FastBootstrap: true,
		MaxNodes:      5,
		disableNFS4:   true,
		disableNBD:    true,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	const bucket = "dyn-seed-1to5"
	requireMRCreateBucketEventually(t, ctx, c, bucket)

	// Step helper: after each addNode, verify group count and PUT+GET.
	type step struct {
		afterNodes     int
		expectedGroups int
	}
	steps := []step{
		{1, 8}, // initial seed
		{2, 8}, // no new groups
		{3, 12},
		{4, 16},
		{5, 20},
	}

	// Verify step 1 (already started).
	waitForShardGroupCount(t, c.dataDirs[c.leaderIdx], steps[0].expectedGroups, 30*time.Second)
	requireMRPutObjectFromAnyNodeEventually(t, ctx, c, bucket,
		fmt.Sprintf("key-after-%d-nodes", steps[0].afterNodes), []byte("data"))

	// Steps 2–5: add a node then verify.
	for _, s := range steps[1:] {
		c.addNode(t)
		waitForShardGroupCount(t, c.dataDirs[c.leaderIdx], s.expectedGroups, 60*time.Second)
		t.Logf("nodes=%d: shard groups >= %d confirmed", s.afterNodes, s.expectedGroups)
		requireMRPutObjectFromAnyNodeEventually(t, ctx, c, bucket,
			fmt.Sprintf("key-after-%d-nodes", s.afterNodes), []byte("data"))
	}
}
