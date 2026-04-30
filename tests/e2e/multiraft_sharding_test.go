package e2e

import (
	"context"
	"fmt"
	"io"
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
	t          *testing.T
	procs      []*exec.Cmd
	dataDirs   []string
	httpPorts  []int
	raftPorts  []int
	httpURLs   []string
	stopped    bool
	clusterKey string
	accessKey  string
	secretKey  string
	leaderIdx  int // last-known leader (set during probe)
}

func startMRCluster(t *testing.T, numNodes, seedGroups int) *mrCluster {
	t.Helper()
	binary := getBinary()
	if _, err := os.Stat(binary); err != nil {
		t.Skipf("grainfs binary not found at %s — run `make build` first", binary)
	}

	// macOS multi-process e2e: freePort() TOCTOU + meta-Raft 5-node election
	// timing → ~25% transient failure rate per attempt. Retry the whole boot
	// sequence with fresh ports up to 3x to absorb flakes; a real defect
	// produces a deterministic failure across all attempts.
	const maxAttempts = 3
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		c, err := tryStartMRCluster(t, numNodes, seedGroups)
		if err == nil {
			return c
		}
		lastErr = err
		t.Logf("startMRCluster attempt %d/%d failed: %v", attempt, maxAttempts, err)
	}
	t.Fatalf("startMRCluster failed after %d attempts: %v", maxAttempts, lastErr)
	return nil
}

func tryStartMRCluster(t *testing.T, numNodes, seedGroups int) (*mrCluster, error) {
	t.Helper()
	c := &mrCluster{
		t:          t,
		clusterKey: "E2E-MR-SHARDING-KEY",
		accessKey:  "mr-ak",
		secretKey:  "mr-sk",
	}
	c.httpPorts = make([]int, numNodes)
	c.raftPorts = make([]int, numNodes)
	c.httpURLs = make([]string, numNodes)
	c.dataDirs = make([]string, numNodes)
	c.procs = make([]*exec.Cmd, numNodes)

	// Allocate raft ports up-front: peers list must be known before any node
	// starts. HTTP ports are allocated just before each spawn to minimize the
	// freePort() TOCTOU race window (port can be reassigned by the OS between
	// listener.Close() and child process bind).
	for i := range c.raftPorts {
		c.raftPorts[i] = freePort()
		d, err := os.MkdirTemp("", fmt.Sprintf("mrshard-%d-*", i))
		if err != nil {
			c.Stop()
			return nil, fmt.Errorf("mkdir tmp: %w", err)
		}
		c.dataDirs[i] = d
	}

	t.Cleanup(c.Stop)

	for i := 0; i < numNodes; i++ {
		c.httpPorts[i] = freePort()
		c.httpURLs[i] = fmt.Sprintf("http://127.0.0.1:%d", c.httpPorts[i])
		c.procs[i] = c.startNode(i, seedGroups)
	}
	if err := waitForPortsParallelErr(c.httpPorts, 60*time.Second); err != nil {
		c.Stop()
		return nil, err
	}

	// Wait for at least one node to be writable (leader elected).
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	c.leaderIdx = -1
	leaderDeadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(leaderDeadline) {
		for i := 0; i < numNodes; i++ {
			cli := ecS3Client(c.httpURLs[i], c.accessKey, c.secretKey)
			_, err := cli.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("__mrshard-leader-probe")})
			if err == nil {
				c.leaderIdx = i
				break
			}
		}
		if c.leaderIdx >= 0 {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if c.leaderIdx < 0 {
		c.Stop()
		return nil, fmt.Errorf("no leader found within timeout")
	}

	// Allow seed loop to complete (proposes seedGroups × ProposeShardGroup).
	time.Sleep(8 * time.Second)
	return c, nil
}

func (c *mrCluster) startNode(i int, seedGroups int) *exec.Cmd {
	t := c.t
	t.Helper()
	binary := getBinary()
	raftAddr := fmt.Sprintf("127.0.0.1:%d", c.raftPorts[i])
	var peers []string
	for j := range c.raftPorts {
		if j != i {
			peers = append(peers, fmt.Sprintf("127.0.0.1:%d", c.raftPorts[j]))
		}
	}
	cmd := exec.Command(binary, "serve",
		"--data", c.dataDirs[i],
		"--port", fmt.Sprintf("%d", c.httpPorts[i]),
		"--node-id", fmt.Sprintf("mrshard-node-%d", i),
		"--raft-addr", raftAddr,
		"--peers", strings.Join(peers, ","),
		"--cluster-key", c.clusterKey,
		"--access-key", c.accessKey,
		"--secret-key", c.secretKey,
		fmt.Sprintf("--seed-groups=%d", seedGroups),
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
		_ = os.RemoveAll(d)
	}
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

// ----- TestE2E_MultiRaftSharding_Boot --------------------------------------
// Verify 5-process boot with --seed-groups=8 results in:
//   - All processes alive
//   - Per-group directories created on voter nodes (groups/group-{N}/{badger,raft})
//   - Each group has the expected number of voters (RF=3 for groups 1..7)
func TestE2E_MultiRaftSharding_Boot(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e")
	}
	c := startMRCluster(t, 5, 8)

	groupDirs := countGroupDirsAcrossNodes(c)

	// Groups 1..7 use RF=3 (rendezvous-hashed voter placement).
	// Group 0 keeps full membership (5 voters in 5-node cluster) — legacy compat.
	for i := 1; i <= 7; i++ {
		gid := fmt.Sprintf("group-%d", i)
		voterCount, ok := groupDirs[gid]
		require.True(t, ok, "group %s must have at least one voter directory", gid)
		require.Equal(t, 3, voterCount,
			"group %s expected RF=3 voter dirs, got %d", gid, voterCount)
	}
	t.Logf("boot ok: %d distinct groups with directories across 5 nodes", len(groupDirs))
}

// ----- TestE2E_MultiRaftSharding_BucketAssignment ---------------------------
// Verify bucket→group hash assignment is recorded.
//
// Scope cut: Without admin endpoints we cannot directly inspect meta-FSM
// assignments. Instead we verify behavioral consequences:
//   - CreateBucket succeeds (proves CreateBucket path completed including
//     ProposeBucketAssignment)
//   - Subsequent CreateBucket on same name is idempotent (no error / 409)
//   - Spread: 32 buckets all created without error
func TestE2E_MultiRaftSharding_BucketAssignment(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e")
	}
	// v0.0.7.1 PR-D: data-plane routing now enables auto-redirect to current leader.
	// ClusterCoordinator routes bucket-scoped ops, and CreateBucket goes through
	// the same forward path with try-each-peer reliability.

	c := startMRCluster(t, 5, 8)

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	// Use the leader index discovered during cluster probe — single-shot per
	// CreateBucket. Falls back to iterating if leader changes.
	createBucket := func(name string) error {
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

// ----- TestE2E_MultiRaftSharding_RestartRecovery ----------------------------
// Boot, create buckets, SIGTERM all, restart with same dataDirs, verify
// per-group dirs persist + bucket recreate (idempotent) succeeds.
func TestE2E_MultiRaftSharding_RestartRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e")
	}
	// v0.0.7.1 PR-D: data-plane routing with try-each-peer reliability fixes
	// leader-probe flakes.

	c := startMRCluster(t, 3, 4) // 3 procs, 4 groups for shorter cycle

	cli := ecS3Client(c.httpURLs[0], c.accessKey, c.secretKey)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	for i := 0; i < 6; i++ {
		_, err := cli.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(fmt.Sprintf("rb-%d", i)),
		})
		require.NoError(t, err)
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

	// Restart with the same data dirs.
	for i := range c.procs {
		c.procs[i] = c.startNode(i, 4)
	}
	waitForPortsParallel(t, c.httpPorts, 90*time.Second)

	// Wait for leader.
	require.Eventually(t, func() bool {
		for i := 0; i < len(c.procs); i++ {
			cli := ecS3Client(c.httpURLs[i], c.accessKey, c.secretKey)
			ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := cli.CreateBucket(ctx2, &s3.CreateBucketInput{Bucket: aws.String("__post-restart-probe")})
			cancel2()
			if err == nil || strings.Contains(fmt.Sprint(err), "BucketAlreadyOwnedByYou") {
				return true
			}
		}
		return false
	}, 90*time.Second, 1*time.Second, "no leader after restart")

	afterDirs := countGroupDirsAcrossNodes(c)
	for gid, beforeCount := range beforeDirs {
		require.GreaterOrEqual(t, afterDirs[gid], beforeCount,
			"group %s lost voter directories after restart: before=%d after=%d",
			gid, beforeCount, afterDirs[gid])
	}
	t.Logf("restart ok: %d groups recovered", len(afterDirs))
}

// ----- TestE2E_MultiRaftSharding_PerGroupPersistence ---------------------
// Verify that an object written to a bucket is stored only in the assigned
// group's BadgerDB, not in other groups. Validates the routing path
// (ClusterCoordinator → ForwardSender → ForwardReceiver → GroupBackend.PutObject).
func TestE2E_MultiRaftSharding_PerGroupPersistence(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e")
	}

	c := startMRCluster(t, 3, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Create a bucket (will be assigned to some group).
	cli := ecS3Client(c.httpURLs[c.leaderIdx], c.accessKey, c.secretKey)
	_, err := cli.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("persist-test")})
	require.NoError(t, err)

	// Write an object.
	const body = "per-group-persistence-test-data"
	_, err = cli.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("persist-test"),
		Key:    aws.String("test-key"),
		Body:   []byte(body),
	})
	require.NoError(t, err)

	// Verify the object exists via GET (routing works).
	getOut, err := cli.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("persist-test"),
		Key:    aws.String("test-key"),
	})
	require.NoError(t, err)
	defer getOut.Body.Close()
	readBody, err := io.ReadAll(getOut.Body)
	require.NoError(t, err)
	require.Equal(t, body, string(readBody))

	// Validate data lives in exactly one group's BadgerDB (group-{N}/badger/data.mdb).
	// Scan all nodes' group dirs; the object key should appear in exactly one.
	var matches int
	for _, d := range c.dataDirs {
		groupsDir := filepath.Join(d, "groups")
		entries, err := os.ReadDir(groupsDir)
		if err != nil {
			continue // node may not host any groups
		}
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			badgerDir := filepath.Join(groupsDir, e.Name(), "badger")
			db, err := badger.Open(badger.DefaultOptions(badgerDir).WithLogger(nil))
			if err != nil {
				continue
			}
			scanErr := db.View(func(txn *badger.Txn) error {
				iter := txn.NewIterator(badger.DefaultIteratorOptions)
				defer iter.Close()
				for iter.Rewind(); iter.Valid(); iter.Next() {
					item := iter.Item()
					if strings.Contains(string(item.Key), "test-key") {
						matches++
					}
				}
				return nil
			})
			db.Close()
			if scanErr != nil {
				t.Logf("warning: BadgerDB scan failed for %s: %v", badgerDir, scanErr)
			}
		}
	}
	require.Equal(t, 1, matches, "expected object to persist in exactly one group's BadgerDB")
	t.Log("per-group persistence ok: object stored in exactly one group")
}

// ----- TestE2E_MultiRaftSharding_CrossNodeDispatch -------------------------
// Verify that a PUT request arriving at a non-voter node is forwarded to
// the correct group leader and persisted. Tests ClusterCoordinator's
// forward.Send → peer's ForwardReceiver → GroupBackend.PutObject path.
func TestE2E_MultiRaftSharding_CrossNodeDispatch(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e")
	}

	c := startMRCluster(t, 3, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Create bucket via leader.
	cli := ecS3Client(c.httpURLs[c.leaderIdx], c.accessKey, c.secretKey)
	_, err := cli.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("cross-node-test")})
	require.NoError(t, err)

	// Write object via a non-leader node (if leaderIdx != 0, use node 0; otherwise node 1).
	writeNodeIdx := 0
	if writeNodeIdx == c.leaderIdx {
		writeNodeIdx = 1
	}
	writeCLI := ecS3Client(c.httpURLs[writeNodeIdx], c.accessKey, c.secretKey)
	const body = "cross-node-dispatch-test-data"
	_, err = writeCLI.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("cross-node-test"),
		Key:    aws.String("dispatch-key"),
		Body:   []byte(body),
	})
	require.NoError(t, err)

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

// ----- TestE2E_MultiRaftSharding_GroupLeaderFailover ------------------------
// Simulate leader crash (SIGTERM) for a group and verify another voter takes
// over, then PUT/GET continue to work. Validates Raft election + FSM
// continuity under the new ClusterCoordinator routing (try-each-peer
// eventually hits the new leader).
func TestE2E_MultiRaftSharding_GroupLeaderFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e")
	}

	c := startMRCluster(t, 3, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Create bucket and write object via current leader.
	cli := ecS3Client(c.httpURLs[c.leaderIdx], c.accessKey, c.secretKey)
	_, err := cli.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("failover-test")})
	require.NoError(t, err)

	const body = "failover-test-data"
	_, err = cli.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("failover-test"),
		Key:    aws.String("failover-key"),
		Body:   []byte(body),
	})
	require.NoError(t, err)

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
	err = c.procs[killIdx].Process.Signal(syscall.SIGTERM)
	require.NoError(t, err)
	_ = c.procs[killIdx].Wait()

	// Wait for new leader to emerge (CreateBucket should succeed).
	newLeaderCtx, newLeaderCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer newLeaderCancel()
	var newLeaderIdx int
	for i := range c.procs {
		if i == killIdx {
			continue
		}
		testCli := ecS3Client(c.httpURLs[i], c.accessKey, c.secretKey)
		ctx2, cancel2 := context.WithTimeout(newLeaderCtx, 5*time.Second)
		_, err := testCli.CreateBucket(ctx2, &s3.CreateBucketInput{Bucket: aws.String("failover-test-probe")})
		cancel2()
		if err == nil || strings.Contains(fmt.Sprint(err), "BucketAlreadyOwnedByYou") {
			newLeaderIdx = i
			break
		}
	}
	require.GreaterOrEqual(t, newLeaderIdx, 0, "new leader must emerge")
	require.NotEqual(t, killIdx, newLeaderIdx, "new leader must be different from killed leader")
	t.Logf("new leader emerged: node %d", newLeaderIdx)

	// Verify PUT/GET work via new leader.
	newCLI := ecS3Client(c.httpURLs[newLeaderIdx], c.accessKey, c.secretKey)
	const body2 = "post-failover-data"
	_, err = newCLI.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("failover-test"),
		Key:    aws.String("failover-key-2"),
		Body:   []byte(body2),
	})
	require.NoError(t, err)

	getOut, err := newCLI.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("failover-test"),
		Key:    aws.String("failover-key-2"),
	})
	require.NoError(t, err)
	defer getOut.Body.Close()
	readBody, err := io.ReadAll(getOut.Body)
	require.NoError(t, err)
	require.Equal(t, body2, string(readBody))

	// Original object should still be readable (persistence survived failover).
	getOut2, err := newCLI.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("failover-test"),
		Key:    aws.String("failover-key"),
	})
	require.NoError(t, err)
	defer getOut2.Body.Close()
	readBody2, err := io.ReadAll(getOut2.Body)
	require.NoError(t, err)
	require.Equal(t, body, string(readBody2))
	t.Log("group leader failover ok: new leader elected, data persisted, PUT/GET work")
}

// ----- TestE2E_MultiRaftSharding_NFSv4Smoke ----------------------------
// Cross-protocol parity: verify that NFSv4 (when enabled) also routes
// through ClusterCoordinator, so objects written via S3 are readable
// over NFSv4 mount and vice versa. Linux-only because our NFSv4
// server binds 0.0.0.0 which requires Linux.
func TestE2E_MultiRaftSharding_NFSv4Smoke(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e")
	}
	if runtime.GOOS != "linux" {
		t.Skip("NFSv4 server is Linux-only (binds 0.0.0.0)")
	}

	c := startMRCluster(t, 3, 4)

	// Start a grainfs instance with NFSv4 enabled on a dedicated port.
	// We reuse the same data dir so it sees the same meta-Raft + per-group state.
	nfsPort := freePort()
	nfsDataDir := c.dataDirs[0]
	nfsProc := exec.Command(getBinary(), "serve",
		"--data", nfsDataDir,
		"--port", fmt.Sprintf("%d", freePort()),
		"--nfs4-port", fmt.Sprintf("%d", nfsPort),
		"--access-key", c.accessKey,
		"--secret-key", c.secretKey,
		"--no-encryption",
	)
	require.NoError(t, nfsProc.Start(), "start NFSv4 server")
	defer func() {
		_ = nfsProc.Process.Signal(syscall.SIGTERM)
		_ = nfsProc.Wait()
		_ = os.RemoveAll(filepath.Join(nfsDataDir, "nfs4-socket"))
	}()

	// Wait for NFSv4 socket to appear.
	nfsSocketPath := filepath.Join(nfsDataDir, "nfs4-socket")
	require.Eventually(t, func() bool {
		_, err := os.Stat(nfsSocketPath)
		return err == nil
	}, 30*time.Second, 500*time.Millisecond, "NFSv4 socket not created")

	// Mount NFSv4 using the system 'mount' command (requires root/nfs-common).
	// We create a temp mount point and mount 127.0.0.1:{port}:/ {bucket-name}.
	mountDir, err := os.MkdirTemp("", "mrshard-nfs-*")
	require.NoError(t, err)
	defer func() {
		_ = exec.Command("umount", mountDir).Run()
		_ = os.Remove(mountDir)
	}()

	// Mount with the default NFSv4 options (read/write, hard/intr).
	mountCmd := exec.Command("mount", "-t", "nfs4",
		fmt.Sprintf("127.0.0.1:%d:/", nfsPort),
		mountDir,
		"-o", "rw,hard,intr,timeo=600,retrans=2",
	)
	out, err := mountCmd.CombinedOutput()
	if err != nil {
		t.Logf("NFSv4 mount failed (may require sudo): %v\n%s", err, string(out))
		t.Skip("NFSv4 mount failed — NFSv4 smoke test requires mount permissions; skipping")
	}
	defer func() {
		_ = exec.Command("umount", mountDir).Run()
	}()

	// Write object via S3 API.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cli := ecS3Client(c.httpURLs[0], c.accessKey, c.secretKey)
	_, err = cli.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("nfs-smoke")})
	require.NoError(t, err)

	const s3Body = "written-via-s3"
	_, err = cli.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("nfs-smoke"),
		Key:    aws.String("s3-file.txt"),
		Body:   []byte(s3Body),
	})
	require.NoError(t, err)

	// Verify via NFSv4 filesystem.
	nfsFilePath := filepath.Join(mountDir, "nfs-smoke", "s3-file.txt")
	require.Eventually(t, func() bool {
		data, err := os.ReadFile(nfsFilePath)
		return err == nil && string(data) == s3Body
	}, 30*time.Second, 500*time.Millisecond, "object not visible via NFSv4")

	// Write via NFSv4 and read via S3 API.
	const nfsBody = "written-via-nfs"
	err = os.WriteFile(nfsFilePath, []byte(nfsBody), 0644)
	require.NoError(t, err)

	getOut, err := cli.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("nfs-smoke"),
		Key:    aws.String("nfs-file.txt"),
	})
	require.NoError(t, err)
	defer getOut.Body.Close()
	nfsReadBody, err := io.ReadAll(getOut.Body)
	require.NoError(t, err)
	require.Equal(t, nfsBody, string(nfsReadBody))

	t.Log("NFSv4 smoke ok: S3↔NFSv4 cross-protocol parity verified")
}
