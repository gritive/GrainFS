package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

type ClusterMode string

const (
	ClusterModeDynamicJoin ClusterMode = "dynamic-join"
	ClusterModeStaticPeers ClusterMode = "static-peers"

	// joinPendingFile mirrors serveruntime.JoinPendingFile to avoid an import cycle.
	joinPendingFile = ".join-pending"
)

type e2eClusterOptions struct {
	Nodes         int
	Mode          ClusterMode
	ClusterKey    string
	AccessKey     string
	SecretKey     string
	LogPrefix     string
	ScrubInterval string
	DisableNFS    bool
	DisableNBD    bool
	EnablePprof   bool
	// NoBootstrap when true skips the bootstrapAdminViaUDSAnyResult call so
	// the cluster starts with an empty IAM store. Only supported in
	// ClusterModeDynamicJoin. bootstrap-test runners use this to drive the
	// first-SA creation themselves.
	NoBootstrap bool
	// ExtraArgs is appended verbatim to every node's `grainfs serve` cmdline.
	// Use for per-test flag tweaks (e.g. `--vlog-warn-ratio=0.001`,
	// `--badger-gc-disable=true`) so the harness stays generic.
	ExtraArgs []string
}

type e2eCluster struct {
	t             testing.TB
	mode          ClusterMode
	procs         []*exec.Cmd
	dataDirs      []string
	httpPorts     []int
	raftPorts     []int
	nfs4Ports     []int
	nbdPorts      []int
	pprofPorts    []int
	httpURLs      []string
	clusterKey    string
	encKeyFile    string
	accessKey     string
	secretKey     string
	saID          string
	wildcardAdmin bool
	noBootstrap   bool
	logPrefix     string
	scrubInterval string
	extraArgs     []string
	stopped       bool
	leaderIdx     int
}

func startE2ECluster(t testing.TB, opts e2eClusterOptions) *e2eCluster {
	t.Helper()
	c := startE2EClusterNoCleanup(t, opts)
	ginkgo.DeferCleanup(c.Stop)
	return c
}

// startE2EClusterNoCleanup is identical to startE2ECluster except it does NOT
// register t.Cleanup(c.Stop). Intended for process-global shared fixtures
// whose lifetime is managed by TestMain teardown. The caller is responsible
// for calling c.Stop() exactly once.
func startE2EClusterNoCleanup(t testing.TB, opts e2eClusterOptions) *e2eCluster {
	t.Helper()
	opts = normalizeE2EClusterOptions(opts)
	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		c, err := tryStartE2ECluster(t, opts)
		if err == nil {
			return c
		}
		lastErr = err
		t.Logf("e2e cluster start attempt %d failed: %v", attempt, err)
		time.Sleep(time.Duration(attempt) * 250 * time.Millisecond)
	}
	gomega.Expect(lastErr).NotTo(gomega.HaveOccurred())
	return nil
}

func normalizeE2EClusterOptions(opts e2eClusterOptions) e2eClusterOptions {
	rejectRemovedECExtraArgs(opts.ExtraArgs)
	if opts.Nodes == 0 {
		opts.Nodes = 2
	}
	if opts.Mode == "" {
		opts.Mode = ClusterModeDynamicJoin
	}
	if opts.ClusterKey == "" {
		opts.ClusterKey = "E2E-CLUSTER-KEY"
	}
	if opts.AccessKey == "" {
		opts.AccessKey = "e2e-ak"
	}
	if opts.SecretKey == "" {
		opts.SecretKey = "e2e-sk"
	}
	if opts.LogPrefix == "" {
		opts.LogPrefix = "grainfs-e2e-cluster"
	}
	if opts.ScrubInterval == "" {
		opts.ScrubInterval = "0"
	}
	return opts
}

func rejectRemovedECExtraArgs(args []string) {
	for _, arg := range args {
		if arg == "--ec-data" || strings.HasPrefix(arg, "--ec-data=") ||
			arg == "--ec-parity" || strings.HasPrefix(arg, "--ec-parity=") ||
			arg == "--seed-groups" || strings.HasPrefix(arg, "--seed-groups=") {
			panic(fmt.Sprintf("removed zero-config flag %q: use Nodes to select the automatic profile", arg))
		}
	}
}

func runNormalizeOptionsRejectsRemovedZeroConfigFlags(t testing.TB) {
	t.Helper()
	for _, arg := range []string{"--ec-data=2", "--ec-data", "--ec-parity=1", "--ec-parity", "--seed-groups=2", "--seed-groups"} {
		gomega.Expect(func() {
			normalizeE2EClusterOptions(e2eClusterOptions{ExtraArgs: []string{arg}})
		}).To(gomega.PanicWith(
			fmt.Sprintf("removed zero-config flag %q: use Nodes to select the automatic profile", arg),
		))
	}
}

func runNormalizeOptionsAllowsNonECExtraArgs(t testing.TB) {
	t.Helper()
	opts := normalizeE2EClusterOptions(e2eClusterOptions{
		ExtraArgs: []string{"--vlog-warn-ratio=0.001"},
	})
	gomega.Expect(opts.ExtraArgs).To(gomega.Equal([]string{"--vlog-warn-ratio=0.001"}))
}

func tryStartE2ECluster(t testing.TB, opts e2eClusterOptions) (*e2eCluster, error) {
	t.Helper()
	binary := getBinary()
	if _, err := os.Stat(binary); err != nil {
	}

	c := &e2eCluster{
		t:             t,
		mode:          opts.Mode,
		clusterKey:    opts.ClusterKey,
		encKeyFile:    makeSharedEncryptionKeyFile(t),
		accessKey:     opts.AccessKey,
		secretKey:     opts.SecretKey,
		noBootstrap:   opts.NoBootstrap,
		logPrefix:     opts.LogPrefix,
		scrubInterval: opts.ScrubInterval,
		extraArgs:     append([]string(nil), opts.ExtraArgs...),
		leaderIdx:     -1,
	}
	c.procs = make([]*exec.Cmd, opts.Nodes)
	c.dataDirs = make([]string, opts.Nodes)
	c.httpPorts = make([]int, opts.Nodes)
	c.raftPorts = make([]int, opts.Nodes)
	c.nfs4Ports = make([]int, opts.Nodes)
	c.nbdPorts = make([]int, opts.Nodes)
	c.pprofPorts = make([]int, opts.Nodes)
	c.httpURLs = make([]string, opts.Nodes)

	portCount := opts.Nodes * 4
	if opts.EnablePprof {
		portCount += opts.Nodes
	}
	ports := uniqueFreePorts(portCount)
	for i := 0; i < opts.Nodes; i++ {
		c.httpPorts[i] = ports[i]
		c.raftPorts[i] = ports[opts.Nodes+i]
		if opts.DisableNFS {
			c.nfs4Ports[i] = 0
		} else {
			c.nfs4Ports[i] = ports[2*opts.Nodes+i]
		}
		if opts.DisableNBD {
			c.nbdPorts[i] = 0
		} else {
			c.nbdPorts[i] = ports[3*opts.Nodes+i]
		}
		if opts.EnablePprof {
			c.pprofPorts[i] = ports[4*opts.Nodes+i]
		}
		c.httpURLs[i] = fmt.Sprintf("http://127.0.0.1:%d", c.httpPorts[i])
		// Use /tmp explicitly: on macOS, $TMPDIR resolves to a long
		// /var/folders/tt/<random>/T/ path that, combined with a long
		// LogPrefix and per-test suffix, can push the resulting admin.sock
		// path past the 104-byte sun_path limit (macOS) and fail bind with
		// "invalid argument". /tmp is short on every platform we run.
		dir, err := os.MkdirTemp("/tmp", fmt.Sprintf("ge-%d-*", i))
		if err != nil {
			c.Stop()
			return nil, err
		}
		c.dataDirs[i] = dir
	}

	switch opts.Mode {
	case ClusterModeDynamicJoin:
		return c.startDynamicJoin()
	case ClusterModeStaticPeers:
		return c.startStaticPeers()
	default:
		c.Stop()
		return nil, fmt.Errorf("unknown e2e cluster mode %q", opts.Mode)
	}
}

func (c *e2eCluster) startDynamicJoin() (*e2eCluster, error) {
	c.procs[0] = c.startNode(c.t, 0)
	if err := waitForPortsParallelErrWithProcesses(c.httpPorts[:1], c.procs[:1], 60*time.Second); err != nil {
		c.Stop()
		return nil, err
	}
	time.Sleep(2 * time.Second)

	// Bootstrap admin SA on the seed node before any followers join.
	// Node 0 is the leader at this point in dynamic-join mode, so the
	// /v1/iam/sa propose succeeds against its admin UDS.
	// When NoBootstrap is set the IAM store is left empty; the caller is
	// responsible for driving the first SA creation via the admin UDS.
	if !c.noBootstrap {
		admin, _ := bootstrapAdminViaUDSAnyResult(c.t, c.dataDirs[:1], 30*time.Second)
		c.accessKey, c.secretKey = admin.AccessKey, admin.SecretKey
		c.saID = admin.SAID
		c.wildcardAdmin = bootstrapResultHasWildcardAdmin(admin)
	}

	// Followers: write .join-pending before starting so they boot directly in
	// join mode without a separate restart step.
	for i := 1; i < len(c.procs); i++ {
		if err := c.writeJoinPending(i, c.raftAddr(0)); err != nil {
			c.Stop()
			return nil, err
		}
		c.procs[i] = c.startNode(c.t, i)
		if err := waitForPortsParallelErrWithProcesses(c.httpPorts[i:i+1], c.procs[i:i+1], 90*time.Second); err != nil {
			c.Stop()
			return nil, err
		}
	}
	c.leaderIdx = 0
	patchSnapshotInterval(c.t, c.dataDirs[c.leaderIdx], "0s")
	return c, nil
}

func (c *e2eCluster) startStaticPeers() (*e2eCluster, error) {
	if c.noBootstrap {
		c.Stop()
		return nil, fmt.Errorf("NoBootstrap is not supported with ClusterModeStaticPeers: leader detection requires admin creds")
	}
	// Bootstrap node 0 as the seed leader.
	c.procs[0] = c.startNode(c.t, 0)
	if err := waitForPortsParallelErrWithProcesses(c.httpPorts[:1], c.procs[:1], 60*time.Second); err != nil {
		c.Stop()
		return nil, err
	}

	// Followers: write .join-pending before starting so they boot in join mode.
	for i := 1; i < len(c.procs); i++ {
		if err := c.writeJoinPending(i, c.raftAddr(0)); err != nil {
			c.Stop()
			return nil, err
		}
		c.procs[i] = c.startNode(c.t, i)
		time.Sleep(150 * time.Millisecond)
	}
	if err := waitForPortsParallelErrWithProcesses(c.httpPorts, c.procs, 60*time.Second); err != nil {
		c.Stop()
		return nil, err
	}

	// Bootstrap admin SA via UDS once the cluster has quorum. Try every
	// node — only the leader's propose succeeds; others return an error
	// and the helper retries the next data dir.
	probeBucket := "e2e-static-leader-probe"
	admin, _ := bootstrapAdminViaUDSAnyResult(c.t, c.dataDirs, 60*time.Second)
	c.accessKey, c.secretKey = admin.AccessKey, admin.SecretKey
	c.saID = admin.SAID
	c.wildcardAdmin = bootstrapResultHasWildcardAdmin(admin)
	c.GrantAdminOnBuckets(probeBucket)
	if err := adminCreateBucketWithPolicyAttachAny(c.dataDirs, c.saID, probeBucket, 60*time.Second); err != nil {
		c.Stop()
		return nil, fmt.Errorf("create writable probe bucket via admin UDS: %w", err)
	}

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
			return tryPutObject(ctx, cli, probeBucket, "__leader_probe", []byte("probe"))
		},
	)
	if err != nil {
		c.Stop()
		return nil, fmt.Errorf("no writable endpoint found within timeout: %w", err)
	}
	c.leaderIdx = leaderIdx
	patchSnapshotInterval(c.t, c.dataDirs[c.leaderIdx], "0s")
	return c, nil
}

func adminCreateBucketWithPolicyAttachAny(dataDirs []string, saID, bucket string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		for _, dir := range dataDirs {
			sock := filepath.Join(dir, "admin.sock")
			if _, err := os.Stat(sock); err != nil {
				lastErr = err
				continue
			}
			if err := tryAdminCreateBucketWithPolicyAttach(sock, bucket, saID, "bucket-admin"); err != nil {
				lastErr = err
				continue
			}
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return lastErr
}

func waitForAdminBucketWritable(
	ctx context.Context,
	dataDirs []string,
	endpoints []string,
	accessKey, secretKey, saID, bucket string,
	timeout time.Duration,
) (int, error) {
	if err := adminCreateBucketWithPolicyAttachAny(dataDirs, saID, bucket, 60*time.Second); err != nil {
		return -1, err
	}
	return waitForWritableEndpoint(
		ctx,
		endpoints,
		timeout,
		5*time.Second,
		1*time.Second,
		func(attemptCtx context.Context, endpoint string) error {
			cli := ecS3Client(endpoint, accessKey, secretKey)
			return tryPutObject(attemptCtx, cli, bucket, "__leader_probe", []byte("probe"))
		},
	)
}

func (c *e2eCluster) EnsureBucketWritable(ctx context.Context, bucket string, timeout time.Duration) (int, error) {
	c.t.Helper()
	if c.saID == "" {
		return -1, fmt.Errorf("cannot create %s without bootstrap sa_id", bucket)
	}
	return waitForAdminBucketWritable(ctx, c.dataDirs, c.httpURLs, c.accessKey, c.secretKey, c.saID, bucket, timeout)
}

func (c *e2eCluster) startNode(t testing.TB, i int) *exec.Cmd {
	t.Helper()
	logFile, err := os.CreateTemp("", fmt.Sprintf("%s-node-%d-*.log", c.logPrefix, i))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	t.Cleanup(func() {
		_ = logFile.Close()
		if t.Failed() && keepE2EArtifacts() {
			t.Logf("e2e cluster node %d log saved to %s", i, logFile.Name())
		} else {
			_ = os.Remove(logFile.Name())
		}
	})

	args := []string{
		"serve",
		"--data", c.dataDirs[i],
		"--port", fmt.Sprintf("%d", c.httpPorts[i]),
		"--node-id", c.nodeID(i),
		"--raft-addr", c.raftAddr(i),
		"--cluster-key", c.clusterKey,
		"--encryption-key-file", c.encKeyFile,
		"--nfs4-port", fmt.Sprintf("%d", c.nfs4Ports[i]),
		"--nbd-port", fmt.Sprintf("%d", c.nbdPorts[i]),
		"--scrub-interval", c.scrubIntervalArg(),
		"--lifecycle-interval", "0",
		// Match helpers_test.go: shrink NFS write buffer idle for e2e so
		// fdatasync-only flows don't wait 30s for the flusher.
		"--nfs-write-buffer-idle", "1s",
	}
	if c.pprofPorts[i] != 0 {
		args = append(args, "--pprof-port", fmt.Sprintf("%d", c.pprofPorts[i]))
	}
	args = append(args, c.extraArgs...)

	cmd := exec.Command(getBinary(), args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	gomega.Expect(cmd.Start()).To(gomega.Succeed(), "start e2e cluster node %d", i)
	return cmd
}

func (c *e2eCluster) scrubIntervalArg() string {
	if c == nil {
		return "0"
	}
	return c.scrubInterval
}

func (c *e2eCluster) nodeID(i int) string {
	return fmt.Sprintf("n%d", i+1)
}

func (c *e2eCluster) raftAddr(i int) string {
	return fmt.Sprintf("127.0.0.1:%d", c.raftPorts[i])
}

// writeJoinPending writes the .join-pending sentinel into node i's data dir
// so that grainfs serve boots directly in join mode without a separate restart.
// Also stages the seed's keystore (keys/0.key + cluster.id) into the joiner's
// data dir — wireDEKKeeper refuses to auto-generate a KEK on a joining node
// (§7 B3 / F#21) and Phase A binds the handshake to the cluster identity.
func (c *e2eCluster) writeJoinPending(i int, seedRaftAddr string) error {
	return writeNodeJoinPending(c.dataDirs[i], c.dataDirs[0], seedRaftAddr)
}

// writeNodeJoinPending writes the .join-pending sentinel into dataDir so that
// grainfs serve boots directly in join mode. Use before starting non-seed nodes
// in tests that manage processes directly (outside e2eCluster).
//
// seedDataDir is the seed node's data dir; the seed's keys/0.key + cluster.id
// are copied into dataDir so the joiner can complete the KEK handshake. If
// seedDataDir is empty, key-staging is skipped (legacy single-node tests).
//
// Phase A: mirrors the real operator workflow (scp <peer>:<dataDir>/keys/0.key
// and <peer>:<dataDir>/cluster.id). Both files must exist on the seed before
// followers boot.
func writeNodeJoinPending(dataDir, seedDataDir, seedRaftAddr string) error {
	if seedDataDir != "" {
		seedKEK := filepath.Join(seedDataDir, "keys", "0.key")
		if data, err := os.ReadFile(seedKEK); err == nil {
			if werr := os.MkdirAll(filepath.Join(dataDir, "keys"), 0o700); werr != nil {
				return fmt.Errorf("mkdir joiner keys dir: %w", werr)
			}
			if werr := os.WriteFile(filepath.Join(dataDir, "keys", "0.key"), data, 0o600); werr != nil {
				return fmt.Errorf("stage joiner keys/0.key: %w", werr)
			}
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("read seed keys/0.key: %w", err)
		}
		seedClusterID := filepath.Join(seedDataDir, "cluster.id")
		if data, err := os.ReadFile(seedClusterID); err == nil {
			if werr := os.WriteFile(filepath.Join(dataDir, "cluster.id"), data, 0o600); werr != nil {
				return fmt.Errorf("stage joiner cluster.id: %w", werr)
			}
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("read seed cluster.id: %w", err)
		}
	}
	return os.WriteFile(
		filepath.Join(dataDir, joinPendingFile),
		[]byte(seedRaftAddr), 0o600,
	)
}

// KillNode terminates node i's process via SIGKILL while preserving its
// dataDir / port assignments. The slot c.procs[i] is set to nil; use
// RestartNode to bring it back in the same slot. Cleanup safety: the Stop
// loop below tolerates nil entries (terminateProcess checks for nil).
func (c *e2eCluster) KillNode(i int) {
	if c == nil || i < 0 || i >= len(c.procs) {
		return
	}
	if c.procs[i] != nil {
		terminateProcess(c.procs[i])
		c.procs[i] = nil
	}
}

// RestartNode re-launches node i with the preserved data dir / ports.
// Mirrors the initial startNode call. Caller should poll waitClusterSettled
// or getStatusJSON after this to confirm the node has rejoined.
func (c *e2eCluster) RestartNode(t testing.TB, i int) {
	t.Helper()
	if c == nil || i < 0 || i >= len(c.procs) {
		return
	}
	if c.procs[i] != nil {
		return // already running
	}
	c.procs[i] = c.startNode(t, i)
}

func (c *e2eCluster) Stop() {
	if c == nil || c.stopped {
		return
	}
	c.stopped = true
	for _, p := range c.procs {
		terminateProcess(p)
	}
	for _, dir := range c.dataDirs {
		_ = os.RemoveAll(dir)
	}
}

func (c *e2eCluster) S3Client(i int) *s3.Client {
	return ecS3Client(c.httpURLs[i], c.accessKey, c.secretKey)
}

// AwaitWriteFromNonOwner polls non-leader peers issuing tiny probe writes
// against an isolated __grainfs_probe internal-prefix namespace until at
// least one succeeds or deadline expires. Used by fault tests to assert
// raft leader rotation has completed (a HeadBucket polling proxy can
// return stale-ack during election; a successful committed write is the
// direct evidence).
//
// The probe bucket is created on first use and is namespace-isolated
// from user buckets — the __grainfs_ prefix is the GrainFS internal
// bucket convention.
func (c *e2eCluster) AwaitWriteFromNonOwner(bucket, key string, deadline time.Duration) error {
	if c == nil {
		return fmt.Errorf("nil cluster")
	}
	// Ensure probe bucket exists (idempotent).
	c.GrantAdminOnBuckets(bucket)
	ctx := context.Background()
	if err := adminCreateBucketWithPolicyAttachAny(c.dataDirs, c.saID, bucket, 60*time.Second); err != nil {
		return fmt.Errorf("create probe bucket %s: %w", bucket, err)
	}

	end := time.Now().Add(deadline)
	body := []byte("probe")
	for time.Now().Before(end) {
		for i, p := range c.procs {
			if p == nil || i == c.leaderIdx {
				continue
			}
			if err := tryPutObject(ctx, c.S3Client(i), bucket, key, body); err == nil {
				return nil
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("AwaitWriteFromNonOwner(%q,%q) timed out after %s", bucket, key, deadline)
}

func (c *e2eCluster) GrantAdminOnBuckets(buckets ...string) {
	c.t.Helper()
	if len(buckets) == 0 || c.wildcardAdmin {
		return
	}
	if c.saID == "" {
		c.t.Fatalf("cannot grant bucket admin without bootstrap sa_id")
	}
	policyAttachAdminOnBucketsViaUDSAny(c.t, c.dataDirs, c.saID, buckets, 60*time.Second)
}

var _ = ginkgo.Describe("E2E cluster harness options", func() {
	ginkgo.It("allows non-EC extra args", func() {
		runNormalizeOptionsAllowsNonECExtraArgs(ginkgo.GinkgoTB())
	})

	ginkgo.It("rejects removed zero-config flags", func() {
		runNormalizeOptionsRejectsRemovedZeroConfigFlags(ginkgo.GinkgoTB())
	})
})
