package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
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
	// ExtraArgs is appended verbatim to every node's `grainfs serve` cmdline.
	// Use for per-test flag tweaks (e.g. `--vlog-warn-ratio=0.001`,
	// `--badger-gc-disable=true`) so the harness stays generic.
	ExtraArgs []string
}

type e2eCluster struct {
	t             *testing.T
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
	logPrefix     string
	scrubInterval string
	extraArgs     []string
	stopped       bool
	leaderIdx     int
}

func startE2ECluster(t *testing.T, opts e2eClusterOptions) *e2eCluster {
	t.Helper()
	opts = normalizeE2EClusterOptions(opts)
	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		c, err := tryStartE2ECluster(t, opts)
		if err == nil {
			t.Cleanup(c.Stop)
			return c
		}
		lastErr = err
		t.Logf("e2e cluster start attempt %d failed: %v", attempt, err)
		time.Sleep(time.Duration(attempt) * 250 * time.Millisecond)
	}
	require.NoError(t, lastErr)
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

func TestNormalizeE2EClusterOptionsRejectsRemovedZeroConfigFlags(t *testing.T) {
	for _, arg := range []string{"--ec-data=2", "--ec-data", "--ec-parity=1", "--ec-parity", "--seed-groups=2", "--seed-groups"} {
		t.Run(arg, func(t *testing.T) {
			require.PanicsWithValue(t,
				fmt.Sprintf("removed zero-config flag %q: use Nodes to select the automatic profile", arg),
				func() {
					normalizeE2EClusterOptions(e2eClusterOptions{ExtraArgs: []string{arg}})
				})
		})
	}
}

func TestNormalizeE2EClusterOptionsAllowsNonECExtraArgs(t *testing.T) {
	opts := normalizeE2EClusterOptions(e2eClusterOptions{
		ExtraArgs: []string{"--vlog-warn-ratio=0.001"},
	})
	require.Equal(t, []string{"--vlog-warn-ratio=0.001"}, opts.ExtraArgs)
}

func tryStartE2ECluster(t *testing.T, opts e2eClusterOptions) (*e2eCluster, error) {
	t.Helper()
	binary := getBinary()
	if _, err := os.Stat(binary); err != nil {
		t.Skipf("grainfs binary not found at %s - run `make build` first", binary)
	}

	c := &e2eCluster{
		t:             t,
		mode:          opts.Mode,
		clusterKey:    opts.ClusterKey,
		encKeyFile:    makeSharedEncryptionKeyFile(t),
		accessKey:     opts.AccessKey,
		secretKey:     opts.SecretKey,
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
	admin, _ := bootstrapAdminViaUDSAnyResult(c.t, c.dataDirs[:1], 30*time.Second)
	c.accessKey, c.secretKey = admin.AccessKey, admin.SecretKey
	c.saID = admin.SAID
	c.wildcardAdmin = bootstrapResultHasWildcardAdmin(admin)

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
	probeBucket := "__e2e-static-leader-probe"
	admin, _ := bootstrapAdminViaUDSAnyResult(c.t, c.dataDirs, 60*time.Second)
	c.accessKey, c.secretKey = admin.AccessKey, admin.SecretKey
	c.saID = admin.SAID
	c.wildcardAdmin = bootstrapResultHasWildcardAdmin(admin)
	c.GrantAdminOnBuckets(probeBucket)

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
		return nil, fmt.Errorf("no writable endpoint found within timeout: %w", err)
	}
	c.leaderIdx = leaderIdx
	patchSnapshotInterval(c.t, c.dataDirs[c.leaderIdx], "0s")
	return c, nil
}

func (c *e2eCluster) startNode(t *testing.T, i int) *exec.Cmd {
	t.Helper()
	logFile, err := os.CreateTemp("", fmt.Sprintf("%s-node-%d-*.log", c.logPrefix, i))
	require.NoError(t, err)
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
	}
	if c.pprofPorts[i] != 0 {
		args = append(args, "--pprof-port", fmt.Sprintf("%d", c.pprofPorts[i]))
	}
	args = append(args, c.extraArgs...)

	cmd := exec.Command(getBinary(), args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	require.NoError(t, cmd.Start(), "start e2e cluster node %d", i)
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
func (c *e2eCluster) writeJoinPending(i int, seedRaftAddr string) error {
	return writeNodeJoinPending(c.dataDirs[i], seedRaftAddr)
}

// writeNodeJoinPending writes the .join-pending sentinel into dataDir so that
// grainfs serve boots directly in join mode. Use before starting non-seed nodes
// in tests that manage processes directly (outside e2eCluster).
func writeNodeJoinPending(dataDir, seedRaftAddr string) error {
	return os.WriteFile(
		fmt.Sprintf("%s/%s", dataDir, joinPendingFile),
		[]byte(seedRaftAddr), 0o600,
	)
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

func (c *e2eCluster) GrantAdminOnBuckets(buckets ...string) {
	c.t.Helper()
	if len(buckets) == 0 || c.wildcardAdmin {
		return
	}
	if c.saID == "" {
		c.t.Fatalf("cannot grant bucket admin without bootstrap sa_id")
	}
	grantAdminOnBucketsViaUDSAny(c.t, c.dataDirs, c.saID, buckets, 60*time.Second)
}
