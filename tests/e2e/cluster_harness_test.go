package e2e

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

type ClusterMode string

const (
	ClusterModeDynamicJoin ClusterMode = "dynamic-join"
	ClusterModeStaticPeers ClusterMode = "static-peers"
)

type e2eClusterOptions struct {
	Nodes      int
	SeedGroups int
	Mode       ClusterMode
	ClusterKey string
	AccessKey  string
	SecretKey  string
	ECData     int
	ECParity   int
	LogPrefix  string
	DisableNFS bool
	DisableNBD bool
}

type e2eCluster struct {
	t          *testing.T
	mode       ClusterMode
	procs      []*exec.Cmd
	dataDirs   []string
	httpPorts  []int
	raftPorts  []int
	nfs4Ports  []int
	nbdPorts   []int
	httpURLs   []string
	clusterKey string
	accessKey  string
	secretKey  string
	seedGroups int
	ecData     int
	ecParity   int
	logPrefix  string
	stopped    bool
	leaderIdx  int
}

func startE2ECluster(t *testing.T, opts e2eClusterOptions) *e2eCluster {
	t.Helper()
	c, err := tryStartE2ECluster(t, normalizeE2EClusterOptions(opts))
	require.NoError(t, err)
	t.Cleanup(c.Stop)
	return c
}

func normalizeE2EClusterOptions(opts e2eClusterOptions) e2eClusterOptions {
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
	if opts.SeedGroups == 0 {
		opts.SeedGroups = 1
	}
	if opts.LogPrefix == "" {
		opts.LogPrefix = "grainfs-e2e-cluster"
	}
	return opts
}

func tryStartE2ECluster(t *testing.T, opts e2eClusterOptions) (*e2eCluster, error) {
	t.Helper()
	binary := getBinary()
	if _, err := os.Stat(binary); err != nil {
		t.Skipf("grainfs binary not found at %s - run `make build` first", binary)
	}

	c := &e2eCluster{
		t:          t,
		mode:       opts.Mode,
		clusterKey: opts.ClusterKey,
		accessKey:  opts.AccessKey,
		secretKey:  opts.SecretKey,
		seedGroups: opts.SeedGroups,
		ecData:     opts.ECData,
		ecParity:   opts.ECParity,
		logPrefix:  opts.LogPrefix,
		leaderIdx:  -1,
	}
	c.procs = make([]*exec.Cmd, opts.Nodes)
	c.dataDirs = make([]string, opts.Nodes)
	c.httpPorts = make([]int, opts.Nodes)
	c.raftPorts = make([]int, opts.Nodes)
	c.nfs4Ports = make([]int, opts.Nodes)
	c.nbdPorts = make([]int, opts.Nodes)
	c.httpURLs = make([]string, opts.Nodes)

	ports := uniqueFreePorts(opts.Nodes * 4)
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
		c.httpURLs[i] = fmt.Sprintf("http://127.0.0.1:%d", c.httpPorts[i])
		dir, err := os.MkdirTemp("", fmt.Sprintf("%s-node-%d-*", opts.LogPrefix, i))
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
	if err := waitForPortsParallelErr(c.httpPorts[:1], 60*time.Second); err != nil {
		c.Stop()
		return nil, err
	}
	time.Sleep(2 * time.Second)
	for i := 1; i < len(c.procs); i++ {
		c.procs[i] = c.startNode(c.t, i)
		if err := waitForPortsParallelErr(c.httpPorts[i:i+1], 90*time.Second); err != nil {
			c.Stop()
			return nil, err
		}
	}
	c.leaderIdx = 0
	return c, nil
}

func (c *e2eCluster) startStaticPeers() (*e2eCluster, error) {
	initialNodes := len(c.procs)
	if len(c.procs) > 1 {
		initialNodes = len(c.procs)/2 + 1
	}
	for i := 0; i < initialNodes; i++ {
		c.procs[i] = c.startNode(c.t, i)
		time.Sleep(150 * time.Millisecond)
	}
	if err := waitForPortsParallelErr(c.httpPorts[:initialNodes], 60*time.Second); err != nil {
		c.Stop()
		return nil, err
	}
	for i := initialNodes; i < len(c.procs); i++ {
		c.procs[i] = c.startNode(c.t, i)
		time.Sleep(150 * time.Millisecond)
	}
	if err := waitForPortsParallelErr(c.httpPorts, 60*time.Second); err != nil {
		c.Stop()
		return nil, err
	}
	time.Sleep(4 * time.Second)
	c.leaderIdx = 0
	return c, nil
}

func (c *e2eCluster) startNode(t *testing.T, i int) *exec.Cmd {
	t.Helper()
	logFile, err := os.CreateTemp("", fmt.Sprintf("%s-node-%d-*.log", c.logPrefix, i))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = logFile.Close()
		if t.Failed() {
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
		"--access-key", c.accessKey,
		"--secret-key", c.secretKey,
		"--ec-data", fmt.Sprintf("%d", c.ecData),
		"--ec-parity", fmt.Sprintf("%d", c.ecParity),
		"--seed-groups", fmt.Sprintf("%d", c.seedGroups),
		"--nfs4-port", fmt.Sprintf("%d", c.nfs4Ports[i]),
		"--nbd-port", fmt.Sprintf("%d", c.nbdPorts[i]),
		"--snapshot-interval", "0",
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
		"--no-encryption",
	}
	if c.mode == ClusterModeDynamicJoin && i > 0 {
		args = append(args, "--join", c.raftAddr(0))
	}
	if c.mode == ClusterModeStaticPeers {
		args = append(args, "--peers", c.staticPeersFor(i))
	}

	cmd := exec.Command(getBinary(), args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	require.NoError(t, cmd.Start(), "start e2e cluster node %d", i)
	return cmd
}

func (c *e2eCluster) nodeID(i int) string {
	if c.mode == ClusterModeStaticPeers {
		return c.raftAddr(i)
	}
	return fmt.Sprintf("n%d", i+1)
}

func (c *e2eCluster) raftAddr(i int) string {
	return fmt.Sprintf("127.0.0.1:%d", c.raftPorts[i])
}

func (c *e2eCluster) staticPeersFor(i int) string {
	peers := make([]string, 0, len(c.raftPorts)-1)
	for j := range c.raftPorts {
		if j != i {
			peers = append(peers, c.raftAddr(j))
		}
	}
	return strings.Join(peers, ",")
}

func (c *e2eCluster) Stop() {
	if c == nil || c.stopped {
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
		done := make(chan struct{})
		go func(p *exec.Cmd) {
			_ = p.Wait()
			close(done)
		}(p)
		select {
		case <-done:
		case <-time.After(time.Until(deadline)):
			_ = p.Process.Kill()
			<-done
		}
	}
	for _, dir := range c.dataDirs {
		_ = os.RemoveAll(dir)
	}
}

func (c *e2eCluster) S3Client(i int) *s3.Client {
	return ecS3Client(c.httpURLs[i], c.accessKey, c.secretKey)
}
