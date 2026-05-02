# Dynamic Join E2E Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make dynamic join the default multi-node e2e harness while keeping static peer startup explicit for legacy topology tests.

**Architecture:** Introduce a shared `e2eCluster` harness in `tests/e2e/cluster_harness_test.go` with `ClusterModeDynamicJoin` as the default and `ClusterModeStaticPeers` as an explicit option. Move ad hoc dynamic join startup out of `cluster_join_e2e_test.go`, then migrate service-level tests to the shared harness while preserving static-only tests under a legacy helper.

**Tech Stack:** Go e2e tests, `os/exec` process harness, AWS SDK S3 client, existing GrainFS `serve` CLI flags.

---

### Task 1: Create Shared Cluster Harness

**Files:**
- Create: `tests/e2e/cluster_harness_test.go`
- Modify: none
- Test: `go test ./tests/e2e -run '^TestE2E_JoinedNodeEdgeForwardsBeforeDataReady$' -count=1 -timeout 3m`

- [ ] **Step 1: Move cluster process types into a shared file**

Create `tests/e2e/cluster_harness_test.go` with:

```go
package e2e

import (
	"fmt"
	"os"
	"os/exec"
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
	Nodes       int
	SeedGroups  int
	Mode        ClusterMode
	ClusterKey  string
	AccessKey   string
	SecretKey   string
	ECData      int
	ECParity    int
	LogPrefix   string
	DisableNFS  bool
	DisableNBD  bool
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
```

- [ ] **Step 2: Add startup implementation**

Append:

```go
func tryStartE2ECluster(t *testing.T, opts e2eClusterOptions) (*e2eCluster, error) {
	t.Helper()
	binary := getBinary()
	if _, err := os.Stat(binary); err != nil {
		t.Skipf("grainfs binary not found at %s — run `make build` first", binary)
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
```

- [ ] **Step 3: Add node process helpers**

Append:

```go
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
```

- [ ] **Step 4: Add process argument construction**

Append:

```go
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
```

Import `strings` in this file when adding `staticPeersFor`.

- [ ] **Step 5: Add cleanup and client helpers**

Append:

```go
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
```

- [ ] **Step 6: Run targeted compile**

Run:

```bash
go test ./tests/e2e -run '^$'
```

Expected: package compiles and reports `ok`.

- [ ] **Step 7: Commit**

```bash
git add tests/e2e/cluster_harness_test.go
git commit -m "test: add shared e2e cluster harness"
```

### Task 2: Move Dynamic Join Tests To Shared Harness

**Files:**
- Modify: `tests/e2e/cluster_join_e2e_test.go`
- Test: `GRAINFS_BINARY=/tmp/grainfs-join go test ./tests/e2e -run '^(TestE2E_JoinedNodeEdgeForwardsBeforeDataReady|TestE2E_AllServicesAvailableOnJoinedNodes|TestE2E_DefaultBucketOnlySeedCreates)$' -count=1 -timeout 5m -v`

- [ ] **Step 1: Delete local dynamic harness from join test file**

Remove these declarations from `tests/e2e/cluster_join_e2e_test.go` by deleting each complete declaration body from its opening line through its closing brace:

```go
type dynamicJoinCluster struct
func startDynamicJoinCluster(t *testing.T, nodes int) *dynamicJoinCluster
func (c *dynamicJoinCluster) startNode(t *testing.T, i int) *exec.Cmd
func (c *dynamicJoinCluster) Stop()
```

After deletion, imports must no longer include `fmt`, `os`, `os/exec`, or `syscall` unless another test in the file still uses them.

- [ ] **Step 2: Replace test startup calls**

Replace:

```go
c := startDynamicJoinCluster(t, 2)
```

with:

```go
c := startE2ECluster(t, e2eClusterOptions{
	Nodes:     2,
	Mode:      ClusterModeDynamicJoin,
	LogPrefix: "grainfs-dynamic-join",
})
```

in all three join tests.

- [ ] **Step 3: Replace client construction**

Replace:

```go
joinClient := ecS3Client(c.httpURLs[1], c.accessKey, c.secretKey)
seedClient := ecS3Client(c.httpURLs[0], c.accessKey, c.secretKey)
```

with:

```go
joinClient := c.S3Client(1)
seedClient := c.S3Client(0)
```

Replace loop-local clients:

```go
client := ecS3Client(endpoint, c.accessKey, c.secretKey)
```

with:

```go
client := c.S3Client(i)
```

where the loop is changed to `for i, endpoint := range c.httpURLs`.

- [ ] **Step 4: Format and run dynamic join e2e**

Run:

```bash
gofmt -w tests/e2e/cluster_harness_test.go tests/e2e/cluster_join_e2e_test.go
GRAINFS_BINARY=/tmp/grainfs-join go test ./tests/e2e -run '^(TestE2E_JoinedNodeEdgeForwardsBeforeDataReady|TestE2E_AllServicesAvailableOnJoinedNodes|TestE2E_DefaultBucketOnlySeedCreates)$' -count=1 -timeout 5m -v
```

Expected: all three tests pass.

- [ ] **Step 5: Commit**

```bash
git add tests/e2e/cluster_harness_test.go tests/e2e/cluster_join_e2e_test.go
git commit -m "test: use shared harness for dynamic join e2e"
```

### Task 3: Make Existing MR Harness Explicitly Static Legacy

**Files:**
- Modify: `tests/e2e/multiraft_sharding_test.go`
- Test: `GRAINFS_BINARY=/tmp/grainfs-join go test ./tests/e2e -run '^TestE2E_MultiRaftSharding_AllNodeServices$' -count=1 -timeout 4m -v`

- [ ] **Step 1: Rename helper to static legacy**

Rename:

```go
func startMRCluster(t *testing.T, numNodes, seedGroups int) *mrCluster
func tryStartMRCluster(t *testing.T, numNodes, seedGroups int) (*mrCluster, error)
```

to:

```go
func startStaticMRCluster(t *testing.T, numNodes, seedGroups int) *mrCluster
func tryStartStaticMRCluster(t *testing.T, numNodes, seedGroups int) (*mrCluster, error)
```

Update all call sites in `tests/e2e/multiraft_sharding_test.go`, `tests/e2e/seed_groups_test.go`, and `tests/e2e/iceberg_duckdb_test.go`.

- [ ] **Step 2: Update comments**

Replace comments that describe the helper as the normal cluster path with explicit static wording:

```go
// startStaticMRCluster starts the legacy static --peers topology. Use this only
// for tests that require precomputed data-group voter placement or static
// bootstrap compatibility. New service-level multi-node tests should use
// startE2ECluster with ClusterModeDynamicJoin.
```

- [ ] **Step 3: Run static service e2e**

Run:

```bash
gofmt -w tests/e2e/multiraft_sharding_test.go tests/e2e/seed_groups_test.go tests/e2e/iceberg_duckdb_test.go
GRAINFS_BINARY=/tmp/grainfs-join go test ./tests/e2e -run '^TestE2E_MultiRaftSharding_AllNodeServices$' -count=1 -timeout 4m -v
```

Expected: test passes.

- [ ] **Step 4: Commit**

```bash
git add tests/e2e/multiraft_sharding_test.go tests/e2e/seed_groups_test.go tests/e2e/iceberg_duckdb_test.go
git commit -m "test: mark static peer e2e harness as legacy"
```

### Task 4: Add Node Count Matrix Tests For Dynamic Join Services

**Files:**
- Modify: `tests/e2e/cluster_join_e2e_test.go`
- Test: `GRAINFS_BINARY=/tmp/grainfs-join go test ./tests/e2e -run '^TestE2E_DynamicJoinServices_NodeCounts$' -count=1 -timeout 6m -v`

- [ ] **Step 1: Add table-driven node-count test**

Add:

```go
func TestE2E_DynamicJoinServices_NodeCounts(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e")
	}
	for _, nodes := range []int{2, 3} {
		t.Run(fmt.Sprintf("N%d", nodes), func(t *testing.T) {
			c := startE2ECluster(t, e2eClusterOptions{
				Nodes:     nodes,
				Mode:      ClusterModeDynamicJoin,
				LogPrefix: fmt.Sprintf("grainfs-dynamic-join-n%d", nodes),
			})
			waitForPortsParallel(t, c.nfs4Ports, 30*time.Second)
			waitForPortsParallel(t, c.nbdPorts, 30*time.Second)

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			bucket := fmt.Sprintf("dynamic-services-n%d", nodes)
			key := "object.txt"
			body := []byte(fmt.Sprintf("hello from n%d", nodes))

			require.Eventually(t, func() bool {
				return tryCreateBucket(ctx, c.S3Client(nodes-1), bucket) == nil
			}, 30*time.Second, 500*time.Millisecond)
			require.NoError(t, tryPutObject(ctx, c.S3Client(nodes-1), bucket, key, body))
			for i := 0; i < nodes; i++ {
				got, err := getObjectBytes(ctx, c.S3Client(i), bucket, key)
				require.NoErrorf(t, err, "GetObject through node %d", i)
				require.Equal(t, body, got)
			}
		})
	}
}
```

Ensure `fmt` is imported in `cluster_join_e2e_test.go`.

- [ ] **Step 2: Run the matrix test**

Run:

```bash
gofmt -w tests/e2e/cluster_join_e2e_test.go
GRAINFS_BINARY=/tmp/grainfs-join go test ./tests/e2e -run '^TestE2E_DynamicJoinServices_NodeCounts$' -count=1 -timeout 6m -v
```

Expected: `N2` and `N3` subtests pass.

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/cluster_join_e2e_test.go
git commit -m "test: add dynamic join service node matrix"
```

### Task 5: Final Verification

**Files:**
- Modify: none
- Test: all targeted commands below

- [ ] **Step 1: Run unit and command tests**

```bash
go test ./internal/cluster ./cmd/grainfs
```

Expected: both packages pass.

- [ ] **Step 2: Build test binary**

```bash
go build -o /tmp/grainfs-join ./cmd/grainfs
```

Expected: command exits 0.

- [ ] **Step 3: Run dynamic join suite**

```bash
GRAINFS_BINARY=/tmp/grainfs-join go test ./tests/e2e -run '^(TestE2E_JoinedNodeEdgeForwardsBeforeDataReady|TestE2E_AllServicesAvailableOnJoinedNodes|TestE2E_DefaultBucketOnlySeedCreates|TestE2E_DynamicJoinServices_NodeCounts)$' -count=1 -timeout 8m -v
```

Expected: all tests pass.

- [ ] **Step 4: Run static legacy smoke**

```bash
GRAINFS_BINARY=/tmp/grainfs-join go test ./tests/e2e -run '^TestE2E_MultiRaftSharding_AllNodeServices$' -count=1 -timeout 4m -v
```

Expected: test passes.

- [ ] **Step 5: Run singleton smoke/default bucket**

```bash
GRAINFS_BINARY=/tmp/grainfs-join go test ./tests/e2e -run '^(TestSmoke_DeploymentVerification|TestDefaultBucket_ExistsOnStartup)$' -count=1 -timeout 4m -v
```

Expected: both tests pass.

- [ ] **Step 6: Confirm clean diff**

```bash
git diff --check
git status --short --branch
```

Expected: no whitespace errors; only intentional committed changes remain.

- [ ] **Step 7: Commit any final cleanup**

If Step 6 shows intentional uncommitted cleanup:

```bash
git add <paths>
git commit -m "test: clean up dynamic join e2e coverage"
```
