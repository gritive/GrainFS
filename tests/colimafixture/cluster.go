// Package colimafixture boots a 3-node grainfs cluster on the macOS host with
// per-protocol ports bound to all interfaces (0.0.0.0) so a colima Linux VM
// can reach them at HOST_IP=192.168.5.2. Used by tests/<proto>_colima
// cluster-mount tests (Tasks 15b/16/17) to reuse the existing colima
// single-node mount infrastructure against a real cluster.
//
// This file is intentionally NOT _test.go: it must be importable by
// `tests/<proto>_colima` packages (which carry the `colima` build tag),
// and helpers here take testing.TB so both Test* and Benchmark* can use them.
//
// Boot model (simplified from tests/e2e/multiraft_sharding_test.go):
//   - Seed node (idx 0) starts first; we wait for HTTP /, then bootstrap an
//     admin SA via the seed's admin UDS so AK/SK are stable across followers.
//   - Followers write a `.join-pending` file containing the seed's raft addr,
//     then boot in join mode. We wait for each follower's HTTP / to come up.
//   - No retry loop, no leader-probe: fail-fast if boot doesn't converge in
//     time. Boot test (TestColimaClusterFixtureBoots) must run in <60s.
package colimafixture

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/clusteradmin"
	"github.com/gritive/GrainFS/internal/transport"
)

// Cluster is the handle returned by StartCluster. All ports bind 0.0.0.0 on
// the host, so colima VM clients can reach them via 192.168.5.2:<port>.
type Cluster struct {
	HTTPPorts []int    // S3/admin HTTP ports (bind :%d on host)
	RaftPorts []int    // cluster transport HTTP ports
	JoinPorts []int    // Zero-CA HTTP join-listener ports
	DataDirs  []string // per-node data directories (temp)

	LeaderIdx int    // 0 — seed is always the initial leader at boot
	AccessKey string // bootstrap admin SA access key
	SecretKey string // bootstrap admin SA secret key
	SAID      string // bootstrap admin SA id (for grant calls)

	procs   []*exec.Cmd
	logs    []*os.File
	stopped bool
}

// Options configure cluster boot.
type Options struct {
	// NumNodes is the cluster size. Defaults to 3 when 0.
	NumNodes int

	// Binary overrides the grainfs binary path (default: ../../bin/grainfs).
	Binary string

	// SkipCleanup disables the automatic t.Cleanup(c.Stop) registration.
	// Callers using a process-global sync.Once fixture pattern must set
	// this true and arrange Stop from TestMain (or an equivalent
	// package-lifetime hook) so the cluster outlives the first caller's t.
	SkipCleanup bool
}

// StartCluster boots a 3-node grainfs cluster on the macOS host with all
// protocol ports bound to 0.0.0.0 so colima VM clients can connect. The
// returned Cluster's Stop method is registered with t.Cleanup; callers may
// also call it explicitly. Fatals via t on any boot error.
func StartCluster(t testing.TB, opts Options) *Cluster {
	t.Helper()

	numNodes := opts.NumNodes
	if numNodes == 0 {
		numNodes = 3
	}
	binary := opts.Binary
	if binary == "" {
		binary = "../../bin/grainfs"
	}
	_, _ = os.Stat(binary)

	c := &Cluster{
		HTTPPorts: make([]int, numNodes),
		RaftPorts: make([]int, numNodes),
		JoinPorts: make([]int, numNodes),
		DataDirs:  make([]string, numNodes),
		procs:     make([]*exec.Cmd, numNodes),
		logs:      make([]*os.File, numNodes),
	}
	if !opts.SkipCleanup {
		t.Cleanup(c.Stop)
	}

	// Allocate 6 slots/node (http, raft, reserved, reserved, reserved, join).
	// Slots 2–4 per node are reserved for future or removed protocols so that
	// port indices (and the join slot at index 5) remain stable across changes.
	ports := uniqueFreePorts(t, numNodes*6)
	for i := 0; i < numNodes; i++ {
		c.HTTPPorts[i] = ports[i]
		c.RaftPorts[i] = ports[numNodes+i]
		c.JoinPorts[i] = ports[5*numNodes+i]
		dir, err := os.MkdirTemp("", fmt.Sprintf("grainfs-colima-cluster-%d-*", i))
		if err != nil {
			c.Stop()
			t.Fatalf("mkdtemp node %d: %v", i, err)
		}
		c.DataDirs[i] = dir
	}

	clusterKey := "COLIMA-FIXTURE-CLUSTER-KEY"

	// Stage the cluster transport PSK on the SEED only (node 0). Followers join
	// via invite-join; the minted bundle delivers the sealed KEK+PSK+cluster.id.
	if err := transport.NewKeystore(c.DataDirs[0]).WriteCurrent(clusterKey); err != nil {
		c.Stop()
		t.Fatalf("pre-seed seed keys.d/current.key: %v", err)
	}

	// Seed (node 0) starts first.
	c.procs[0], c.logs[0] = c.spawn(t, binary, 0, nil)
	if err := waitHTTPReady(c.HTTPPorts[0], 60*time.Second); err != nil {
		c.Stop()
		t.Fatalf("seed node http not ready: %v", err)
	}

	// Bootstrap admin SA on the seed before followers join.
	sock := filepath.Join(c.DataDirs[0], "admin.sock")
	if err := waitForFile(sock, 30*time.Second); err != nil {
		c.Stop()
		t.Fatalf("admin socket %s did not appear: %v", sock, err)
	}
	saID, ak, sk, err := bootstrapAdminSA(sock, 30*time.Second)
	if err != nil {
		c.Stop()
		t.Fatalf("bootstrap admin SA: %v", err)
	}
	c.SAID, c.AccessKey, c.SecretKey = saID, ak, sk

	// Followers join via invite-join: mint a single-use bundle on the seed's
	// admin UDS and boot each follower with GRAINFS_INVITE_BUNDLE + a clean data
	// dir. The bundle delivers the sealed KEK+PSK+cluster.id, so no key/identity
	// pre-staging is needed.
	for i := 1; i < numNodes; i++ {
		bundle, err := mintInvite(binary, sock)
		if err != nil {
			c.Stop()
			t.Fatalf("mint invite for follower %d: %v", i, err)
		}
		c.procs[i], c.logs[i] = c.spawn(t, binary, i, []string{"GRAINFS_INVITE_BUNDLE=" + bundle})
	}
	for i := 1; i < numNodes; i++ {
		if err := waitHTTPReady(c.HTTPPorts[i], 90*time.Second); err != nil {
			c.Stop()
			t.Fatalf("follower node %d http not ready: %v", i, err)
		}
	}

	// Wait until the seed admin UDS reports membership convergence. HTTP /
	// can respond before raft has actually accepted the follower join — so
	// poll Status().Peers until it includes all numNodes raft addrs (or
	// LeaderID is set and peer count matches). Without this, downstream
	// colima tests can write to node 0 and read from node 1 before the
	// follower has caught up via raft replication.
	leaderIdx, err := waitForMembership(sock, c.RaftPorts, 60*time.Second)
	if err != nil {
		c.Stop()
		t.Fatalf("cluster membership did not converge: %v", err)
	}
	c.LeaderIdx = leaderIdx
	return c
}

// Stop terminates all node processes and removes temp directories. Safe to
// call multiple times. Registered via t.Cleanup, but callers may also invoke
// directly (e.g. mid-test to test restart behavior in future tasks).
func (c *Cluster) Stop() {
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
		done := make(chan error, 1)
		go func(p *exec.Cmd) { done <- p.Wait() }(p)
		select {
		case <-done:
		case <-time.After(time.Until(deadline)):
			_ = p.Process.Kill()
			<-done
		}
	}
	for _, f := range c.logs {
		if f != nil {
			_ = f.Close()
		}
	}
	for _, d := range c.DataDirs {
		if d == "" {
			continue
		}
		_ = os.RemoveAll(d)
	}
}

// HTTPURL returns the host-local URL for node i. Colima VM tests should use
// fmt.Sprintf("http://%s:%d", HOST_IP, c.HTTPPorts[i]) instead.
func (c *Cluster) HTTPURL(i int) string {
	return fmt.Sprintf("http://127.0.0.1:%d", c.HTTPPorts[i])
}

// spawn launches one grainfs serve process. Join mode is signaled by the
// presence of a `.join-pending` file in the data dir (written by the caller
// before calling spawn); the seed has no such file and bootstraps a new
// cluster.
func (c *Cluster) spawn(t testing.TB, binary string, i int, extraEnv []string) (*exec.Cmd, *os.File) {
	t.Helper()
	raftAddr := fmt.Sprintf("127.0.0.1:%d", c.RaftPorts[i])
	joinAddr := fmt.Sprintf("127.0.0.1:%d", c.JoinPorts[i])
	logFile, err := os.CreateTemp("", fmt.Sprintf("grainfs-colima-cluster-%d-*.log", i))
	if err != nil {
		t.Fatalf("create node %d log: %v", i, err)
	}
	args := []string{
		"serve",
		"--data", c.DataDirs[i],
		"--port", fmt.Sprintf("%d", c.HTTPPorts[i]),
		"--node-id", raftAddr,
		"--raft-addr", raftAddr,
		"--join-listen-addr", joinAddr,
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
	}
	cmd := exec.Command(binary, args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if len(extraEnv) > 0 {
		cmd.Env = append(os.Environ(), extraEnv...)
	}
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		_ = os.Remove(logFile.Name())
		t.Fatalf("start node %d: %v", i, err)
	}
	return cmd, logFile
}

// uniqueFreePorts picks n TCP ports and keeps listeners open until the full set
// is reserved, so concurrent fixtures cannot claim the same port.
func uniqueFreePorts(t testing.TB, n int) []int {
	t.Helper()
	tcpListeners := make([]net.Listener, 0, n)
	ports := make([]int, 0, n)
	for len(ports) < n {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			for _, prev := range tcpListeners {
				_ = prev.Close()
			}
			t.Fatalf("listen :0: %v", err)
		}
		port := ln.Addr().(*net.TCPAddr).Port
		tcpListeners = append(tcpListeners, ln)
		ports = append(ports, port)
	}
	for _, ln := range tcpListeners {
		_ = ln.Close()
	}
	return ports
}

// writeJoinPending mirrors writeNodeJoinPending from
// tests/e2e/cluster_harness_test.go: a 0o600 file at <dataDir>/.join-pending
// mintInvite runs `grainfs cluster invite create` against the seed's admin UDS
// and returns the single-use bundle token (the line after the
// "GRAINFS_INVITE_BUNDLE" prompt). The admin UDS + meta-raft leadership + join
// listener take a moment to settle after the HTTP port opens, so it retries.
func mintInvite(binary, sock string) (string, error) {
	var out []byte
	var lastErr error
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		out, lastErr = exec.Command(binary, "cluster", "invite", "create", "--endpoint", sock).CombinedOutput()
		if lastErr == nil {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	if lastErr != nil {
		return "", fmt.Errorf("invite create: %w (out: %s)", lastErr, string(out))
	}
	seenPrompt := false
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if seenPrompt && line != "" {
			return line, nil
		}
		if strings.Contains(line, "GRAINFS_INVITE_BUNDLE") {
			seenPrompt = true
		}
	}
	return "", fmt.Errorf("could not parse invite bundle from output:\n%s", string(out))
}

func waitForFile(path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if _, err := os.Stat(path); err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("file %s did not appear within %v", path, timeout)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func waitHTTPReady(port int, timeout time.Duration) error {
	url := fmt.Sprintf("http://127.0.0.1:%d/", port)
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			return nil
		}
		lastErr = err
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("http port %d not ready within %v: %v", port, timeout, lastErr)
}

// bootstrapAdminSA POSTs to the admin UDS at sock to create the initial admin
// SA. Returns sa_id, access_key, secret_key. Mirrors
// tests/e2e/iam_helpers_test.go:bootstrapAdminViaUDS but with retry — the
// first SA-create on a fresh cluster only succeeds after the meta-Raft has
// elected a leader (typically <2s on the seed alone).
func bootstrapAdminSA(sock string, timeout time.Duration) (saID, ak, sk string, err error) {
	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, _addr string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", sock)
			},
		},
		Timeout: 10 * time.Second,
	}
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		body := strings.NewReader(`{"name":"admin","description":"colima cluster fixture bootstrap"}`)
		req, rerr := http.NewRequestWithContext(context.Background(), "POST",
			"http://unix/v1/iam/sa", body)
		if rerr != nil {
			return "", "", "", rerr
		}
		req.Header.Set("Content-Type", "application/json")
		resp, derr := client.Do(req)
		if derr != nil {
			lastErr = derr
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
			buf, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			lastErr = fmt.Errorf("bootstrap status %d: %s", resp.StatusCode, string(buf))
			time.Sleep(200 * time.Millisecond)
			continue
		}
		var out struct {
			SAID      string `json:"sa_id"`
			AccessKey string `json:"access_key"`
			SecretKey string `json:"secret_key"`
		}
		if jerr := json.NewDecoder(resp.Body).Decode(&out); jerr != nil {
			_ = resp.Body.Close()
			lastErr = jerr
			time.Sleep(200 * time.Millisecond)
			continue
		}
		_ = resp.Body.Close()
		if out.AccessKey == "" || out.SecretKey == "" {
			lastErr = fmt.Errorf("bootstrap returned empty creds")
			time.Sleep(200 * time.Millisecond)
			continue
		}
		return out.SAID, out.AccessKey, out.SecretKey, nil
	}
	return "", "", "", fmt.Errorf("bootstrap admin SA: %w", lastErr)
}

// waitForMembership polls the seed's admin UDS until Status reports a leader
// AND the membership set contains all raftPorts (i.e., every node has joined
// the meta-Raft). Returns the index of the current leader. Without this poll,
// HTTP / can answer before followers have actually been added to the raft
// configuration, which breaks downstream cross-node reads/writes.
func waitForMembership(sock string, raftPorts []int, timeout time.Duration) (int, error) {
	cli := clusteradmin.NewClient(sock)
	wantAddrs := make(map[string]int, len(raftPorts))
	for i, p := range raftPorts {
		wantAddrs[fmt.Sprintf("127.0.0.1:%d", p)] = i
	}
	deadline := time.Now().Add(timeout)
	var lastErr error
	var lastStatus string
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		status, err := cli.Status(ctx)
		cancel()
		if err != nil {
			lastErr = err
			time.Sleep(300 * time.Millisecond)
			continue
		}
		lastStatus = fmt.Sprintf("leader=%q peers=%v state=%s", status.LeaderID, status.Peers, status.State)
		if status.LeaderID == "" {
			lastErr = fmt.Errorf("no leader yet: %s", lastStatus)
			time.Sleep(300 * time.Millisecond)
			continue
		}
		// status.Peers excludes self (see internal/cluster/raftnode_adapter.go
		// Peers()), so a converged N-node cluster reports N-1 peers — all of
		// which must be members of the wantAddrs set.
		want := len(raftPorts) - 1
		seen := 0
		for _, p := range status.Peers {
			if _, ok := wantAddrs[p]; ok {
				seen++
			}
		}
		if seen < want {
			lastErr = fmt.Errorf("only %d/%d peers in membership: %s",
				seen, want, lastStatus)
			time.Sleep(300 * time.Millisecond)
			continue
		}
		// Find the leader idx.
		if idx, ok := wantAddrs[status.LeaderID]; ok {
			return idx, nil
		}
		// Leader is some node we don't know about — should not happen, but
		// fall back to 0 (seed) which is at least a valid raft node.
		return 0, nil
	}
	return 0, fmt.Errorf("membership not converged within %v: %v (last: %s)", timeout, lastErr, lastStatus)
}
