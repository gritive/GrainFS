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
	"crypto/rand"
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
)

// Cluster is the handle returned by StartCluster. All ports bind 0.0.0.0 on
// the host, so colima VM clients can reach them via 192.168.5.2:<port>.
type Cluster struct {
	HTTPPorts []int    // S3/admin HTTP ports (bind :%d on host)
	RaftPorts []int    // QUIC raft ports
	NFSPorts  []int    // NFSv4 ports (bind :%d)
	NBDPorts  []int    // NBD ports (bind :%d)
	P9Ports   []int    // 9P2000.L ports (bind 0.0.0.0 via --9p-bind)
	DataDirs  []string // per-node data directories (temp)

	LeaderIdx int    // 0 — seed is always the initial leader at boot
	AccessKey string // bootstrap admin SA access key
	SecretKey string // bootstrap admin SA secret key
	SAID      string // bootstrap admin SA id (for grant calls)

	procs   []*exec.Cmd
	logs    []*os.File
	tempEnc string // encryption key file (shared across nodes)
	stopped bool
}

// Options configure cluster boot.
type Options struct {
	// NumNodes is the cluster size. Defaults to 3 when 0.
	NumNodes int

	// EnableNFS enables NFSv4 listeners (binds 0.0.0.0 by default).
	EnableNFS bool

	// EnableNBD enables NBD listeners (binds 0.0.0.0 by default).
	EnableNBD bool

	// EnableP9 enables 9P listeners with --9p-bind 0.0.0.0 so the colima VM
	// can reach them. Off by default — only set true if the test mounts 9P.
	EnableP9 bool

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
		NFSPorts:  make([]int, numNodes),
		NBDPorts:  make([]int, numNodes),
		P9Ports:   make([]int, numNodes),
		DataDirs:  make([]string, numNodes),
		procs:     make([]*exec.Cmd, numNodes),
		logs:      make([]*os.File, numNodes),
		tempEnc:   makeEncryptionKeyFile(t),
	}
	if !opts.SkipCleanup {
		t.Cleanup(c.Stop)
	}

	// Allocate up to 5 ports/node (http, raft, nfs4, nbd, 9p). Even when a
	// protocol is disabled we still reserve the slot so port indices are
	// stable; serve will simply not listen on a `0` port.
	ports := uniqueFreePorts(t, numNodes*5)
	for i := 0; i < numNodes; i++ {
		c.HTTPPorts[i] = ports[i]
		c.RaftPorts[i] = ports[numNodes+i]
		if opts.EnableNFS {
			c.NFSPorts[i] = ports[2*numNodes+i]
		}
		if opts.EnableNBD {
			c.NBDPorts[i] = ports[3*numNodes+i]
		}
		if opts.EnableP9 {
			c.P9Ports[i] = ports[4*numNodes+i]
		}
		dir, err := os.MkdirTemp("", fmt.Sprintf("grainfs-colima-cluster-%d-*", i))
		if err != nil {
			c.Stop()
			t.Fatalf("mkdtemp node %d: %v", i, err)
		}
		c.DataDirs[i] = dir
	}

	clusterKey := "COLIMA-FIXTURE-CLUSTER-KEY"

	// Seed (node 0) starts first.
	c.procs[0], c.logs[0] = c.spawn(t, binary, 0, clusterKey)
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

	// §7 B3 — followers must have the cluster KEK + cluster identity in
	// place BEFORE serve boots; otherwise wireDEKKeeper refuses startup in
	// join mode. Phase A binds the join handshake to per-cluster KEK +
	// cluster_id, so both files must be staged. Mirror the real operator
	// workflow (scp <peer>:<data>/keys/0.key and <peer>:<data>/cluster.id)
	// by copying the seed's keystore + identity into each follower's data
	// dir.
	seedKEK := filepath.Join(c.DataDirs[0], "keys", "0.key")
	if err := waitForFile(seedKEK, 30*time.Second); err != nil {
		c.Stop()
		t.Fatalf("seed keys/0.key did not appear: %v", err)
	}
	seedClusterID := filepath.Join(c.DataDirs[0], "cluster.id")
	if err := waitForFile(seedClusterID, 30*time.Second); err != nil {
		c.Stop()
		t.Fatalf("seed cluster.id did not appear: %v", err)
	}
	// Followers join via .join-pending file containing seed raft addr.
	seedRaftAddr := fmt.Sprintf("127.0.0.1:%d", c.RaftPorts[0])
	for i := 1; i < numNodes; i++ {
		if err := os.MkdirAll(filepath.Join(c.DataDirs[i], "keys"), 0o700); err != nil {
			c.Stop()
			t.Fatalf("mkdir follower %d keys dir: %v", i, err)
		}
		if err := copyFileMode(seedKEK, filepath.Join(c.DataDirs[i], "keys", "0.key"), 0o600); err != nil {
			c.Stop()
			t.Fatalf("copy keys/0.key to follower %d: %v", i, err)
		}
		if err := copyFileMode(seedClusterID, filepath.Join(c.DataDirs[i], "cluster.id"), 0o600); err != nil {
			c.Stop()
			t.Fatalf("copy cluster.id to follower %d: %v", i, err)
		}
		if err := writeJoinPending(c.DataDirs[i], seedRaftAddr); err != nil {
			c.Stop()
			t.Fatalf("write .join-pending node %d: %v", i, err)
		}
		c.procs[i], c.logs[i] = c.spawn(t, binary, i, clusterKey)
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
	if c.tempEnc != "" {
		_ = os.Remove(c.tempEnc)
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
func (c *Cluster) spawn(t testing.TB, binary string, i int, clusterKey string) (*exec.Cmd, *os.File) {
	t.Helper()
	raftAddr := fmt.Sprintf("127.0.0.1:%d", c.RaftPorts[i])
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
		"--cluster-key", clusterKey,
		"--encryption-key-file", c.tempEnc,
		"--nfs4-port", fmt.Sprintf("%d", c.NFSPorts[i]),
		"--nbd-port", fmt.Sprintf("%d", c.NBDPorts[i]),
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
	}
	if c.P9Ports[i] > 0 {
		args = append(args,
			"--9p-bind", "0.0.0.0",
			"--9p-port", fmt.Sprintf("%d", c.P9Ports[i]),
		)
	}
	cmd := exec.Command(binary, args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		_ = os.Remove(logFile.Name())
		t.Fatalf("start node %d: %v", i, err)
	}
	return cmd, logFile
}

// uniqueFreePorts picks n ports where both TCP and UDP are free. The fixture
// uses TCP for HTTP/NFS/NBD/9P and UDP for QUIC raft, so checking TCP alone can
// choose a port that later fails the raft listener with EADDRINUSE.
func uniqueFreePorts(t testing.TB, n int) []int {
	t.Helper()
	tcpListeners := make([]net.Listener, 0, n)
	udpListeners := make([]net.PacketConn, 0, n)
	ports := make([]int, 0, n)
	for len(ports) < n {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			for _, prev := range tcpListeners {
				_ = prev.Close()
			}
			for _, prev := range udpListeners {
				_ = prev.Close()
			}
			t.Fatalf("listen :0: %v", err)
		}
		port := ln.Addr().(*net.TCPAddr).Port
		pc, err := net.ListenPacket("udp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			_ = ln.Close()
			continue
		}
		tcpListeners = append(tcpListeners, ln)
		udpListeners = append(udpListeners, pc)
		ports = append(ports, port)
	}
	for _, ln := range tcpListeners {
		_ = ln.Close()
	}
	for _, pc := range udpListeners {
		_ = pc.Close()
	}
	return ports
}

func makeEncryptionKeyFile(t testing.TB) string {
	t.Helper()
	f, err := os.CreateTemp("", "grainfs-colima-enckey-*")
	if err != nil {
		t.Fatalf("create enckey: %v", err)
	}
	var key [32]byte
	if _, err := rand.Read(key[:]); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		t.Fatalf("read rand: %v", err)
	}
	if _, err := f.Write(key[:]); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		t.Fatalf("write enckey: %v", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(f.Name())
		t.Fatalf("close enckey: %v", err)
	}
	return f.Name()
}

// writeJoinPending mirrors writeNodeJoinPending from
// tests/e2e/cluster_harness_test.go: a 0o600 file at <dataDir>/.join-pending
// whose contents are the seed's raft addr ("127.0.0.1:<raftPort>"). The
// grainfs binary detects this on boot and joins instead of starting a new
// cluster.
func writeJoinPending(dataDir, seedRaftAddr string) error {
	return os.WriteFile(filepath.Join(dataDir, ".join-pending"), []byte(seedRaftAddr), 0o600)
}

// copyFileMode copies src into dst with the given permission. Used by the
// cluster fixture to plant the seed's keys/0.key + cluster.id on follower
// data dirs before they boot in join mode — wireDEKKeeper refuses to
// auto-generate a KEK on a joiner (§7 B3 / F#21) and Phase A binds the
// handshake to the cluster identity.
func copyFileMode(src, dst string, mode os.FileMode) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, data, mode)
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
		if err := seedBootstrapTrustedProxyCIDR(client, sock); err != nil {
			lastErr = err
			time.Sleep(200 * time.Millisecond)
			continue
		}
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

func seedBootstrapTrustedProxyCIDR(client *http.Client, sock string) error {
	body := strings.NewReader(`{"value":"127.0.0.1/32"}`)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPut,
		"http://unix/v1/config/trusted-proxy.cidr", body)
	if err != nil {
		return fmt.Errorf("build trusted-proxy.cidr seed: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("seed trusted-proxy.cidr: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		buf, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("seed trusted-proxy.cidr on %s -> %d: %s", sock, resp.StatusCode, string(buf))
	}
	return nil
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
