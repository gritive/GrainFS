package e2e

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/hugelgupf/p9/p9"
	"github.com/onsi/gomega"
)

// p9Target abstracts a grainfs fixture exposing both an admin UDS and a 9P TCP
// port. NFS§B T14 §B checkpoint tests use this to exercise 9P attach/walk over
// the wire on both SingleNode and Cluster3Node topologies.
//
// p9Addr returns the host:port to dial; adminSock is the leader admin.sock
// path (for cluster, leader-resident — the FSM replicates state to followers
// for the cluster sub-tests that walk against a follower's 9P port).
type p9Target struct {
	name      string
	p9Addr    func(i int) string
	adminSock func(i int) string
	s3URL     func(i int) string
	dataDirs  func() []string
	nodeCount int
	isCluster bool
}

// newSingleNodeP9Target starts a dedicated single-node grainfs with --9p-port
// wired. Phase 0 (no admin bootstrap) — callers that need Phase 2 must
// bootstrap explicitly via flipToPhase2 or iamCreateSA.
func newSingleNodeP9Target(t testing.TB) *p9Target {
	t.Helper()

	dir, err := os.MkdirTemp("", "grainfs-p9-e2e-*")
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	httpPort := freePort()
	p9Port := freePort()
	cmd := exec.Command(getBinary(), "serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", httpPort),
		"--nfs4-port", fmt.Sprintf("%d", freePort()),
		"--nbd-port", fmt.Sprintf("%d", freePort()),
		"--9p-bind", "127.0.0.1",
		"--9p-port", fmt.Sprintf("%d", p9Port),
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
		"--cluster-key", "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899",
	)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	gomega.Expect(cmd.Start()).ToNot(gomega.HaveOccurred(), "start single-node 9P e2e server")
	t.Cleanup(func() { terminateProcess(cmd) })

	waitForPort(t, httpPort, 30*time.Second)
	waitForPort(t, p9Port, 30*time.Second)
	sock := filepath.Join(dir, "admin.sock")
	requireFileWithin(t, sock, 10*time.Second)
	// Disable auto-snapshot for deterministic behavior. PATCH does not require
	// IAM bootstrap.
	gomega.Expect(patchSnapshotIntervalM(dir, "0s")).ToNot(gomega.HaveOccurred())

	s3URL := fmt.Sprintf("http://127.0.0.1:%d", httpPort)
	p9Endpoint := fmt.Sprintf("127.0.0.1:%d", p9Port)

	return &p9Target{
		name:      "single",
		p9Addr:    func(i int) string { return p9Endpoint },
		adminSock: func(i int) string { return sock },
		s3URL:     func(i int) string { return s3URL },
		dataDirs:  func() []string { return []string{dir} },
		nodeCount: 1,
		isCluster: false,
	}
}

// newClusterP9Target boots a 3-node mrCluster with --9p-port wired on every
// node. Cluster is bootstrapped (Phase 2) — anon attach to /default is NOT
// possible (the 9P attach gate does not honor S3's default-bucket implicit-anon
// allow path; see commit body for F-§B-9P-anon-attach-phase2 finding).
// Cluster3Node sub-contexts therefore cover only MountSA Miss/NoPolicy cases
// which exercise the resolver pre-check + authorizer deny path, both of which
// are Phase-agnostic.
//
// Caller must be in a Ginkgo node (BeforeAll/BeforeEach) — newMRCluster
// registers cleanup via ginkgo.DeferCleanup.
func newClusterP9Target(t testing.TB) *p9Target {
	t.Helper()
	c := startStaticMRClusterWithOptions(t, 3, mrClusterOptions{
		disableNBD:    true,
		disableNFS4:   true,
		enableP9:      true,
		FastBootstrap: true,
	})

	// Snapshot per-node addresses for closure capture. Leader can shift but
	// p9 ports and admin socks are tied to data dirs; the leader's admin sock
	// is where MountSA writes must land.
	p9Addrs := make([]string, 3)
	socks := make([]string, 3)
	urls := make([]string, 3)
	dataDirs := append([]string(nil), c.dataDirs...)
	for i := 0; i < 3; i++ {
		p9Addrs[i] = fmt.Sprintf("127.0.0.1:%d", c.p9Ports[i])
		socks[i] = c.dataDirs[i] + "/admin.sock"
		urls[i] = c.httpURLs[i]
	}

	return &p9Target{
		name:   "cluster3",
		p9Addr: func(i int) string { return p9Addrs[i%3] },
		adminSock: func(i int) string {
			// Admin writes target the current leader's sock; reads from any.
			return c.dataDirs[c.leaderIdx] + "/admin.sock"
		},
		s3URL:     func(i int) string { return urls[i%3] },
		dataDirs:  func() []string { return dataDirs },
		nodeCount: 3,
		isCluster: true,
	}
}

// dialP9 opens a hugelgupf/p9 client connected to the target's 9P port.
// Caller must Close() the returned client.
func dialP9(t testing.TB, tgt *p9Target, nodeIdx int) *p9.Client {
	t.Helper()
	conn, err := net.DialTimeout("tcp", tgt.p9Addr(nodeIdx), 5*time.Second)
	gomega.Expect(err).ToNot(gomega.HaveOccurred(), "dial 9p %s", tgt.p9Addr(nodeIdx))
	cli, err := p9.NewClient(conn, p9.WithMessageSize(64*1024))
	gomega.Expect(err).ToNot(gomega.HaveOccurred(), "p9 NewClient")
	return cli
}

// attachP9 issues a Tattach with the given aname. Caller is responsible for
// closing the returned File (when err == nil).
func attachP9(t testing.TB, tgt *p9Target, nodeIdx int, aname string) (p9.File, *p9.Client, error) {
	t.Helper()
	cli := dialP9(t, tgt, nodeIdx)
	f, err := cli.Attach(aname)
	if err != nil {
		_ = cli.Close()
		return nil, nil, err
	}
	return f, cli, nil
}

// requireFileWithin polls for path to exist up to timeout.
func requireFileWithin(t testing.TB, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("file %s did not appear within %v", path, timeout)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// closeP9File best-effort closes a p9 file; ignores errors. Useful in defers.
func closeP9File(f p9.File) {
	if f != nil {
		_ = f.Close()
	}
}

// drainP9 reads up to maxBytes from a p9.File starting at offset 0 and returns
// the bytes. Used by Content-Type test to read object contents.
func drainP9(f p9.File, maxBytes int) ([]byte, error) {
	buf := make([]byte, maxBytes)
	n, err := f.ReadAt(buf, 0)
	if err != nil && err != io.EOF {
		return nil, err
	}
	return buf[:n], nil
}

// ensureBootstrapped guarantees the target's admin UDS has an admin SA
// (Phase 2). For cluster targets this is a no-op (cluster fixture bootstraps
// at start). For single-node Phase 0 targets, this seeds trusted-proxy.cidr
// and creates the first SA, which atomically flips iam.anon-enabled to false
// (Phase 2 magical-moment transition — F#26).
func ensureBootstrapped(t testing.TB, tgt *p9Target) {
	t.Helper()
	if tgt.isCluster {
		return // already bootstrapped via startMRCluster
	}
	sock := tgt.adminSock(0)
	if isAdminBootstrapped(sock) {
		return
	}
	// Seed trusted-proxy.cidr so the F#26-tls-posture pre-check accepts the
	// first SA create on a loopback fixture without TLS.
	gomega.Expect(seedBootstrapTrustedProxyCIDR(sock)).ToNot(gomega.HaveOccurred())
	_, _, err := tryBootstrapAdminViaUDS(sock)
	gomega.Expect(err).ToNot(gomega.HaveOccurred(), "bootstrap admin SA for p9 e2e")
}

// isAdminBootstrapped reports whether the admin UDS already has an admin SA.
// Used by ensureBootstrapped to avoid creating a duplicate.
func isAdminBootstrapped(sock string) bool {
	cli := iamadminClientForSock(sock)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	items, err := cli.SAList(ctx)
	if err != nil {
		return false
	}
	return len(items) > 0
}
