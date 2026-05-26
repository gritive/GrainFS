package e2e

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// startUnbootstrappedSingleNode spawns a single-node grainfs binary like
// startIsolatedE2EServer but skips the admin SA bootstrap. Bootstrap-test
// runners drive the first SA creation themselves (or omit it, for pre-bootstrap
// checks). Returns (dataDir, s3URL, adminSock, port).
func startUnbootstrappedSingleNode(t testing.TB) (dataDir, s3URL, adminSock string, port int) {
	t.Helper()

	dir, err := os.MkdirTemp("", "grainfs-e2e-bootstrap-*")
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "mkdtemp")
	ginkgo.DeferCleanup(func() { _ = removeE2EDir(dir) })

	port = freePort()
	cmd := exec.Command(getBinary(), "serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", port),
		"--nfs4-port", fmt.Sprintf("%d", freePort()),
		"--nbd-port", fmt.Sprintf("%d", freePort()),
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
		"--cluster-key", "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	gomega.Expect(cmd.Start()).To(gomega.Succeed(), "start unbootstrapped e2e server")
	ginkgo.DeferCleanup(func() { terminateProcess(cmd) })

	s3URL = fmt.Sprintf("http://127.0.0.1:%d", port)
	waitForPort(t, port, 30*time.Second)
	adminSock = filepath.Join(dir, "admin.sock")

	// Wait for admin.sock then disable auto-snapshot for deterministic e2e
	// behavior. Tests that need the auto-snapshot loop PATCH it back to a
	// non-zero interval explicitly. PATCH /v1/cluster/config does not require
	// IAM bootstrap, so it works even though this helper intentionally skips
	// admin SA creation.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(adminSock); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("admin socket %s did not appear within 10s", adminSock)
		}
		time.Sleep(50 * time.Millisecond)
	}
	gomega.Expect(patchSnapshotIntervalM(dir, "0s")).To(gomega.Succeed(), "disable auto-snapshot")
	return dir, s3URL, adminSock, port
}

// startUnbootstrappedCluster spawns a fresh N-node cluster with no admin SA
// bootstrapped. Returns the *e2eCluster; cleanup is registered via
// startE2ECluster's t.Cleanup. Admin UDS sock paths are
// c.dataDirs[i]+"/admin.sock".
func startUnbootstrappedCluster(t testing.TB, nodes int) *e2eCluster {
	t.Helper()
	return startE2ECluster(t, e2eClusterOptions{
		Nodes:       nodes,
		Mode:        ClusterModeDynamicJoin,
		NoBootstrap: true,
	})
}

// iamBootstrapTarget abstracts a fixture (single-node or N-node cluster) for
// e2e tests that exercise the IAM bootstrap path. Parallel to iamAdminTarget
// for tests that start with an empty IAM store.
type iamBootstrapTarget struct {
	name      string // "single" or "cluster4"
	nodes     int
	s3URL     func() string // leader or only node S3 HTTP endpoint
	adminSock func() string // single: dataDir/admin.sock; cluster: leader's admin.sock
	dataDir   func() string // single: dataDir; cluster: leader dataDir
	dataDirs  func() []string
	isCluster bool
	cluster   *e2eCluster // non-nil for cluster
}

// newSingleNodeBootstrapTarget returns an iamBootstrapTarget backed by a
// freshly spawned unbootstrapped single-node server. Cleanup is registered
// via t.Cleanup inside startUnbootstrappedSingleNode.
func newSingleNodeBootstrapTarget(t testing.TB) iamBootstrapTarget {
	t.Helper()
	dir, url, sock, _ := startUnbootstrappedSingleNode(t)
	return iamBootstrapTarget{
		name:      "single",
		nodes:     1,
		s3URL:     func() string { return url },
		adminSock: func() string { return sock },
		dataDir:   func() string { return dir },
		dataDirs:  func() []string { return []string{dir} },
		isCluster: false,
	}
}

// newClusterBootstrapTarget returns an iamBootstrapTarget backed by a freshly
// spawned 4-node cluster with no admin SA. Cleanup is registered via
// startE2ECluster's t.Cleanup.
func newClusterBootstrapTarget(t testing.TB) iamBootstrapTarget {
	t.Helper()
	c := startUnbootstrappedCluster(t, 4)
	return iamBootstrapTarget{
		name:      "cluster4",
		nodes:     4,
		s3URL:     func() string { return c.httpURLs[c.leaderIdx] },
		adminSock: func() string { return c.dataDirs[c.leaderIdx] + "/admin.sock" },
		dataDir:   func() string { return c.dataDirs[c.leaderIdx] },
		dataDirs:  func() []string { return append([]string(nil), c.dataDirs...) },
		isCluster: true,
		cluster:   c,
	}
}
