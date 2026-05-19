// Shared colima 3-node cluster fixture for cluster_mount_{9p,nbd,nfs4}
// tests. Single sync.Once boot drives all three protocols against the
// same cluster — net 3-boot to 1-boot reduction vs the old per-package
// fixtures under tests/{9p,nbd,nfs4}_colima/cluster_mount_test.go.
//
// Cleanup contract: colimafixture.StartCluster is called with
// SkipCleanup=true so the fixture survives the first caller's t.Cleanup.
// TestMain teardown invokes shutdownSharedColimaCluster() to stop the
// cluster at process exit.
//
// Colima dependency: cluster boots with --9p-bind/--nfs4-bind/--nbd-bind
// 0.0.0.0 so the colima VM can reach each protocol port via the host
// bridge IP. Tests fail when colima is not running — same policy as
// the NFSv4 mount block in multiraft_sharding_test.go.

package e2e

import (
	"os"
	"os/exec"
	"sync"
	"testing"

	"github.com/gritive/GrainFS/tests/colimafixture"
)

var (
	colimaClusterOnce sync.Once
	colimaClusterRef  *colimafixture.Cluster
)

// getOrInitSharedColimaCluster returns the process-global colima cluster
// fixture. The boot enables 9P + NBD + NFSv4 so that all three protocol-
// specific cluster_mount tests share a single 3-node grainfs process group.
//
// Cleanup is registered on the first caller's t via colimafixture.StartCluster.
// All cluster_mount tests in this package share that cleanup since they
// run inside the same test binary.
func getOrInitSharedColimaCluster(t *testing.T) *colimafixture.Cluster {
	t.Helper()
	colimaClusterOnce.Do(func() {
		colimaClusterRef = colimafixture.StartCluster(t, colimafixture.Options{
			EnableP9:    true,
			EnableNBD:   true,
			EnableNFS:   true,
			SkipCleanup: true,
		})
	})
	if colimaClusterRef == nil {
		t.Fatal("colima cluster fixture initialization failed")
	}
	return colimaClusterRef
}

// shutdownSharedColimaCluster stops the shared colima cluster if one was
// booted. Safe to call when no test triggered initialization. Invoked from
// TestMain teardown.
func shutdownSharedColimaCluster() {
	if colimaClusterRef != nil {
		colimaClusterRef.Stop()
	}
}

// envOrDefault returns os.Getenv(key) when set, else def. Lifted verbatim
// from the per-package colima single-node test files so that the merged
// cluster_mount tests stay self-contained when those packages eventually
// fold their single-node variants into tests/e2e/ as well.
func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// runColimaAdminCLI invokes the grainfs binary against a specific node's
// admin UDS. Returns combined stdout+stderr and exit code. Shared by the
// 9P / NBD / NFSv4 cluster_mount tests.
func runColimaAdminCLI(t *testing.T, dataDir string, args ...string) (string, int) {
	t.Helper()
	binary := envOrDefault("GRAINFS_BINARY", "../../bin/grainfs")
	full := append(append([]string{}, args...), "--endpoint", dataDir+"/admin.sock")
	cmd := exec.Command(binary, full...)
	out, _ := cmd.CombinedOutput()
	code := 0
	if cmd.ProcessState != nil {
		code = cmd.ProcessState.ExitCode()
	}
	return string(out), code
}
