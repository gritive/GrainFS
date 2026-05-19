//go:build colima

// Cluster-mount NFSv4 test (Task 15b): drives the shared 3-node colima
// cluster fixture, mounts NFSv4 from inside the colima VM against node 0,
// writes a file through the mount, then verifies the write is visible via
// signed S3 GET against every cluster node.
//
// Coexists with the single-node TestMain in nfs4_colima_test.go: the cluster
// fixture is lazily started here under sync.Once so adding the cluster case
// does not perturb the single-node setup. If colima is not running, TestMain
// already exits 0 before any test runs.

package nfs4_colima

import (
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/tests/colimafixture"
)

var (
	clusterOnce sync.Once
	clusterRef  *colimafixture.Cluster
)

// getClusterFixture lazy-boots the 3-node cluster on first use. The fixture
// registers its own t.Cleanup via the FIRST t that triggers the boot — that
// t may finish before sibling cluster tests; later tests reuse the still-live
// cluster (Stop becomes a no-op once stopped is true, but stopped only flips
// when the cleanup actually fires; testing.T cleanups run at the END of the
// process for tests sharing this package, AFTER all tests in this test
// binary complete). For our single-test usage this is fine.
func getClusterFixture(t *testing.T) *colimafixture.Cluster {
	t.Helper()
	clusterOnce.Do(func() {
		clusterRef = colimafixture.StartCluster(t, colimafixture.Options{
			EnableNFS: true,
		})
	})
	return clusterRef
}

// runAdminCLI invokes the grainfs binary against a specific node's admin UDS.
// Returns combined stdout+stderr and exit code.
func runAdminCLI(t *testing.T, dataDir string, args ...string) (string, int) {
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

// httpHeadObjectSigned issues a signed S3 HEAD against the given endpoint and
// returns the HTTP status code.
func httpHeadObjectSigned(t *testing.T, endpoint, ak, sk, bucket, key string) int {
	t.Helper()
	url := fmt.Sprintf("%s/%s/%s", strings.TrimRight(endpoint, "/"), bucket, key)
	req, err := http.NewRequest(http.MethodHead, url, nil)
	require.NoError(t, err)
	s3auth.SignRequest(req, ak, sk, "us-east-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func TestNFS4Cluster_WriteVisibleAcrossNodes(t *testing.T) {
	c := getClusterFixture(t)
	hostIP := envOrDefault("HOST_IP", "192.168.5.2")
	leaderDir := c.DataDirs[c.LeaderIdx]

	bucket := fmt.Sprintf("cluster-nfs4-write-%d", time.Now().UnixNano())

	// 1. Create bucket on leader.
	out, code := runAdminCLI(t, leaderDir, "bucket", "create", bucket)
	require.Equalf(t, 0, code, "bucket create on leader: %s", out)

	// 2. Add NFS export for the bucket on leader. Replicated to followers via
	//    the meta-raft so every node's nfs4server serves the export.
	out, code = runAdminCLI(t, leaderDir, "nfs", "export", "add", bucket)
	require.Equalf(t, 0, code, "nfs export add: %s", out)

	// 3. Mount NFSv4 in the colima VM against node 0, write a file, unmount.
	mnt := fmt.Sprintf("/mnt/grainfs-nfs4-cluster-%d", time.Now().UnixNano())
	mountCmd := fmt.Sprintf(
		"set -e; sudo mkdir -p %s && "+
			"sudo mount -t nfs4 -o vers=4.1,port=%d %s:/%s %s && "+
			"echo cluster-hello | sudo tee %s/cluster-test.txt >/dev/null && "+
			"sudo sync && sudo umount %s",
		mnt, c.NFSPorts[0], hostIP, bucket, mnt, mnt, mnt,
	)
	t.Cleanup(func() {
		colimaSSH("sh", "-c", fmt.Sprintf("sudo umount -l %s 2>/dev/null; sudo rmdir %s 2>/dev/null", mnt, mnt)).Run() //nolint:errcheck
	})
	out2 := runColimaSSH(t, "sh", "-c", mountCmd)
	t.Logf("mount+write output: %s", out2)

	// 4. Verify via signed S3 HEAD on every node. Allow time for raft
	//    propagation of the object metadata to followers.
	for i := 0; i < len(c.HTTPPorts); i++ {
		i := i
		require.Eventuallyf(t, func() bool {
			status := httpHeadObjectSigned(t, c.HTTPURL(i), c.AccessKey, c.SecretKey, bucket, "cluster-test.txt")
			return status == http.StatusOK
		}, 60*time.Second, 500*time.Millisecond, "node %d did not observe cluster-test.txt", i)
	}
}
