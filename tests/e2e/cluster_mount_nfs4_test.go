// Cluster-mount NFSv4 test: mounts NFSv4 from inside the colima VM against
// node 0, writes a file through the mount, then verifies the write is
// visible via signed S3 HEAD against every cluster node.
//
// Migrated from tests/nfs4_colima/cluster_mount_test.go.

package e2e

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/s3auth"
)

// colimaHTTPHeadObjectSigned issues a signed S3 HEAD against the given
// endpoint and returns the HTTP status code.
func colimaHTTPHeadObjectSigned(t *testing.T, endpoint, ak, sk, bucket, key string) int {
	t.Helper()
	u := fmt.Sprintf("%s/%s/%s", strings.TrimRight(endpoint, "/"), bucket, key)
	req, err := http.NewRequest(http.MethodHead, u, nil)
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

func TestColimaClusterNFS4WriteVisibleAcrossNodesE2E(t *testing.T) {
	c := getOrInitSharedColimaCluster(t)
	hostIP := envOrDefault("HOST_IP", "192.168.5.2")
	leaderDir := c.DataDirs[c.LeaderIdx]

	bucket := fmt.Sprintf("cluster-nfs4-write-%d", time.Now().UnixNano())

	// 1. Create bucket on leader.
	out, code := runColimaAdminCLI(t, leaderDir, "bucket", "create", bucket)
	require.Equalf(t, 0, code, "bucket create on leader: %s", out)

	// 2. Add NFS export for the bucket on leader. Replicated to followers via
	//    the meta-raft so every node's nfs4server serves the export.
	out, code = runColimaAdminCLI(t, leaderDir, "nfs", "export", "add", bucket)
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
			status := colimaHTTPHeadObjectSigned(t, c.HTTPURL(i), c.AccessKey, c.SecretKey, bucket, "cluster-test.txt")
			return status == http.StatusOK
		}, 60*time.Second, 500*time.Millisecond, "node %d did not observe cluster-test.txt", i)
	}
}
