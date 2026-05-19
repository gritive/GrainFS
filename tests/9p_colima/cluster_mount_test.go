//go:build colima

// Cluster-mount 9P test (Task 17): drives the shared 3-node colima cluster
// fixture, mounts 9P from inside the colima VM against each node sequentially,
// and verifies a file written through node 0's 9P port is visible when
// re-mounting against node 1 and node 2.
//
// Coexists with the single-node TestMain in 9p_colima_test.go: the cluster
// fixture is lazily started here under sync.Once so adding the cluster case
// does not perturb the single-node setup.

package p9_colima

import (
	"fmt"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/tests/colimafixture"
)

var (
	clusterOnce sync.Once
	clusterRef  *colimafixture.Cluster
)

func getClusterFixture(t *testing.T) *colimafixture.Cluster {
	t.Helper()
	clusterOnce.Do(func() {
		clusterRef = colimafixture.StartCluster(t, colimafixture.Options{
			EnableP9: true,
		})
	})
	return clusterRef
}

// mount9PNode mounts <bucket> via 9P from node `nodeIdx`'s P9 port, runs fn
// against the mountpoint, then unmounts.
func mount9PNode(t *testing.T, c *colimafixture.Cluster, nodeIdx int, bucket string, fn func(mnt string)) {
	t.Helper()
	hostIP := envOrDefault("HOST_IP", "192.168.5.2")
	mnt := fmt.Sprintf("/mnt/grainfs-9p-cluster-n%d-%d", nodeIdx, time.Now().UnixNano())
	runColimaSSH(t, "sudo", "mkdir", "-p", mnt)
	runColimaSSH(t, "sudo", "mount", "-t", "9p",
		"-o", fmt.Sprintf("trans=tcp,port=%d,version=9p2000.L,msize=262144,aname=/%s", c.P9Ports[nodeIdx], bucket),
		hostIP, mnt)
	defer func() {
		colimaSSH("sudo", "umount", "-l", mnt).Run() //nolint:errcheck
		colimaSSH("sudo", "rmdir", mnt).Run()        //nolint:errcheck
	}()
	fn(mnt)
}

func TestP9Cluster_WriteVisibleAcrossNodes(t *testing.T) {
	c := getClusterFixture(t)
	leaderDir := c.DataDirs[c.LeaderIdx]

	bucket := fmt.Sprintf("cluster-9p-write-%d", time.Now().UnixNano())

	// 1. Create bucket on leader (replicated via raft).
	binary := envOrDefault("GRAINFS_BINARY", "../../bin/grainfs")
	cmdOut, err := exec.Command(binary, "bucket", "create", bucket, "--endpoint", leaderDir+"/admin.sock").CombinedOutput()
	require.NoErrorf(t, err, "bucket create on leader: %s", cmdOut)

	// 2. Ensure 9p kernel modules in colima VM (idempotent).
	colimaSSH("sudo", "modprobe", "9p").Run()           //nolint:errcheck
	colimaSSH("sudo", "modprobe", "9pnet").Run()        //nolint:errcheck
	colimaSSH("sudo", "modprobe", "9pnet_virtio").Run() //nolint:errcheck

	// 3. Mount via node 0's 9P port, write a file, unmount.
	const fileName = "cluster-9p.txt"
	const body = "cluster-9p-hello"
	mount9PNode(t, c, 0, bucket, func(mnt string) {
		writeMountedFile(t, mnt+"/"+fileName, body)
		got := runColimaSSH(t, "sudo", "cat", mnt+"/"+fileName)
		require.Equal(t, body, got, "readback on writer node")
	})

	// 4. Re-mount via every node (including 0) and verify the file is visible.
	//    Allow time for object replication via raft + storage layer.
	for i := 0; i < len(c.P9Ports); i++ {
		i := i
		require.Eventuallyf(t, func() bool {
			ok := false
			mount9PNode(t, c, i, bucket, func(mnt string) {
				out, sshErr := colimaSSH("sudo", "cat", mnt+"/"+fileName).CombinedOutput()
				if sshErr != nil {
					return
				}
				if string(out) == body {
					ok = true
				}
			})
			return ok
		}, 60*time.Second, 1*time.Second, "node %d did not observe %s", i, fileName)
	}
}
