// Cluster-mount 9P test: mounts 9P from inside the colima VM against each
// node sequentially and verifies a file written through node 0 is visible
// when re-mounting against the other cluster nodes.
//
// Migrated from tests/9p_colima/cluster_mount_test.go on the e2e
// classification pass.

package e2e

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/tests/colimafixture"
)

// mountColima9PNode mounts <bucket> via 9P from node `nodeIdx`'s P9 port,
// runs fn against the mountpoint, then unmounts.
func mountColima9PNode(t *testing.T, c *colimafixture.Cluster, nodeIdx int, bucket string, fn func(mnt string)) {
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

func TestColimaCluster9PWriteVisibleAcrossNodesE2E(t *testing.T) {
	c := getOrInitSharedColimaCluster(t)
	leaderDir := c.DataDirs[c.LeaderIdx]

	bucket := fmt.Sprintf("cluster-9p-write-%d", time.Now().UnixNano())

	// 1. Create bucket on leader (replicated via raft).
	out, code := runColimaAdminCLI(t, leaderDir, "bucket", "create", bucket)
	require.Equalf(t, 0, code, "bucket create on leader: %s", out)

	// 2. Ensure 9p kernel modules in colima VM (idempotent).
	colimaSSH("sudo", "modprobe", "9p").Run()           //nolint:errcheck
	colimaSSH("sudo", "modprobe", "9pnet").Run()        //nolint:errcheck
	colimaSSH("sudo", "modprobe", "9pnet_virtio").Run() //nolint:errcheck

	// 3. Mount via node 0's 9P port, write a file, unmount.
	const fileName = "cluster-9p.txt"
	const body = "cluster-9p-hello"
	mountColima9PNode(t, c, 0, bucket, func(mnt string) {
		runColimaSSH(t, "sh", "-c", fmt.Sprintf("echo -n %q | sudo tee %s/%s >/dev/null", body, mnt, fileName))
		got := runColimaSSH(t, "sudo", "cat", mnt+"/"+fileName)
		require.Equal(t, body, got, "readback on writer node")
	})

	// 4. Re-mount via every node (including 0) and verify the file is visible.
	//    Allow time for object replication via raft + storage layer.
	for i := 0; i < len(c.P9Ports); i++ {
		i := i
		require.Eventuallyf(t, func() bool {
			ok := false
			mountColima9PNode(t, c, i, bucket, func(mnt string) {
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
