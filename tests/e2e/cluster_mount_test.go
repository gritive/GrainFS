// Cluster-mount tests run from inside a colima VM against the shared 3-node
// colima cluster fixture. Each sub-test exercises one mount protocol
// (9P, NBD, NFSv4) and verifies cluster-wide visibility through the data
// plane (signed S3 ListObjects / HEAD or re-mount on a peer node).

package e2e

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/tests/colimafixture"
)

// Constants kept aligned with the project's NBD volume default. See
// internal/nbd/handshake.go (exportNameMatches) — the NBD server only
// accepts the single fixed "default" export.
const (
	colimaNBDVolSize = "8388608" // 8MiB; covers a single 4KiB block PUT comfortably
	colimaNBDDev     = "/dev/nbd0"
)

type colimaListBucketResultV2 struct {
	XMLName  xml.Name `xml:"ListBucketResult"`
	KeyCount int      `xml:"KeyCount"`
	Contents []struct {
		Key string `xml:"Key"`
	} `xml:"Contents"`
}

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

// colimaListVolumeObjects performs a signed S3 ListObjectsV2 against the
// given endpoint and returns the number of objects under __vol/default/.
// Returns (-1, error) on transport/HTTP error so callers can distinguish
// a real failure from "not yet replicated".
func colimaListVolumeObjects(t *testing.T, endpoint, ak, sk string) (int, error) {
	t.Helper()
	u := fmt.Sprintf("%s/%s?list-type=2&prefix=%s", strings.TrimRight(endpoint, "/"),
		"__grainfs_volumes", url.QueryEscape("__vol/default/"))
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return -1, err
	}
	s3auth.SignRequest(req, ak, sk, "us-east-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return -1, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return -1, fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
	}
	var parsed colimaListBucketResultV2
	if err := xml.Unmarshal(body, &parsed); err != nil {
		return -1, fmt.Errorf("xml parse: %w", err)
	}
	if parsed.KeyCount > 0 {
		return parsed.KeyCount, nil
	}
	return len(parsed.Contents), nil
}

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

// TestColimaClusterMountE2E exercises 9P, NBD, and NFSv4 mount paths from
// inside the colima VM against a shared 3-node colima cluster fixture.
// Each sub-test verifies cluster-wide visibility through the data plane.
func TestColimaClusterMountE2E(t *testing.T) {
	t.Run("Cluster3Node", func(t *testing.T) {
		c := getOrInitSharedColimaCluster(t)
		runColimaClusterMountCases(t, c)
	})
}

func runColimaClusterMountCases(t *testing.T, c *colimafixture.Cluster) {
	t.Helper()
	leaderDir := c.DataDirs[c.LeaderIdx]
	hostIP := envOrDefault("HOST_IP", "192.168.5.2")

	t.Run("9PWriteVisibleAcrossNodes", func(t *testing.T) {
		bucket := fmt.Sprintf("cluster-9p-write-%d", time.Now().UnixNano())

		out, code := runColimaAdminCLI(t, leaderDir, "bucket", "create", bucket)
		require.Equalf(t, 0, code, "bucket create on leader: %s", out)

		colimaSSH("sudo", "modprobe", "9p").Run()           //nolint:errcheck
		colimaSSH("sudo", "modprobe", "9pnet").Run()        //nolint:errcheck
		colimaSSH("sudo", "modprobe", "9pnet_virtio").Run() //nolint:errcheck

		const fileName = "cluster-9p.txt"
		const body = "cluster-9p-hello"
		mountColima9PNode(t, c, 0, bucket, func(mnt string) {
			runColimaSSH(t, "sh", "-c", fmt.Sprintf("echo -n %q | sudo tee %s/%s >/dev/null", body, mnt, fileName))
			got := runColimaSSH(t, "sudo", "cat", mnt+"/"+fileName)
			require.Equal(t, body, got, "readback on writer node")
		})

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
	})

	t.Run("NBDWriteReplicatesAcrossNodes", func(t *testing.T) {
		out, code := runColimaAdminCLI(t, leaderDir, "volume", "create", "default", "--size", colimaNBDVolSize)
		require.Equalf(t, 0, code, "volume create on leader: %s", out)

		colimaSSH("sudo", "modprobe", "nbd", "max_part=0").Run()  //nolint:errcheck
		colimaSSH("sudo", "nbd-client", "-d", colimaNBDDev).Run() //nolint:errcheck
		time.Sleep(300 * time.Millisecond)

		runColimaSSH(t, "sudo", "nbd-client",
			hostIP, fmt.Sprintf("%d", c.NBDPorts[0]), colimaNBDDev,
			"-b", "4096", "-N", "default",
		)
		t.Cleanup(func() {
			colimaSSH("sudo", "nbd-client", "-d", colimaNBDDev).Run() //nolint:errcheck
		})

		runColimaSSH(t, "sudo", "bash", "-c",
			fmt.Sprintf("python3 -c \"import sys; sys.stdout.buffer.write(b'\\xde'*4096)\" | dd of=%s bs=4096 count=1 conv=fsync 2>/dev/null", colimaNBDDev))
		runColimaSSH(t, "sudo", "python3", "-c",
			fmt.Sprintf("d=open('%s','rb').read(4096); assert d==b'\\xde'*4096, f'writer-side mismatch on node 0: {set(d)}'", colimaNBDDev))

		colimaSSH("sudo", "nbd-client", "-d", colimaNBDDev).Run() //nolint:errcheck

		for i := 0; i < len(c.HTTPPorts); i++ {
			nodeIdx := i
			var lastN int
			var lastErr error
			require.Eventuallyf(t, func() bool {
				n, err := colimaListVolumeObjects(t, c.HTTPURL(nodeIdx), c.AccessKey, c.SecretKey)
				lastN, lastErr = n, err
				return err == nil && n > 0
			}, 60*time.Second, 1*time.Second,
				"node %d did not list __vol/default/ objects: count=%d err=%v",
				nodeIdx, lastN, lastErr)
			t.Logf("node %d: __vol/default/ object count = %d", nodeIdx, lastN)
		}
	})

	t.Run("NFS4WriteVisibleAcrossNodes", func(t *testing.T) {
		bucket := fmt.Sprintf("cluster-nfs4-write-%d", time.Now().UnixNano())

		out, code := runColimaAdminCLI(t, leaderDir, "bucket", "create", bucket)
		require.Equalf(t, 0, code, "bucket create on leader: %s", out)

		out, code = runColimaAdminCLI(t, leaderDir, "nfs", "export", "add", bucket)
		require.Equalf(t, 0, code, "nfs export add: %s", out)

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

		for i := 0; i < len(c.HTTPPorts); i++ {
			i := i
			require.Eventuallyf(t, func() bool {
				status := colimaHTTPHeadObjectSigned(t, c.HTTPURL(i), c.AccessKey, c.SecretKey, bucket, "cluster-test.txt")
				return status == http.StatusOK
			}, 60*time.Second, 500*time.Millisecond, "node %d did not observe cluster-test.txt", i)
		}
	})
}
