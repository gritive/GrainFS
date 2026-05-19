// Cluster-mount NBD test: connects nbd-client inside the colima VM against
// node 0's NBD port, writes a 0xDE pattern, and verifies cluster
// replication via signed S3 ListObjects against every node.
//
// Verification strategy: NBD reads at non-leader nodes are not part of
// the cluster contract. We probe the data layer (signed S3 ListObjects
// of __grainfs_volumes/__vol/default/) on every node instead — same
// pattern TestE2E_MultiRaftSharding_NBDRoutesThroughCoordinator uses.
// This also avoids kernel-fragile nbd-client reconnect storms.
//
// Migrated from tests/nbd_colima/cluster_mount_test.go.

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

func TestColimaClusterNBDWriteReplicatesAcrossNodesE2E(t *testing.T) {
	c := getOrInitSharedColimaCluster(t)
	leaderDir := c.DataDirs[c.LeaderIdx]
	hostIP := envOrDefault("HOST_IP", "192.168.5.2")

	// 1. Create the "default" volume on leader (replicated via raft).
	out, code := runColimaAdminCLI(t, leaderDir, "volume", "create", "default", "--size", colimaNBDVolSize)
	require.Equalf(t, 0, code, "volume create on leader: %s", out)

	// 2. Connect to node 0, write 0xDE pattern to block 0 with conv=fsync,
	//    disconnect. Single nbd-client lifecycle — no reconnect storm.
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
	// Same-node readback sanity check.
	runColimaSSH(t, "sudo", "python3", "-c",
		fmt.Sprintf("d=open('%s','rb').read(4096); assert d==b'\\xde'*4096, f'writer-side mismatch on node 0: {set(d)}'", colimaNBDDev))

	// Disconnect so the write-back has flushed before we probe replication.
	colimaSSH("sudo", "nbd-client", "-d", colimaNBDDev).Run() //nolint:errcheck

	// 3. Verify the volume's backing objects are visible via signed S3
	//    ListObjects on every node.
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
}
