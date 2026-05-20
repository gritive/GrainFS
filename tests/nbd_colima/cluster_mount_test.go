//go:build colima

package nbd_colima

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/tests/colimafixture"
	"github.com/stretchr/testify/require"
)

const clusterNBDVolSize = "8388608"

type listBucketResultV2 struct {
	XMLName  xml.Name `xml:"ListBucketResult"`
	KeyCount int      `xml:"KeyCount"`
	Contents []struct {
		Key string `xml:"Key"`
	} `xml:"Contents"`
}

func TestNBD_ClusterMountReplicatesAcrossNodes(t *testing.T) {
	c := colimafixture.StartCluster(t, colimafixture.Options{EnableNBD: true})
	leaderDir := c.DataDirs[c.LeaderIdx]

	out, code := runClusterAdminCLI(t, leaderDir, "volume", "create", "default", "--size", clusterNBDVolSize)
	require.Equalf(t, 0, code, "volume create on leader: %s", out)
	grantClusterAdmin(t, c, leaderDir, "__grainfs_volumes")

	hostIP := envOrDefault("HOST_IP", "192.168.5.2")
	colimaSSH("sudo", "modprobe", "nbd", "max_part=0").Run() //nolint:errcheck
	colimaSSH("sudo", "nbd-client", "-d", nbdDev).Run()      //nolint:errcheck
	time.Sleep(300 * time.Millisecond)

	runColimaSSH(t, "sudo", "nbd-client",
		hostIP, fmt.Sprintf("%d", c.NBDPorts[0]), nbdDev,
		"-b", "4096", "-N", "default",
	)
	t.Cleanup(func() {
		colimaSSH("sudo", "nbd-client", "-d", nbdDev).Run() //nolint:errcheck
	})

	runColimaSSH(t, "sudo", "bash", "-c",
		fmt.Sprintf("python3 -c \"import sys; sys.stdout.buffer.write(b'\\xde'*4096)\" | dd of=%s bs=4096 count=1 conv=fsync 2>/dev/null", nbdDev))
	runColimaSSH(t, "sudo", "python3", "-c",
		fmt.Sprintf("d=open('%s','rb').read(4096); assert d==b'\\xde'*4096, f'writer-side mismatch on node 0: {set(d)}'", nbdDev))

	colimaSSH("sudo", "nbd-client", "-d", nbdDev).Run() //nolint:errcheck

	for i := 0; i < len(c.HTTPPorts); i++ {
		nodeIdx := i
		var lastN int
		var lastErr error
		require.Eventuallyf(t, func() bool {
			n, err := listClusterVolumeObjects(t, c.HTTPURL(nodeIdx), c.AccessKey, c.SecretKey)
			lastN, lastErr = n, err
			return err == nil && n > 0
		}, 60*time.Second, time.Second,
			"node %d did not list __vol/default/ objects: count=%d err=%v",
			nodeIdx, lastN, lastErr)
		t.Logf("node %d: __vol/default/ object count = %d", nodeIdx, lastN)
	}
}

func listClusterVolumeObjects(t *testing.T, endpoint, ak, sk string) (int, error) {
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
	var parsed listBucketResultV2
	if err := xml.Unmarshal(body, &parsed); err != nil {
		return -1, fmt.Errorf("xml parse: %w", err)
	}
	if parsed.KeyCount > 0 {
		return parsed.KeyCount, nil
	}
	return len(parsed.Contents), nil
}

func grantClusterAdmin(t *testing.T, c *colimafixture.Cluster, leaderDir, bucket string) {
	t.Helper()
	out, code := runClusterAdminCLI(t, leaderDir, "iam", "grant", "put", c.SAID, bucket, "Admin")
	require.Equalf(t, 0, code, "grant admin on %s: %s", bucket, out)
}

func runClusterAdminCLI(t *testing.T, dataDir string, args ...string) (string, int) {
	t.Helper()
	binary := os.Getenv("GRAINFS_BINARY")
	if binary == "" {
		binary = "../../bin/grainfs"
	}
	full := append(append([]string{}, args...), "--endpoint", dataDir+"/admin.sock")
	cmd := exec.Command(binary, full...)
	out, _ := cmd.CombinedOutput()
	code := 0
	if cmd.ProcessState != nil {
		code = cmd.ProcessState.ExitCode()
	}
	return string(out), code
}
