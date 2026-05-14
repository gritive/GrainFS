//go:build compat

package compat

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

// compatCluster is a multi-node grainfs cluster for compat testing.
// Each node runs a potentially different binary version.
type compatCluster struct {
	t          *testing.T
	binaries   []string
	dataDirs   []string
	httpPorts  []int
	raftPorts  []int
	httpURLs   []string
	encKeyFile string
	accessKey  string
	secretKey  string
	procs      []*exec.Cmd
	stopped    bool
}

// startCompatCluster starts an N-node cluster.
// binaries[i] is the grainfs binary to use for node i.
// If binaries[i] == "" uses getBinary().
// Node 0 is always the seed (leader). Followers join via .join-pending.
func startCompatCluster(t *testing.T, binaries []string) *compatCluster {
	t.Helper()

	n := len(binaries)
	if n == 0 {
		t.Fatal("startCompatCluster: binaries must be non-empty")
	}

	ports := uniqueFreePorts(2 * n)

	c := &compatCluster{
		t:          t,
		binaries:   make([]string, n),
		dataDirs:   make([]string, n),
		httpPorts:  make([]int, n),
		raftPorts:  make([]int, n),
		httpURLs:   make([]string, n),
		procs:      make([]*exec.Cmd, n),
		encKeyFile: makeSharedEncryptionKeyFile(t),
	}

	for i := 0; i < n; i++ {
		bin := binaries[i]
		if bin == "" {
			bin = getBinary()
		}
		c.binaries[i] = bin
		c.httpPorts[i] = ports[i]
		c.raftPorts[i] = ports[n+i]
		c.httpURLs[i] = fmt.Sprintf("http://127.0.0.1:%d", c.httpPorts[i])

		dir, err := os.MkdirTemp("/tmp", fmt.Sprintf("gc-%d-*", i))
		require.NoError(t, err, "mkdtemp for node %d", i)
		c.dataDirs[i] = dir
	}

	// Start seed node (node 0).
	c.procs[0] = c.startNode(0)
	require.NoError(t, waitForPort(c.httpPorts[0], 60*time.Second), "wait for node 0 HTTP port")
	time.Sleep(2 * time.Second)

	// Bootstrap admin SA on the seed node via admin UDS.
	ak, sk := bootstrapCompatAdmin(t, c.dataDirs[0], 30*time.Second)
	c.accessKey = ak
	c.secretKey = sk

	// Start followers.
	for i := 1; i < n; i++ {
		joinPending := filepath.Join(c.dataDirs[i], ".join-pending")
		raftAddr := fmt.Sprintf("127.0.0.1:%d", c.raftPorts[0])
		require.NoError(t,
			os.WriteFile(joinPending, []byte(raftAddr), 0o600),
			"write .join-pending for node %d", i,
		)
		c.procs[i] = c.startNode(i)
		require.NoError(t, waitForPort(c.httpPorts[i], 90*time.Second), "wait for node %d HTTP port", i)
	}

	t.Cleanup(c.Stop)
	return c
}

// startNode launches the grainfs process for node i and returns the Cmd.
func (c *compatCluster) startNode(i int) *exec.Cmd {
	c.t.Helper()
	if _, err := os.Stat(c.binaries[i]); err != nil {
		c.t.Skipf("grainfs binary not found at %s — run make build first", c.binaries[i])
	}

	logFile, err := os.CreateTemp("", fmt.Sprintf("grainfs-compat-node-%d-*.log", i))
	require.NoError(c.t, err, "create log file for node %d", i)
	c.t.Cleanup(func() {
		_ = logFile.Close()
		if !c.t.Failed() {
			_ = os.Remove(logFile.Name())
		} else {
			c.t.Logf("compat cluster node %d log: %s", i, logFile.Name())
		}
	})

	nodeID := fmt.Sprintf("n%d", i+1)
	raftAddr := fmt.Sprintf("127.0.0.1:%d", c.raftPorts[i])

	args := []string{
		"serve",
		"--data", c.dataDirs[i],
		"--port", fmt.Sprintf("%d", c.httpPorts[i]),
		"--node-id", nodeID,
		"--raft-addr", raftAddr,
		"--cluster-key", "COMPAT-KEY",
		"--encryption-key-file", c.encKeyFile,
		"--nfs4-port", "0",
		"--nbd-port", "0",
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
	}

	cmd := exec.Command(c.binaries[i], args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	require.NoError(c.t, cmd.Start(), "start compat cluster node %d", i)
	return cmd
}

// Stop terminates all nodes and removes data directories.
func (c *compatCluster) Stop() {
	if c == nil || c.stopped {
		return
	}
	c.stopped = true
	for _, p := range c.procs {
		terminateProcess(p)
	}
	for _, dir := range c.dataDirs {
		_ = os.RemoveAll(dir)
	}
}

// S3Client returns an S3 client pointed at node i using the bootstrap creds.
func (c *compatCluster) S3Client(i int) *s3.Client {
	return s3.New(s3.Options{
		BaseEndpoint: aws.String(c.httpURLs[i]),
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider(c.accessKey, c.secretKey, ""),
		UsePathStyle: true,
	})
}

// AdminSock returns the admin Unix socket path for node i.
func (c *compatCluster) AdminSock(i int) string {
	return filepath.Join(c.dataDirs[i], "admin.sock")
}

// CreateSnapshot POSTs to /admin/snapshots on node nodeIdx and returns the seq number.
func (c *compatCluster) CreateSnapshot(t *testing.T, nodeIdx int) int {
	t.Helper()
	url := c.httpURLs[nodeIdx] + "/admin/snapshots"
	resp, err := httpPostJSON(url, map[string]string{"reason": "compat-test"})
	require.NoError(t, err, "POST /admin/snapshots on node %d", nodeIdx)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "CreateSnapshot node %d", nodeIdx)

	var out struct {
		Seq uint64 `json:"seq"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out), "decode snapshot response")
	return int(out.Seq)
}

// RestoreSnapshot POSTs to /admin/snapshots/:seq/restore on node nodeIdx.
func (c *compatCluster) RestoreSnapshot(t *testing.T, nodeIdx int, seq int) {
	t.Helper()
	url := fmt.Sprintf("%s/admin/snapshots/%d/restore", c.httpURLs[nodeIdx], seq)
	resp, err := httpPostJSON(url, nil)
	require.NoError(t, err, "POST /admin/snapshots/%d/restore on node %d", seq, nodeIdx)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "RestoreSnapshot seq=%d node=%d", seq, nodeIdx)
}

// bootstrapCompatAdmin retries POST /v1/iam/sa on node 0's admin UDS until it
// succeeds or the timeout expires.
func bootstrapCompatAdmin(t *testing.T, dataDir string, timeout time.Duration) (ak, sk string) {
	t.Helper()
	sock := filepath.Join(dataDir, "admin.sock")
	client := iamUDSClient(sock)

	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		body := strings.NewReader(`{"name":"admin","description":"compat bootstrap"}`)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		req, err := http.NewRequestWithContext(ctx, "POST", "http://unix/v1/iam/sa", body)
		if err != nil {
			cancel()
			lastErr = err
			time.Sleep(500 * time.Millisecond)
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		cancel()
		if err != nil {
			lastErr = err
			time.Sleep(500 * time.Millisecond)
			continue
		}
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
			lastErr = fmt.Errorf("bootstrap %s -> %d: %s", sock, resp.StatusCode, string(respBody))
			time.Sleep(500 * time.Millisecond)
			continue
		}
		var out struct {
			AccessKey string `json:"access_key"`
			SecretKey string `json:"secret_key"`
		}
		if err := json.Unmarshal(respBody, &out); err != nil {
			lastErr = err
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if out.AccessKey == "" || out.SecretKey == "" {
			lastErr = fmt.Errorf("bootstrap %s: empty creds", sock)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		return out.AccessKey, out.SecretKey
	}
	t.Fatalf("bootstrapCompatAdmin: no success within %v: %v", timeout, lastErr)
	return "", ""
}

// iamUDSClient builds an *http.Client that dials the admin Unix socket.
func iamUDSClient(sock string) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives: true,
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", sock)
			},
		},
	}
}

// httpPostJSON POSTs a JSON body to url and returns the response.
func httpPostJSON(url string, body any) (*http.Response, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	var req *http.Request
	var err error
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		req, err = http.NewRequestWithContext(context.Background(), "POST", url, bytes.NewReader(buf))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
	} else {
		req, err = http.NewRequestWithContext(context.Background(), "POST", url, http.NoBody)
		if err != nil {
			return nil, err
		}
	}
	return client.Do(req)
}
