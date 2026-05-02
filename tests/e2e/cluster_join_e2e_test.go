package e2e

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

type dynamicJoinCluster struct {
	procs     []*exec.Cmd
	dataDirs  []string
	httpURLs  []string
	httpPorts []int
	raftPorts []int
	nfsPorts  []int
	nbdPorts  []int
	accessKey string
	secretKey string
}

func startDynamicJoinCluster(t *testing.T, nodes int) *dynamicJoinCluster {
	t.Helper()
	require.GreaterOrEqual(t, nodes, 2)

	binary := getBinary()
	if _, err := os.Stat(binary); err != nil {
		t.Skipf("grainfs binary not found at %s — run `make build` first", binary)
	}

	ports := uniqueFreePorts(nodes * 4)
	c := &dynamicJoinCluster{
		procs:     make([]*exec.Cmd, nodes),
		dataDirs:  make([]string, nodes),
		httpURLs:  make([]string, nodes),
		httpPorts: ports[:nodes],
		raftPorts: ports[nodes : 2*nodes],
		nfsPorts:  ports[2*nodes : 3*nodes],
		nbdPorts:  ports[3*nodes : 4*nodes],
		accessKey: "join-ak",
		secretKey: "join-sk",
	}
	for i := 0; i < nodes; i++ {
		dir, err := os.MkdirTemp("", fmt.Sprintf("grainfs-join-node-%d-*", i))
		require.NoError(t, err)
		c.dataDirs[i] = dir
		c.httpURLs[i] = fmt.Sprintf("http://127.0.0.1:%d", c.httpPorts[i])
	}
	t.Cleanup(c.Stop)

	c.procs[0] = c.startNode(t, 0)
	waitForPort(t, c.httpPorts[0], 60*time.Second)
	time.Sleep(2 * time.Second)
	for i := 1; i < nodes; i++ {
		c.procs[i] = c.startNode(t, i)
		waitForPort(t, c.httpPorts[i], 90*time.Second)
	}
	return c
}

func (c *dynamicJoinCluster) startNode(t *testing.T, i int) *exec.Cmd {
	t.Helper()
	logFile, err := os.CreateTemp("", fmt.Sprintf("grainfs-dynamic-join-node-%d-*.log", i))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = logFile.Close()
		if t.Failed() {
			t.Logf("dynamic join node %d log saved to %s", i, logFile.Name())
		} else {
			_ = os.Remove(logFile.Name())
		}
	})

	args := []string{
		"serve",
		"--data", c.dataDirs[i],
		"--port", fmt.Sprintf("%d", c.httpPorts[i]),
		"--node-id", fmt.Sprintf("n%d", i+1),
		"--raft-addr", fmt.Sprintf("127.0.0.1:%d", c.raftPorts[i]),
		"--cluster-key", "E2E-DYNAMIC-JOIN-KEY",
		"--access-key", c.accessKey,
		"--secret-key", c.secretKey,
		"--ec-data", "0",
		"--ec-parity", "0",
		"--seed-groups", "1",
		"--nfs4-port", fmt.Sprintf("%d", c.nfsPorts[i]),
		"--nbd-port", fmt.Sprintf("%d", c.nbdPorts[i]),
		"--snapshot-interval", "0",
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
		"--no-encryption",
	}
	if i > 0 {
		args = append(args, "--join", fmt.Sprintf("127.0.0.1:%d", c.raftPorts[0]))
	}

	cmd := exec.Command(getBinary(), args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	require.NoError(t, cmd.Start(), "start dynamic join node %d", i)
	return cmd
}

func (c *dynamicJoinCluster) Stop() {
	for _, p := range c.procs {
		if p != nil && p.Process != nil {
			_ = p.Process.Signal(syscall.SIGTERM)
		}
	}
	deadline := time.Now().Add(10 * time.Second)
	for _, p := range c.procs {
		if p == nil || p.Process == nil {
			continue
		}
		done := make(chan struct{})
		go func(p *exec.Cmd) {
			_ = p.Wait()
			close(done)
		}(p)
		select {
		case <-done:
		case <-time.After(time.Until(deadline)):
			_ = p.Process.Kill()
			<-done
		}
	}
	for _, dir := range c.dataDirs {
		_ = os.RemoveAll(dir)
	}
}

func TestE2E_JoinedNodeEdgeForwardsBeforeDataReady(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e")
	}
	c := startDynamicJoinCluster(t, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	joinClient := ecS3Client(c.httpURLs[1], c.accessKey, c.secretKey)
	seedClient := ecS3Client(c.httpURLs[0], c.accessKey, c.secretKey)
	bucket := "dynamic-join-edge"
	key := "hello.txt"
	body := []byte("hello dynamic join")

	require.Eventually(t, func() bool {
		return tryCreateBucket(ctx, joinClient, bucket) == nil
	}, 30*time.Second, 500*time.Millisecond)
	require.NoError(t, tryPutObject(ctx, joinClient, bucket, key, body))

	gotSeed, err := getObjectBytes(ctx, seedClient, bucket, key)
	require.NoError(t, err)
	require.Equal(t, body, gotSeed)
	gotJoin, err := getObjectBytes(ctx, joinClient, bucket, key)
	require.NoError(t, err)
	require.Equal(t, body, gotJoin)

	_, err = joinClient.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
}

func TestE2E_AllServicesAvailableOnJoinedNodes(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e")
	}
	c := startDynamicJoinCluster(t, 2)
	waitForPortsParallel(t, c.nfsPorts, 30*time.Second)
	waitForPortsParallel(t, c.nbdPorts, 30*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 5 * time.Second}

	reqBody := []byte(`{"namespace":["join_ns"],"properties":{"owner":"e2e"}}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.httpURLs[1]+"/iceberg/v1/namespaces", bytes.NewReader(reqBody))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	req, err = http.NewRequestWithContext(ctx, http.MethodGet, c.httpURLs[0]+"/iceberg/v1/namespaces", nil)
	require.NoError(t, err)
	resp, err = client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Contains(t, string(data), "join_ns")
}

func TestE2E_DefaultBucketOnlySeedCreates(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e")
	}
	c := startDynamicJoinCluster(t, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, endpoint := range c.httpURLs {
		client := ecS3Client(endpoint, c.accessKey, c.secretKey)
		out, err := client.ListBuckets(ctx, &s3.ListBucketsInput{})
		require.NoError(t, err)
		var defaults int
		for _, bucket := range out.Buckets {
			if aws.ToString(bucket.Name) == "default" {
				defaults++
			}
		}
		require.Equal(t, 1, defaults, "default bucket should exist once as shared cluster metadata at %s", endpoint)
	}
}
