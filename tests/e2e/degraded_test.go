package e2e

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

// TestE2E_DegradedMode_WritesBlocked starts a 5-node cluster with EC 3+2,
// kills 3 nodes (leaving 2 live), and verifies that PUT requests return 503
// once the degraded monitor detects the shortage (≤ 30 s).
//
// The degraded condition: liveCount(2) < MinECNodes(3) → degraded=true.
// The monitor fires immediately on start and then every 30 s.
func TestE2E_DegradedMode_WritesBlocked(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping degraded-mode e2e in -short mode")
	}
	binary := getBinary()
	if _, err := os.Stat(binary); err != nil {
		t.Skipf("grainfs binary not found at %s — run `make build` first", binary)
	}

	const (
		clusterKey = "E2E-DEGRADED-KEY"
		accessKey  = "deg-ak"
		secretKey  = "deg-sk"
		bucketName = "degraded-test"
		numNodes   = 5
		ecData     = 3
		ecParity   = 2
	)

	httpPorts := make([]int, numNodes)
	raftPorts := make([]int, numNodes)
	nfs4Ports := make([]int, numNodes)
	nbdPorts := make([]int, numNodes)
	ports := uniqueFreePorts(numNodes * 4)
	for i := range numNodes {
		httpPorts[i] = ports[i]
		raftPorts[i] = ports[numNodes+i]
		nfs4Ports[i] = ports[2*numNodes+i]
		nbdPorts[i] = ports[3*numNodes+i]
	}

	raftAddr := func(i int) string { return fmt.Sprintf("127.0.0.1:%d", raftPorts[i]) }
	httpURL := func(i int) string { return fmt.Sprintf("http://127.0.0.1:%d", httpPorts[i]) }
	peersFor := func(i int) string {
		var out []string
		for j := range raftPorts {
			if j == i {
				continue
			}
			out = append(out, raftAddr(j))
		}
		return strings.Join(out, ",")
	}

	dataDirs := make([]string, numNodes)
	for i := range dataDirs {
		d, err := os.MkdirTemp("", fmt.Sprintf("grainfs-degraded-%d-*", i))
		require.NoError(t, err)
		dataDirs[i] = d
		t.Cleanup(func() { _ = os.RemoveAll(d) })
	}

	startNode := func(i int) *exec.Cmd {
		cmd := exec.Command(binary, "serve",
			"--data", dataDirs[i],
			"--port", fmt.Sprintf("%d", httpPorts[i]),
			"--node-id", raftAddr(i),
			"--raft-addr", raftAddr(i),
			"--peers", peersFor(i),
			"--cluster-key", clusterKey,
			"--access-key", accessKey,
			"--secret-key", secretKey,
			fmt.Sprintf("--ec-data=%d", ecData),
			fmt.Sprintf("--ec-parity=%d", ecParity),
			"--seed-groups", "1",
			"--nfs4-port", fmt.Sprintf("%d", nfs4Ports[i]),
			"--nbd-port", fmt.Sprintf("%d", nbdPorts[i]),
			"--snapshot-interval", "0",
			"--scrub-interval", "0",
			"--lifecycle-interval", "0",
			"--degraded-check-interval", "1s",
			"--no-encryption",
		)
		if testing.Verbose() {
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
		}
		require.NoError(t, cmd.Start(), "start node %d", i)
		return cmd
	}

	procs := make([]*exec.Cmd, numNodes)
	killAll := func() {
		for _, p := range procs {
			if p != nil && p.Process != nil {
				_ = p.Process.Kill()
				_, _ = p.Process.Wait()
			}
		}
	}
	t.Cleanup(killAll)

	// All nodes share the full 5-node peer list. Start the complete peer set
	// before probing writes so meta-raft does not elect against a partial
	// bootstrap view and then churn while the last voters join.
	for i := range numNodes {
		procs[i] = startNode(i)
		time.Sleep(150 * time.Millisecond)
	}
	waitForPortsParallel(t, httpPorts, 60*time.Second)
	time.Sleep(4 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	// Find a leader and create a bucket to confirm the cluster is healthy.
	endpoints := make([]string, numNodes)
	for i := range endpoints {
		endpoints[i] = httpURL(i)
	}
	leaderIdx, err := waitForWritableEndpoint(
		ctx,
		endpoints,
		180*time.Second,
		5*time.Second,
		1*time.Second,
		func(attemptCtx context.Context, endpoint string) error {
			c := ecS3Client(endpoint, accessKey, secretKey)
			return tryCreateBucket(attemptCtx, c, bucketName)
		},
	)
	require.NoError(t, err, "no leader found or CreateBucket never succeeded")
	client := ecS3Client(httpURL(leaderIdx), accessKey, secretKey)
	t.Logf("degraded test: leader node %d at %s", leaderIdx, httpURL(leaderIdx))

	// Verify normal PUT works before killing nodes.
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("before-kill"),
		Body:   bytes.NewReader([]byte("healthy")),
	})
	require.NoError(t, err, "PutObject should succeed with all nodes up")

	// Kill 3 nodes — leave only 2 alive (< MinECNodes=3 → degraded).
	// Pick 3 non-leader nodes to avoid forcing a re-election that could confuse
	// which nodes are "up" for the subsequent client requests.
	killed := 0
	for i := 0; i < numNodes && killed < 3; i++ {
		if i == leaderIdx {
			continue
		}
		t.Logf("degraded test: killing node %d at %s", i, httpURL(i))
		_ = procs[i].Process.Kill()
		_, _ = procs[i].Process.Wait()
		procs[i] = nil
		killed++
	}
	t.Logf("degraded test: %d nodes killed, 2 remaining", killed)

	// The monitor fires immediately on start — but since all nodes started
	// healthy, the immediate fire found live≥MinECNodes (not degraded). This
	// test uses a 1 s monitor interval so the post-kill transition is observed
	// quickly and does not depend on the production 30 s tick boundary.
	// Poll the surviving nodes until one returns 503 for a PUT.
	require.Eventually(t, func() bool {
		for i := 0; i < numNodes; i++ {
			if procs[i] == nil {
				continue
			}
			c := ecS3Client(httpURL(i), accessKey, secretKey)
			_, putErr := c.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String("after-kill"),
				Body:   bytes.NewReader([]byte("should-fail")),
			})
			if putErr != nil {
				errStr := putErr.Error()
				if strings.Contains(errStr, "503") || strings.Contains(errStr, "ServiceUnavailable") {
					t.Logf("degraded test: node %d correctly returned 503/ServiceUnavailable", i)
					return true
				}
			}
		}
		return false
	}, 60*time.Second, 500*time.Millisecond, "expected writes to be blocked (503) after degraded")
}
