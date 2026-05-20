//go:build compat

package compat

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

// TestInstallSnapshotPath verifies that a new N+1 node (current binary) joining
// an N-node cluster (previous binary) can receive a Raft InstallSnapshot RPC
// from the leader and become operational.
func TestInstallSnapshotPath(t *testing.T) {
	prev := prevBinary(t)
	cur := getBinary()

	// Start 2-node all-prev cluster.
	c := startCompatCluster(t, []string{prev, prev})

	// Write data.
	bucket := "install-snap"
	_, err := c.S3Client(0).CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		_, err = c.S3Client(0).PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(fmt.Sprintf("obj%d", i)),
			Body:   strings.NewReader(fmt.Sprintf("data%d", i)),
		})
		require.NoError(t, err)
	}

	// Create snapshot on leader (node 0) via HTTP.
	c.CreateSnapshot(t, 0)

	// Stop node 1 (old follower).
	terminateProcess(c.procs[1])
	c.procs[1] = nil

	// Create a fresh data dir for the replacement node (cur binary).
	newDataDir, err := os.MkdirTemp("/tmp", "compat-snap-new-*")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(newDataDir) })

	// Write .join-pending pointing to node 0's raft addr. §7 B3: stage the
	// seed's kek.key first so wireDEKKeeper on the joiner can complete
	// startup (it refuses to auto-generate a KEK in join mode).
	seedKEK, kekErr := os.ReadFile(filepath.Join(c.dataDirs[0], "kek.key"))
	require.NoError(t, kekErr, "read seed kek.key")
	require.NoError(t, os.WriteFile(filepath.Join(newDataDir, "kek.key"), seedKEK, 0o600))
	seedRaftAddr := fmt.Sprintf("127.0.0.1:%d", c.raftPorts[0])
	require.NoError(t, os.WriteFile(
		filepath.Join(newDataDir, ".join-pending"),
		[]byte(seedRaftAddr), 0o600,
	))

	// Allocate new ports for the replacement node.
	newPorts := uniqueFreePorts(2)
	newHTTP, newRaft := newPorts[0], newPorts[1]

	// Start the replacement node with the current binary.
	// It joins via .join-pending and receives InstallSnapshot from the leader.
	newCmd := startGrainfsNode(t, cur, newDataDir, newHTTP, newRaft, c.encKeyFile)
	t.Cleanup(func() { terminateProcess(newCmd) })

	// Wait for the new node's HTTP port — it must install the snapshot first.
	require.NoError(t, waitForPort(newHTTP, 90*time.Second), "wait for new node HTTP")
	time.Sleep(3 * time.Second)

	// Verify that the new node can serve data replicated via InstallSnapshot.
	newClient := newCompatS3Client(fmt.Sprintf("http://127.0.0.1:%d", newHTTP), c.accessKey, c.secretKey)
	res, err := newClient.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("obj0"),
	})
	require.NoError(t, err)
	defer res.Body.Close()
	got, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, "data0", string(got))
}
