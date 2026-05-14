//go:build compat

package compat

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

func TestSnapshotForwardCompat(t *testing.T) {
	prev := prevBinary(t)
	cur := getBinary()

	ports := uniqueFreePorts(4)
	prevHTTP, curHTTP := ports[0], ports[1]
	prevRaft, curRaft := ports[2], ports[3]

	dataDir, err := os.MkdirTemp("/tmp", "compat-snap-*")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dataDir) })

	encKeyFile := makeSharedEncryptionKeyFile(t)

	// Phase 1: write data + create snapshot with prev binary.
	cmd1 := startGrainfsNode(t, prev, dataDir, prevHTTP, prevRaft, encKeyFile)
	require.NoError(t, waitForPort(prevHTTP, 60*time.Second))
	time.Sleep(2 * time.Second)

	ak, sk := bootstrapCompatAdmin(t, dataDir, 30*time.Second)
	prevClient := newCompatS3Client(fmt.Sprintf("http://127.0.0.1:%d", prevHTTP), ak, sk)

	_, err = prevClient.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String("snap-compat"),
	})
	require.NoError(t, err)
	_, err = prevClient.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String("snap-compat"),
		Key:    aws.String("snap-obj"),
		Body:   strings.NewReader("snapshot data"),
	})
	require.NoError(t, err)

	// Create snapshot via HTTP
	snapURL := fmt.Sprintf("http://127.0.0.1:%d/admin/snapshots", prevHTTP)
	resp, err := httpPostJSON(snapURL, map[string]string{"reason": "compat-test"})
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode, "create snapshot")
	var snapOut struct {
		Seq uint64 `json:"seq"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&snapOut))
	seq := snapOut.Seq

	terminateProcess(cmd1)

	// Phase 2: restart with cur binary, restore snapshot, read data.
	cmd2 := startGrainfsNode(t, cur, dataDir, curHTTP, curRaft, encKeyFile)
	t.Cleanup(func() { terminateProcess(cmd2) })
	require.NoError(t, waitForPort(curHTTP, 60*time.Second))
	time.Sleep(2 * time.Second)

	// Restore snapshot
	restoreURL := fmt.Sprintf("http://127.0.0.1:%d/admin/snapshots/%d/restore", curHTTP, seq)
	resp2, err := httpPostJSON(restoreURL, nil)
	require.NoError(t, err)
	defer resp2.Body.Close()
	require.Equal(t, 200, resp2.StatusCode, "restore snapshot")

	// Wait for restore
	time.Sleep(3 * time.Second)
	require.NoError(t, waitForPort(curHTTP, 30*time.Second))

	curClient := newCompatS3Client(fmt.Sprintf("http://127.0.0.1:%d", curHTTP), ak, sk)
	res, err := curClient.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String("snap-compat"),
		Key:    aws.String("snap-obj"),
	})
	require.NoError(t, err)
	defer res.Body.Close()
	got, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, "snapshot data", string(got))
}
