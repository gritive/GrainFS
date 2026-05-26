//go:build compat

// Phase A status: all standalone prev→current restart tests in this file
// are skipped. Phase A is green-field (no legacy <dataDir>/kek.key
// support); reactivation requires Phase B legacy-migration tooling.

package compat

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

func TestSnapshotLegacyGzipRejectedByCurrent(t *testing.T) {
	t.Skip("Phase A green-field cutover: legacy <dataDir>/kek.key is refused at boot; cross-version restart tests require Phase B migration support")
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
	require.Equal(t, http.StatusOK, resp.StatusCode, "create snapshot")
	var snapOut struct {
		Seq uint64 `json:"seq"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&snapOut))
	seq := snapOut.Seq
	legacyPath := fmt.Sprintf("%s/snapshots/snapshot-%d.json.gz", dataDir, seq)
	if _, err := os.Stat(legacyPath); err != nil {
		terminateProcess(cmd1)
	}

	terminateProcess(cmd1)

	// Phase 2: restart with cur binary; legacy gzip snapshots are not restorable
	// after the zstd snapshot cutover.
	cmd2 := startGrainfsNode(t, cur, dataDir, curHTTP, curRaft, encKeyFile)
	t.Cleanup(func() { terminateProcess(cmd2) })
	require.NoError(t, waitForPort(curHTTP, 60*time.Second))
	time.Sleep(2 * time.Second)

	restoreURL := fmt.Sprintf("http://127.0.0.1:%d/admin/snapshots/%d/restore", curHTTP, seq)
	resp2, err := httpPostJSON(restoreURL, nil)
	require.NoError(t, err)
	defer resp2.Body.Close()
	body, err := io.ReadAll(resp2.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusConflict, resp2.StatusCode, "legacy gzip snapshot must not restore body=%s", body)
	require.Contains(t, strings.ToLower(string(body)), "unsupported snapshot format")
}
