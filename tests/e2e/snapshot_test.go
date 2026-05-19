package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

// snapshotResponse mirrors internal/snapshot.Snapshot for JSON decoding.
type snapshotResponse struct {
	Seq         uint64 `json:"seq"`
	Timestamp   string `json:"timestamp"`
	ObjectCount int    `json:"object_count"`
	SizeBytes   int64  `json:"size_bytes"`
	Reason      string `json:"reason,omitempty"`
}

type snapshotListResponse struct {
	Snapshots []snapshotResponse `json:"snapshots"`
}

type restoreResponse struct {
	RestoredObjects int           `json:"restored_objects"`
	StaleBlobs      []interface{} `json:"stale_blobs"`
}

type errorResponse struct {
	Error string `json:"error"`
	Hint  string `json:"hint"`
}

func postJSON(url string, body interface{}) (*http.Response, error) {
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			return nil, err
		}
	}
	client := &http.Client{Timeout: 10 * time.Second}
	return client.Post(url, "application/json", &buf) //nolint:noctx
}

func createSnapshotE2E(t *testing.T, serverURL, reason string) snapshotResponse {
	t.Helper()
	var lastErr error
	var lastStatus int
	var lastBody string

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var snap snapshotResponse
		resp, err := postJSON(serverURL+"/admin/snapshots", map[string]string{"reason": reason})
		if err != nil {
			lastErr = err
			time.Sleep(500 * time.Millisecond)
			continue
		}
		lastStatus = resp.StatusCode
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		lastBody = string(body)
		if resp.StatusCode != http.StatusOK {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if err := json.Unmarshal(body, &snap); err != nil {
			lastErr = err
			time.Sleep(500 * time.Millisecond)
			continue
		}
		return snap
	}

	require.Failf(t,
		"snapshot should become available after cluster data groups are ready",
		"lastErr=%v status=%d body=%s",
		lastErr, lastStatus, lastBody)
	return snapshotResponse{}
}

// TestSnapshotE2E exercises the /admin/snapshots HTTP surface (create / list
// / restore / 404) against both single-node and 4-node cluster fixtures.
// /admin/snapshots restore mutates global metadata state, so this group
// uses a dedicated per-branch fixture (single boot per fixture, sub-tests
// run in sequence on it) to avoid contaminating the shared fixtures.
func TestSnapshotE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runSnapshotCases(t, newDedicatedSingleNodeS3Target(t, nil))
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		runSnapshotCases(t, newClusterS3TargetWithExtraArgs(t, 4, nil))
	})
}

func runSnapshotCases(t *testing.T, tgt s3Target) {
	t.Helper()
	serverURL := tgt.endpoint(0)
	client := tgt.pickNode(0)
	ctx := context.Background()

	t.Run("CreateAndRestore", func(t *testing.T) {
		bucket := tgt.uniqueBucket(t, "create")

		objects := []string{"a.txt", "b.txt", "c.txt", "d.txt", "e.txt"}
		for _, key := range objects {
			_, err := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
				Body:   strings.NewReader("content-" + key),
			})
			require.NoError(t, err, "put %s", key)
		}

		snap := createSnapshotE2E(t, serverURL, "e2e-test")
		require.NotZero(t, snap.Seq, "snapshot seq must be non-zero")
		require.NotEmpty(t, snap.Timestamp)
		require.Positive(t, snap.ObjectCount, "snapshot must contain objects")

		extras := []string{"extra1.txt", "extra2.txt"}
		for _, key := range extras {
			_, err := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
				Body:   strings.NewReader("extra-" + key),
			})
			require.NoError(t, err, "put extra %s", key)
		}

		listOut, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(bucket),
		})
		require.NoError(t, err)
		require.Len(t, listOut.Contents, 7, "7 objects before restore")

		restoreURL := fmt.Sprintf("%s/admin/snapshots/%d/restore", serverURL, snap.Seq)
		restoreResp, err := postJSON(restoreURL, nil)
		require.NoError(t, err)
		defer restoreResp.Body.Close()
		require.Equal(t, http.StatusOK, restoreResp.StatusCode, "restore status")

		var rr restoreResponse
		require.NoError(t, json.NewDecoder(restoreResp.Body).Decode(&rr))
		require.GreaterOrEqual(t, rr.RestoredObjects, 5, "at least 5 objects restored")
		require.Empty(t, rr.StaleBlobs, "no stale blobs: blobs still exist")

		listOut2, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(bucket),
		})
		require.NoError(t, err)
		require.Len(t, listOut2.Contents, 5, "5 objects after restore (extras removed)")

		for _, key := range objects {
			getResp, err := client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			require.NoError(t, err, "get %s after restore", key)
			body, _ := io.ReadAll(getResp.Body)
			getResp.Body.Close()
			require.Equal(t, "content-"+key, string(body))
		}

		for _, key := range extras {
			_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			require.Error(t, err, "extra object %s should be gone after restore", key)
		}
	})

	t.Run("List", func(t *testing.T) {
		for i := 0; i < 2; i++ {
			createSnapshotE2E(t, serverURL, fmt.Sprintf("list-test-%d", i))
		}

		listResp, err := http.Get(serverURL + "/admin/snapshots") //nolint:noctx
		require.NoError(t, err)
		defer listResp.Body.Close()
		require.Equal(t, http.StatusOK, listResp.StatusCode)

		var lr snapshotListResponse
		require.NoError(t, json.NewDecoder(listResp.Body).Decode(&lr))
		require.GreaterOrEqual(t, len(lr.Snapshots), 2)

		for i := 1; i < len(lr.Snapshots); i++ {
			require.Less(t, lr.Snapshots[i-1].Seq, lr.Snapshots[i].Seq)
		}
	})

	t.Run("NotFound", func(t *testing.T) {
		restoreResp, err := postJSON(serverURL+"/admin/snapshots/999999/restore", nil)
		require.NoError(t, err)
		defer restoreResp.Body.Close()
		require.Equal(t, http.StatusNotFound, restoreResp.StatusCode)

		var er errorResponse
		require.NoError(t, json.NewDecoder(restoreResp.Body).Decode(&er))
		require.NotEmpty(t, er.Error)
		require.NotEmpty(t, er.Hint)
	})
}
