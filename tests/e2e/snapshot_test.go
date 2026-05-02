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
	return http.Post(url, "application/json", &buf) //nolint:noctx
}

func createSnapshotE2E(t *testing.T, reason string) snapshotResponse {
	t.Helper()
	var snap snapshotResponse
	var lastErr error
	var lastStatus int
	var lastBody string
	require.Eventually(t, func() bool {
		resp, err := postJSON(testServerURL+"/admin/snapshots", map[string]string{"reason": reason})
		if err != nil {
			lastErr = err
			return false
		}
		defer resp.Body.Close()
		lastStatus = resp.StatusCode
		body, _ := io.ReadAll(resp.Body)
		lastBody = string(body)
		if resp.StatusCode != http.StatusOK {
			return false
		}
		if err := json.Unmarshal(body, &snap); err != nil {
			lastErr = err
			return false
		}
		return true
	}, 30*time.Second, 500*time.Millisecond,
		"snapshot should become available after cluster data groups are ready: lastErr=%v status=%d body=%s",
		lastErr, lastStatus, lastBody)
	return snap
}

// TestSnapshot_CreateAndRestore is the primary E2E: put objects, snapshot,
// add extra objects, restore, verify only snapshot-time objects remain.
// Note: DeleteObject immediately removes blobs, so restore of deleted objects
// reports stale blobs. This test uses PUT-after-snapshot instead.
func TestSnapshot_CreateAndRestore(t *testing.T) {
	ctx := context.Background()
	bucket := "snapshot-e2e"
	createBucket(t, bucket)

	// Put 5 objects
	objects := []string{"a.txt", "b.txt", "c.txt", "d.txt", "e.txt"}
	for _, key := range objects {
		_, err := testS3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   strings.NewReader("content-" + key),
		})
		require.NoError(t, err, "put %s", key)
	}

	// POST /admin/snapshots — create snapshot
	snap := createSnapshotE2E(t, "e2e-test")
	require.NotZero(t, snap.Seq, "snapshot seq must be non-zero")
	require.NotEmpty(t, snap.Timestamp)
	require.Positive(t, snap.ObjectCount, "snapshot must contain objects")

	// Add 2 extra objects AFTER the snapshot
	extras := []string{"extra1.txt", "extra2.txt"}
	for _, key := range extras {
		_, err := testS3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   strings.NewReader("extra-" + key),
		})
		require.NoError(t, err, "put extra %s", key)
	}

	// Verify 7 objects exist before restore
	listOut, err := testS3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err)
	require.Len(t, listOut.Contents, 7, "7 objects before restore")

	// POST /admin/snapshots/{seq}/restore — go back to snapshot point
	restoreURL := fmt.Sprintf("%s/admin/snapshots/%d/restore", testServerURL, snap.Seq)
	restoreResp, err := postJSON(restoreURL, nil)
	require.NoError(t, err)
	defer restoreResp.Body.Close()
	require.Equal(t, http.StatusOK, restoreResp.StatusCode, "restore status")

	var rr restoreResponse
	require.NoError(t, json.NewDecoder(restoreResp.Body).Decode(&rr))
	require.GreaterOrEqual(t, rr.RestoredObjects, 5, "at least 5 objects restored")
	require.Empty(t, rr.StaleBlobs, "no stale blobs: blobs still exist")

	// Verify exactly 5 original objects remain
	listOut2, err := testS3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err)
	require.Len(t, listOut2.Contents, 5, "5 objects after restore (extras removed)")

	// Verify content of original objects
	for _, key := range objects {
		getResp, err := testS3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err, "get %s after restore", key)
		body, _ := io.ReadAll(getResp.Body)
		getResp.Body.Close()
		require.Equal(t, "content-"+key, string(body))
	}

	// Verify extra objects are gone
	for _, key := range extras {
		_, err := testS3Client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.Error(t, err, "extra object %s should be gone after restore", key)
	}
}

// TestSnapshot_List verifies GET /admin/snapshots returns sorted list.
func TestSnapshot_List(t *testing.T) {
	// Create 2 snapshots
	for i := 0; i < 2; i++ {
		createSnapshotE2E(t, fmt.Sprintf("list-test-%d", i))
	}

	listResp, err := http.Get(testServerURL + "/admin/snapshots") //nolint:noctx
	require.NoError(t, err)
	defer listResp.Body.Close()
	require.Equal(t, http.StatusOK, listResp.StatusCode)

	var lr snapshotListResponse
	require.NoError(t, json.NewDecoder(listResp.Body).Decode(&lr))
	require.GreaterOrEqual(t, len(lr.Snapshots), 2)

	// Verify sorted by seq ascending
	for i := 1; i < len(lr.Snapshots); i++ {
		require.Less(t, lr.Snapshots[i-1].Seq, lr.Snapshots[i].Seq)
	}
}

// TestSnapshot_NotFound verifies restore of nonexistent snapshot returns 404.
func TestSnapshot_NotFound(t *testing.T) {
	restoreResp, err := postJSON(testServerURL+"/admin/snapshots/999999/restore", nil)
	require.NoError(t, err)
	defer restoreResp.Body.Close()
	require.Equal(t, http.StatusNotFound, restoreResp.StatusCode)

	var er errorResponse
	require.NoError(t, json.NewDecoder(restoreResp.Body).Decode(&er))
	require.NotEmpty(t, er.Error)
	require.NotEmpty(t, er.Hint)
}
