package e2e

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

type pitrResponse struct {
	RestoredObjects    int              `json:"restored_objects"`
	WALEntriesReplayed int              `json:"wal_entries_replayed"`
	StaleBlobs         []map[string]any `json:"stale_blobs"`
}

func createPITRSnapshot(t *testing.T, serverURL, reason string) {
	t.Helper()
	var lastErr error
	var lastStatus int
	var lastBody string

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
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
		if resp.StatusCode == http.StatusOK {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}

	require.Failf(t,
		"snapshot should become available after cluster data groups are ready",
		"lastErr=%v status=%d body=%s",
		lastErr, lastStatus, lastBody)
}

// TestPITR_WALReplayAddsObjects verifies WAL replay: objects PUT between snapshot
// and targetTime appear in the PITR result, objects PUT after targetTime do not.
// Note: ECBackend removes shard files on delete, so this test avoids deletes.
func TestPITR_WALReplayAddsObjects(t *testing.T) {
	ctx := context.Background()
	bucket := "pitr-wal-replay"
	serverURL, client := startIsolatedE2EServer(t)
	createBucketWithClient(t, client, bucket)

	// Create snapshot BEFORE putting any objects (empty base)
	createPITRSnapshot(t, serverURL, "pitr-wal-base")

	// Put 3 objects AFTER snapshot — WAL records these
	included := []string{"inc1.txt", "inc2.txt", "inc3.txt"}
	for _, key := range included {
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   strings.NewReader("included-" + key),
		})
		require.NoError(t, err)
	}

	// Record pivot time: WAL entries before this should be replayed
	pivotTime := time.Now().UTC()
	time.Sleep(150 * time.Millisecond)

	// Put 2 objects AFTER pivot time — WAL records these (should be excluded)
	excluded := []string{"exc1.txt", "exc2.txt"}
	for _, key := range excluded {
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   strings.NewReader("excluded-" + key),
		})
		require.NoError(t, err)
	}

	// Verify 5 objects total exist now
	listOut, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	require.NoError(t, err)
	require.Len(t, listOut.Contents, 5, "5 objects before PITR")

	// PITR to pivotTime — should include inc1-3 (WAL replay) but not exc1-2
	pitrResp, err := postJSON(serverURL+"/admin/pitr", map[string]string{
		"to": pivotTime.Format(time.RFC3339Nano),
	})
	require.NoError(t, err)
	defer pitrResp.Body.Close()
	require.Equal(t, http.StatusOK, pitrResp.StatusCode, "PITR should succeed")

	var pr pitrResponse
	require.NoError(t, json.NewDecoder(pitrResp.Body).Decode(&pr))
	require.GreaterOrEqual(t, pr.WALEntriesReplayed, 3, "at least 3 WAL entries replayed (the included puts)")
	for _, b := range pr.StaleBlobs {
		require.NotEqual(t, bucket, b["bucket"], "unexpected stale blob in test bucket: %v", b)
	}

	// Verify only included objects remain in this bucket
	listOut2, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	require.NoError(t, err)
	require.Len(t, listOut2.Contents, 3, "only 3 included objects after PITR")

	// Verify content of included objects
	for _, key := range included {
		getResp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err, "get %s after PITR", key)
		body, _ := io.ReadAll(getResp.Body)
		getResp.Body.Close()
		require.Equal(t, "included-"+key, string(body))
	}

	// Verify excluded objects are gone
	for _, key := range excluded {
		_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.Error(t, err, "excluded %s should not exist after PITR", key)
	}
}

// TestPITR_ExcludesObjectsAddedAfterTarget verifies that objects PUT after the
// target time are not visible after PITR restore.
func TestPITR_ExcludesObjectsAddedAfterTarget(t *testing.T) {
	ctx := context.Background()
	bucket := "pitr-e2e-excludes"
	serverURL, client := startIsolatedE2EServer(t)
	createBucketWithClient(t, client, bucket)

	// Put 2 original objects
	originals := []string{"orig1.txt", "orig2.txt"}
	for _, key := range originals {
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   strings.NewReader("original-" + key),
		})
		require.NoError(t, err)
	}

	// Create snapshot
	createPITRSnapshot(t, serverURL, "pitr-excl-base")

	// Record pivot time (after snapshot, before new puts)
	pivotTime := time.Now().UTC()
	time.Sleep(150 * time.Millisecond)

	// Add objects AFTER pivot time
	extras := []string{"extra1.txt", "extra2.txt", "extra3.txt"}
	for _, key := range extras {
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   strings.NewReader("extra-" + key),
		})
		require.NoError(t, err)
	}

	// Verify 5 objects exist
	listOut, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err)
	require.Len(t, listOut.Contents, 5)

	// PITR to pivot time (before extras were added)
	pitrResp, err := postJSON(serverURL+"/admin/pitr", map[string]string{
		"to": pivotTime.Format(time.RFC3339Nano),
	})
	require.NoError(t, err)
	defer pitrResp.Body.Close()
	require.Equal(t, http.StatusOK, pitrResp.StatusCode)

	// Verify only originals remain
	listOut2, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err)
	require.Len(t, listOut2.Contents, 2, "only 2 original objects after PITR")

	// Verify extras are gone
	for _, key := range extras {
		_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.Error(t, err, "extra %s should not exist after PITR", key)
	}
}

// TestPITR_InvalidTime verifies that an invalid time format returns 400.
func TestPITR_InvalidTime(t *testing.T) {
	serverURL, _ := startIsolatedE2EServer(t)

	resp, err := postJSON(serverURL+"/admin/pitr", map[string]string{"to": "not-a-time"})
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// TestPITR_NoSnapshot verifies that PITR without any snapshot returns an appropriate error.
func TestPITR_NoSnapshot(t *testing.T) {
	serverURL, _ := startIsolatedE2EServer(t)

	// Use a very old time for which there's no snapshot before it (epoch+1s)
	veryOldTime := time.Unix(1, 0).UTC().Format(time.RFC3339Nano)
	resp, err := postJSON(serverURL+"/admin/pitr", map[string]string{"to": veryOldTime})
	require.NoError(t, err)
	defer resp.Body.Close()
	// No snapshot before this time → 404 or 400
	require.True(t, resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusBadRequest,
		"expected 404 or 400, got %d", resp.StatusCode)
}
