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

// TestPITRE2E exercises the /admin/pitr HTTP surface (WAL replay, target-time
// exclusion, invalid time, no snapshot) against both single-node and 4-node
// cluster fixtures. /admin/pitr mutates global metadata state — dedicated
// per-branch fixture, one boot per branch, sub-tests run in sequence.
func TestPITRE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runPITRCases(t, newDedicatedSingleNodeS3Target(t, nil))
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		runPITRCases(t, newClusterS3TargetWithExtraArgs(t, 4, nil))
	})
}

func runPITRCases(t *testing.T, tgt s3Target) {
	t.Helper()
	serverURL := tgt.endpoint(0)
	client := tgt.pickNode(0)
	ctx := context.Background()

	// Invalid-input cases first so they don't accumulate fixture state.
	t.Run("InvalidTime", func(t *testing.T) {
		resp, err := postJSON(serverURL+"/admin/pitr", map[string]string{"to": "not-a-time"})
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})

	t.Run("NoSnapshot", func(t *testing.T) {
		// epoch+1s is always before any snapshot this fixture will produce,
		// so the "no snapshot before target" check is stable regardless of
		// what other sub-tests have already run on this fixture.
		veryOldTime := time.Unix(1, 0).UTC().Format(time.RFC3339Nano)
		resp, err := postJSON(serverURL+"/admin/pitr", map[string]string{"to": veryOldTime})
		require.NoError(t, err)
		defer resp.Body.Close()
		require.True(t, resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusBadRequest,
			"expected 404 or 400, got %d", resp.StatusCode)
	})

	t.Run("WALReplayAddsObjects", func(t *testing.T) {
		bucket := tgt.uniqueBucket(t, "walreplay")
		createPITRSnapshot(t, serverURL, "pitr-wal-base")

		included := []string{"inc1.txt", "inc2.txt", "inc3.txt"}
		for _, key := range included {
			_, err := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
				Body:   strings.NewReader("included-" + key),
			})
			require.NoError(t, err)
		}

		pivotTime := time.Now().UTC()
		time.Sleep(150 * time.Millisecond)

		excluded := []string{"exc1.txt", "exc2.txt"}
		for _, key := range excluded {
			_, err := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
				Body:   strings.NewReader("excluded-" + key),
			})
			require.NoError(t, err)
		}

		listOut, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
		require.NoError(t, err)
		require.Len(t, listOut.Contents, 5, "5 objects before PITR")

		pitrResp, err := postJSON(serverURL+"/admin/pitr", map[string]string{
			"to": pivotTime.Format(time.RFC3339Nano),
		})
		require.NoError(t, err)
		defer pitrResp.Body.Close()
		require.Equal(t, http.StatusOK, pitrResp.StatusCode, "PITR should succeed")

		var pr pitrResponse
		require.NoError(t, json.NewDecoder(pitrResp.Body).Decode(&pr))
		require.GreaterOrEqual(t, pr.WALEntriesReplayed, 3, "at least 3 WAL entries replayed")
		for _, b := range pr.StaleBlobs {
			require.NotEqual(t, bucket, b["bucket"], "unexpected stale blob in test bucket: %v", b)
		}

		listOut2, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
		require.NoError(t, err)
		require.Len(t, listOut2.Contents, 3, "only 3 included objects after PITR")

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

		for _, key := range excluded {
			_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			require.Error(t, err, "excluded %s should not exist after PITR", key)
		}
	})

	t.Run("ExcludesObjectsAddedAfterTarget", func(t *testing.T) {
		bucket := tgt.uniqueBucket(t, "excludes")

		originals := []string{"orig1.txt", "orig2.txt"}
		for _, key := range originals {
			_, err := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
				Body:   strings.NewReader("original-" + key),
			})
			require.NoError(t, err)
		}

		createPITRSnapshot(t, serverURL, "pitr-excl-base")

		pivotTime := time.Now().UTC()
		time.Sleep(150 * time.Millisecond)

		extras := []string{"extra1.txt", "extra2.txt", "extra3.txt"}
		for _, key := range extras {
			_, err := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
				Body:   strings.NewReader("extra-" + key),
			})
			require.NoError(t, err)
		}

		listOut, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
		require.NoError(t, err)
		require.Len(t, listOut.Contents, 5)

		pitrResp, err := postJSON(serverURL+"/admin/pitr", map[string]string{
			"to": pivotTime.Format(time.RFC3339Nano),
		})
		require.NoError(t, err)
		defer pitrResp.Body.Close()
		require.Equal(t, http.StatusOK, pitrResp.StatusCode)

		listOut2, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
		require.NoError(t, err)
		require.Len(t, listOut2.Contents, 2, "only 2 original objects after PITR")

		for _, key := range extras {
			_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			require.Error(t, err, "extra %s should not exist after PITR", key)
		}
	})
}
