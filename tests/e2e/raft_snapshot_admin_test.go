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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestE2E_RaftSnapshotAdminTriggerStatusAndMetrics(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	bucket := "raft-snapshot-admin-e2e"
	_, err := testS3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err)

	resp, err := http.Post(testServerURL+"/admin/raft/snapshot", "application/json", nil) //nolint:noctx
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var trigger struct {
		Index     uint64 `json:"index"`
		Term      uint64 `json:"term"`
		SizeBytes int    `json:"size_bytes"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&trigger))
	require.NotZero(t, trigger.Index)
	require.NotZero(t, trigger.Term)
	require.Positive(t, trigger.SizeBytes)

	statusResp, err := http.Get(testServerURL + "/admin/raft/snapshot") //nolint:noctx
	require.NoError(t, err)
	defer statusResp.Body.Close()
	require.Equal(t, http.StatusOK, statusResp.StatusCode)

	var status struct {
		Available bool   `json:"available"`
		Index     uint64 `json:"index"`
		Term      uint64 `json:"term"`
		SizeBytes int    `json:"size_bytes"`
	}
	require.NoError(t, json.NewDecoder(statusResp.Body).Decode(&status))
	assert.True(t, status.Available)
	assert.Equal(t, trigger.Index, status.Index)
	assert.Equal(t, trigger.Term, status.Term)
	assert.Equal(t, trigger.SizeBytes, status.SizeBytes)

	metricsResp, err := http.Get(testServerURL + "/metrics") //nolint:noctx
	require.NoError(t, err)
	defer metricsResp.Body.Close()
	require.Equal(t, http.StatusOK, metricsResp.StatusCode)
	body, err := io.ReadAll(metricsResp.Body)
	require.NoError(t, err)
	metrics := string(body)
	assert.Contains(t, metrics, `grainfs_raft_snapshot_trigger_total{outcome="success"} 1`)
	assert.Contains(t, metrics, "grainfs_raft_snapshot_last_index")
	assert.True(t, strings.Contains(metrics, "grainfs_raft_snapshot_last_size_bytes"))
}
