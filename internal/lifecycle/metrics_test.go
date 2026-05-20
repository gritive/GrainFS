package lifecycle_test

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"

	"github.com/gritive/GrainFS/internal/lifecycle"
	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/storage"
)

// TestMPUWorker_EmitsAbortedUploadsMetric covers the node_id-labelled MPU
// counter and the rule_match counter for the abort_mpu action.
func TestMPUWorker_EmitsAbortedUploadsMetric(t *testing.T) {
	now := time.Date(2026, 5, 19, 0, 0, 0, 0, time.UTC)
	backend := &fakeMPUBackend{uploads: []storage.MultipartUploadRecord{
		{Bucket: "b", Key: "k", UploadID: "u", InitiatedAt: now.Add(-4 * 24 * time.Hour).Unix()},
	}}
	deleter := &fakeMPUDeleter{}
	store := lifecycle.NewStore(newBadger(t))
	require.NoError(t, store.PutRaw("b", []byte(`<LifecycleConfiguration><Rule><ID>r</ID><Status>Enabled</Status><AbortIncompleteMultipartUpload><DaysAfterInitiation>3</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>`)))

	abortedBefore := testutil.ToFloat64(metrics.LifecycleAbortedUploads.WithLabelValues("b", "node-1"))
	matchBefore := testutil.ToFloat64(metrics.LifecycleRuleMatch.WithLabelValues("r", "abort_mpu"))

	limiter := rate.NewLimiter(100, 10)
	w := lifecycle.NewMPUWorker(store, backend, deleter, time.Minute, limiter,
		lifecycle.WithMPUNowForTest(func() time.Time { return now }),
		lifecycle.WithMPUNodeID("node-1"),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	w.RunCycleForTest(ctx)

	require.Equal(t, abortedBefore+1, testutil.ToFloat64(metrics.LifecycleAbortedUploads.WithLabelValues("b", "node-1")))
	require.Equal(t, matchBefore+1, testutil.ToFloat64(metrics.LifecycleRuleMatch.WithLabelValues("r", "abort_mpu")))
}
