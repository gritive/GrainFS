// Cluster-only saturation e2e for the AppendObject forward buffer.
//
// Spins up a 4-node cluster with a deliberately tiny forward buffer (4 MiB)
// and drives 16 concurrent 1 MiB appends from rotating non-owner nodes.
// At least one request must surface a 503 SlowDown (mapped from
// ErrForwardBufferFull).
package e2e

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

// TestAppendForwardBufferSaturationE2E exercises the cluster forward-buffer
// saturation path. Single-node has no forward buffer, so the test set has
// only a Cluster4Node branch today; the shape stays consistent with the
// other dual-integrated entries so a future single-node analogue (e.g.,
// per-bucket admission control) can drop in.
func TestAppendForwardBufferSaturationE2E(t *testing.T) {
	t.Run("Cluster4Node", func(t *testing.T) {
		runAppendForwardBufferSaturationCases(t, newClusterS3TargetWithExtraArgs(t, 4, []string{
			"--cluster-append-forward-buffer-total-bytes", fmt.Sprintf("%d", 4*1024*1024),
			"--cluster-append-forward-buffer-max-per-request", fmt.Sprintf("%d", 64*1024*1024),
		}))
	})
}

func runAppendForwardBufferSaturationCases(t *testing.T, tgt s3Target) {
	t.Helper()
	bucket := tgt.uniqueBucket(t, "appendsatur")

	var slowDowns int64
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := bytes.Repeat([]byte{byte(i)}, 2*1024*1024)
			err := putAppend(tgt.pickNode(i%tgt.nodes), bucket, fmt.Sprintf("k-%d", i), 0, body)
			if err == nil {
				return
			}
			var apiErr smithy.APIError
			if errors.As(err, &apiErr) && apiErr.ErrorCode() == "SlowDown" {
				atomic.AddInt64(&slowDowns, 1)
			}
		}(i)
	}
	wg.Wait()
	require.GreaterOrEqual(t, atomic.LoadInt64(&slowDowns), int64(1),
		"expected at least one 503 SlowDown from forward buffer saturation")

	require.Eventually(t, func() bool {
		for i := 0; i < tgt.nodes; i++ {
			resp, err := http.Get(tgt.endpoint(i) + "/metrics")
			if err != nil {
				continue
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if bytes.Contains(body, []byte("grainfs_cluster_append_forward_buffer_rejected_total")) {
				return true
			}
		}
		return false
	}, 5*time.Second, 100*time.Millisecond, "rejected counter metric not exposed")
}
