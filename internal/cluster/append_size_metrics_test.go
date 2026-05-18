package cluster

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/metrics"
)

// histSampleCount returns the current sample_count of a histogram by gathering
// the default registry. Panics only on unexpected gather errors.
func histSampleCount(t *testing.T, name string) uint64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() == name {
			for _, m := range mf.GetMetric() {
				if h := m.GetHistogram(); h != nil {
					return h.GetSampleCount()
				}
			}
		}
	}
	return 0
}

// noSeekReader wraps an io.Reader to hide io.Seeker, preventing the coordinator
// pre-check from computing chunkSize. This forces the FSM to be the only cap
// enforcement point, exercising AppendSizeCapRejectedTotal.
type noSeekReader struct{ r io.Reader }

func (n *noSeekReader) Read(p []byte) (int, error) { return n.r.Read(p) }

// TestAppendSizeMetricsObserved verifies:
//   - AppendSizeCapRejectedTotal increments on an FSM-rejected cap append (T14).
//   - AppendCoalescedDepth receives a sample on a successful append (T16).
//   - AppendCoalescedTotalBytes receives a sample on a successful append (T16).
func TestAppendSizeMetricsObserved(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "b"))

	// --- T14 regression: FSM cap rejection increments AppendSizeCapRejectedTotal ---
	// Use noSeekReader to bypass the coordinator pre-check so the FSM is the
	// sole enforcement point and AppendSizeCapRejectedTotal can increment.
	b.SetCoalesceConfig(CoalesceConfig{
		SegmentCount: 1 << 30, SizeBytes: 1 << 60, IdleTimeout: 1 << 60,
		SizeCapBytes: 8,
	})
	// First append (5 bytes) — establishes existing object.
	_, err := b.AppendObject(ctx, "b", "k-cap", 0, &noSeekReader{bytes.NewReader([]byte("hello"))})
	require.NoError(t, err, "first append must succeed")
	rejBefore := testutil.ToFloat64(metrics.AppendSizeCapRejectedTotal)
	// Second append (4 bytes, non-seeker): 5+4=9 > 8 — FSM rejects it.
	_, err = b.AppendObject(ctx, "b", "k-cap", 5, &noSeekReader{bytes.NewReader([]byte("more"))})
	require.Error(t, err, "over-cap append must error")
	rejAfter := testutil.ToFloat64(metrics.AppendSizeCapRejectedTotal)
	require.GreaterOrEqual(t, rejAfter-rejBefore, 1.0, "AppendSizeCapRejectedTotal must increment")

	// --- T16: successful append observes depth and total_bytes histograms ---
	// Use histSampleCount to read sample_count from the default gatherer since
	// testutil.ToFloat64 panics on histogram metrics.
	b.SetCoalesceConfig(CoalesceConfig{
		SegmentCount: 1 << 30, SizeBytes: 1 << 60, IdleTimeout: 1 << 60,
		SizeCapBytes: 0, // no cap
	})
	depthCountBefore := histSampleCount(t, "grainfs_append_coalesced_depth")
	bytesCountBefore := histSampleCount(t, "grainfs_append_coalesced_total_bytes")

	_, err = b.AppendObject(ctx, "b", "k-ok", 0, bytes.NewReader([]byte("hi")))
	require.NoError(t, err, "uncapped append must succeed")

	depthCountAfter := histSampleCount(t, "grainfs_append_coalesced_depth")
	bytesCountAfter := histSampleCount(t, "grainfs_append_coalesced_total_bytes")
	require.Greater(t, depthCountAfter, depthCountBefore, "AppendCoalescedDepth must receive a sample")
	require.Greater(t, bytesCountAfter, bytesCountBefore, "AppendCoalescedTotalBytes must receive a sample")
}
