package cluster

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/metrics"
)

func TestAppendForwardBufferMetrics(t *testing.T) {
	sem := newAppendForwardBuffer(1024)

	before := testutil.ToFloat64(metrics.AppendForwardBufferInflightBytes)
	require.NoError(t, sem.Acquire(context.Background(), 600))
	if got := testutil.ToFloat64(metrics.AppendForwardBufferInflightBytes); got-before != 600 {
		t.Errorf("inflight gauge after Acquire = %v; want before+600", got)
	}
	sem.Release(600)
	if got := testutil.ToFloat64(metrics.AppendForwardBufferInflightBytes); got != before {
		t.Errorf("inflight gauge after Release = %v; want before", got)
	}
}
