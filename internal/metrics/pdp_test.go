package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestPDPRequestsTotal(t *testing.T) {
	PDPRequestsTotal.Reset()
	PDPRequestsTotal.WithLabelValues("admin", "error", "timeout", "closed").Inc()
	PDPRequestsTotal.WithLabelValues("admin", "allow", "", "closed").Inc()
	require.Equal(t, 2, testutil.CollectAndCount(PDPRequestsTotal))
	require.InDelta(t, 1.0, testutil.ToFloat64(PDPRequestsTotal.WithLabelValues("admin", "error", "timeout", "closed")), 0.0001)
}

func TestPDPCacheMetrics(t *testing.T) {
	PDPCacheTotal.Reset()
	PDPCacheTotal.WithLabelValues("admin", "hit", "allow").Inc()
	require.Equal(t, 1, testutil.CollectAndCount(PDPCacheTotal))
	PDPCacheEntries.WithLabelValues("admin").Set(3)
	require.InDelta(t, 3.0, testutil.ToFloat64(PDPCacheEntries.WithLabelValues("admin")), 0.0001)
}

func TestPDPMetrics_HaveScopeLabel(t *testing.T) {
	PDPCacheEntries.Reset() // isolate this test's gauge values from other tests
	PDPRequestsTotal.WithLabelValues("admin", "allow", "", "closed").Inc()
	PDPCacheTotal.WithLabelValues("admin", "hit", "allow").Inc()
	PDPRequestDuration.WithLabelValues("admin").Observe(0.01)
	PDPCacheEntries.WithLabelValues("admin").Set(2)
	PDPCacheEntries.WithLabelValues("protocol_credential").Set(7)
	require.InDelta(t, 2.0, testutil.ToFloat64(PDPCacheEntries.WithLabelValues("admin")), 0.0001)
	require.InDelta(t, 7.0, testutil.ToFloat64(PDPCacheEntries.WithLabelValues("protocol_credential")), 0.0001)
}
