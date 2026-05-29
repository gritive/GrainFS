package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestPDPRequestsTotal(t *testing.T) {
	PDPRequestsTotal.Reset()
	PDPRequestsTotal.WithLabelValues("error", "timeout", "closed").Inc()
	PDPRequestsTotal.WithLabelValues("allow", "", "closed").Inc()
	require.Equal(t, 2, testutil.CollectAndCount(PDPRequestsTotal))
	require.InDelta(t, 1.0, testutil.ToFloat64(PDPRequestsTotal.WithLabelValues("error", "timeout", "closed")), 0.0001)
}

func TestPDPCacheMetrics(t *testing.T) {
	PDPCacheTotal.Reset()
	PDPCacheTotal.WithLabelValues("hit", "allow").Inc()
	require.Equal(t, 1, testutil.CollectAndCount(PDPCacheTotal))

	PDPCacheEntries.Set(3)
	require.InDelta(t, 3.0, testutil.ToFloat64(PDPCacheEntries), 0.0001)
}
