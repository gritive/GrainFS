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
