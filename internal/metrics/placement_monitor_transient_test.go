package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestPlacementMonitorTransientReadErrorRegisteredAndIncrements(t *testing.T) {
	require.NotNil(t, PlacementMonitorTransientReadError.WithLabelValues("segment"))
	require.NotNil(t, PlacementMonitorTransientReadError.WithLabelValues("coalesced"))

	before := testutil.ToFloat64(PlacementMonitorTransientReadError.WithLabelValues("segment"))
	PlacementMonitorTransientReadError.WithLabelValues("segment").Inc()
	after := testutil.ToFloat64(PlacementMonitorTransientReadError.WithLabelValues("segment"))

	require.Equal(t, float64(1), after-before)
}
