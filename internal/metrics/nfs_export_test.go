package metrics

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNFSExportMetricsRegistered(t *testing.T) {
	require.NotNil(t, NFSExportsTotal.WithLabelValues("ro"))
	require.NotNil(t, NFSExportsTotal.WithLabelValues("rw"))
	NFSExportPropagationSeconds.Observe(0.1)
	NFSLookupUnknownExportTotal.Inc()
	NFSRevokedStateIDs.WithLabelValues("export_remove").Inc()
	NFSRevokedStateIDs.WithLabelValues("export_ro_update").Inc()
}
