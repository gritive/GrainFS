package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/compat"
)

func TestMetaFSMApplyCapabilityActivateRecordsActiveFeature(t *testing.T) {
	f := NewMetaFSM()
	payload := buildMetaCapabilityActivatePayload(compat.CapabilityMigrationCutoverV1)
	cmd, err := encodeMetaCmd(clusterpb.MetaCmdTypeCapabilityActivate, payload)
	require.NoError(t, err)

	require.NoError(t, f.applyCmd(cmd))
	require.True(t, f.ActiveFeatures().Has(compat.CapabilityMigrationCutoverV1))
}
