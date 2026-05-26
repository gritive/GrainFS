package serveruntime

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/badgerrole"
)

func TestOptionalRoleDisabledRecognizesIncidentAPI(t *testing.T) {
	feature, ok := OptionalRoleDisabled(badgerrole.DefaultRegistry(), badgerrole.Decision{
		Role:   badgerrole.RoleIncidentState,
		Status: badgerrole.DecisionOpenFailed,
		Action: badgerrole.RecoveryActionDisableFeature,
		Reason: "open failed",
	})

	require.True(t, ok)
	require.Equal(t, "incident-api", feature)
}

func TestSetupClusterReceiptDisablesOptionalRoleWhenDBCannotOpen(t *testing.T) {
	dataFile := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(dataFile, []byte("x"), 0o644))

	opts, wiring, err := SetupClusterReceipt(
		context.Background(),
		ReceiptOptions{
			Enabled:        true,
			PSK:            "test-psk",
			Retention:      time.Hour,
			GossipInterval: time.Hour,
			WindowSize:     1,
		},
		dataFile,
		"node-a",
		nil,
		nil,
		nil,
		nil,
		nil,
	)

	require.NoError(t, err)
	require.Nil(t, wiring)
	require.Empty(t, opts)
}
