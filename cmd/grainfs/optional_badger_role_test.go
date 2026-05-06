package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/badgerrole"
	"github.com/gritive/GrainFS/internal/serveruntime"
	"github.com/gritive/GrainFS/internal/storage"
)

func TestOptionalRoleDisabledRecognizesIncidentAPI(t *testing.T) {
	feature, ok := serveruntime.OptionalRoleDisabled(badgerrole.DefaultRegistry(), badgerrole.Decision{
		Role:   badgerrole.RoleIncidentState,
		Status: badgerrole.DecisionOpenFailed,
		Action: badgerrole.RecoveryActionDisableFeature,
		Reason: "open failed",
	})

	require.True(t, ok)
	require.Equal(t, "incident-api", feature)
}

func TestBuildVolumeManagerDisablesDedupWhenOptionalRoleCannotOpen(t *testing.T) {
	cmd := &cobra.Command{Use: "serve"}
	cmd.Flags().Bool("dedup", true, "")
	cmd.Flags().Int64("block-cache-size", 0, "")
	dataFile := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(dataFile, []byte("x"), 0o644))
	backend, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	defer backend.Close()

	mgr, cache, dedupDB, err := buildVolumeManager(cmd, dataFile, backend)

	require.NoError(t, err)
	require.NotNil(t, mgr)
	require.NotNil(t, cache)
	require.Nil(t, dedupDB)
}

func TestSetupClusterReceiptDisablesOptionalRoleWhenDBCannotOpen(t *testing.T) {
	cmd := &cobra.Command{Use: "serve"}
	cmd.Flags().Bool("heal-receipt-enabled", true, "")
	cmd.Flags().String("heal-receipt-psk", "test-psk", "")
	cmd.Flags().Duration("heal-receipt-retention", time.Hour, "")
	cmd.Flags().Duration("heal-receipt-gossip-interval", time.Hour, "")
	cmd.Flags().Int("heal-receipt-window", 1, "")
	dataFile := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(dataFile, []byte("x"), 0o644))

	opts, wiring, err := setupClusterReceipt(
		context.Background(),
		cmd,
		dataFile,
		"node-a",
		"",
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
