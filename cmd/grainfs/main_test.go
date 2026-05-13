package main

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func testRootWithLogLevelFlag(t *testing.T) *cobra.Command {
	t.Helper()
	cmd := &cobra.Command{Use: "grainfs"}
	cmd.PersistentFlags().String("log-level", "info", "")
	return cmd
}

func TestEffectiveLogLevelUsesEnvWhenFlagDefault(t *testing.T) {
	t.Setenv("GRAINFS_LOG_LEVEL", "debug")
	cmd := testRootWithLogLevelFlag(t)

	require.Equal(t, "debug", effectiveLogLevel(cmd))
}

func TestEffectiveLogLevelFlagOverridesEnv(t *testing.T) {
	t.Setenv("GRAINFS_LOG_LEVEL", "debug")
	cmd := testRootWithLogLevelFlag(t)
	require.NoError(t, cmd.PersistentFlags().Set("log-level", "warn"))

	require.Equal(t, "warn", effectiveLogLevel(cmd))
}
