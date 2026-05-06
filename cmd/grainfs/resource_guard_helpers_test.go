package main

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFDWatchFlagDefault(t *testing.T) {
	flag := serveCmd.Flags().Lookup("fd-watch-enabled")
	require.NotNil(t, flag)
	assert.Equal(t, "true", flag.DefValue)
}

func TestFDWatchEnabledHelper(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.Flags().Bool("fd-watch-enabled", true, "")
	assert.True(t, fdWatchEnabled(cmd))
	require.NoError(t, cmd.Flags().Set("fd-watch-enabled", "false"))
	assert.False(t, fdWatchEnabled(cmd))
}

func TestGoroutineWatchFlagDefault(t *testing.T) {
	flag := serveCmd.Flags().Lookup("goroutine-watch-enabled")
	require.NotNil(t, flag)
	assert.Equal(t, "true", flag.DefValue)
}

func TestGoroutineWatchEnabledHelper(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.Flags().Bool("goroutine-watch-enabled", true, "")
	assert.True(t, goroutineWatchEnabled(cmd))
	require.NoError(t, cmd.Flags().Set("goroutine-watch-enabled", "false"))
	assert.False(t, goroutineWatchEnabled(cmd))
}
