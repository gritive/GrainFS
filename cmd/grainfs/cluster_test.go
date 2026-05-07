package main

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// clusterSubcommandNames returns the registered subcommand names of clusterCmd.
func clusterSubcommandNames() []string {
	names := make([]string, 0, len(clusterCmd.Commands()))
	for _, c := range clusterCmd.Commands() {
		names = append(names, c.Name())
	}
	return names
}

// rootSubcommandNames returns the registered subcommand names of rootCmd.
func rootSubcommandNames() []string {
	names := make([]string, 0, len(rootCmd.Commands()))
	for _, c := range rootCmd.Commands() {
		names = append(names, c.Name())
	}
	return names
}

func TestClusterPlanShowRemoved(t *testing.T) {
	assert.NotContains(t, clusterSubcommandNames(), "plan-show",
		"cluster plan-show stub removed; reintroduce when real local FSM read lands")
}

func TestClusterRebalanceRemoved(t *testing.T) {
	assert.NotContains(t, clusterSubcommandNames(), "rebalance",
		"cluster rebalance stub removed; reintroduce when real local FSM read lands")
}

func TestClusterJoinMovedToRoot(t *testing.T) {
	assert.NotContains(t, clusterSubcommandNames(), "join",
		"cluster join was moved to root as 'grainfs join' (bootstrap convention)")
	assert.Contains(t, rootSubcommandNames(), "join",
		"join must be registered at root alongside serve / migrate / doctor / recover")
}

// clusterClientFromCmd helper tests.

func makeEndpointCmd(t *testing.T, value string, set bool) *cobra.Command {
	t.Helper()
	cmd := &cobra.Command{Use: "test"}
	cmd.Flags().String("endpoint", "", "")
	if set {
		require.NoError(t, cmd.Flags().Set("endpoint", value))
	}
	return cmd
}

func TestClusterClientFromCmd_RequiresEndpoint(t *testing.T) {
	cmd := makeEndpointCmd(t, "", false)
	_, err := clusterClientFromCmd(cmd)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "admin endpoint not configured")
}

func TestClusterClientFromCmd_RejectsHTTPURL(t *testing.T) {
	cmd := makeEndpointCmd(t, "http://127.0.0.1:9000", true)
	_, err := clusterClientFromCmd(cmd)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be a UDS socket path")
	assert.Contains(t, err.Error(), "dashboard-only")
}

func TestClusterClientFromCmd_RejectsHTTPSURL(t *testing.T) {
	cmd := makeEndpointCmd(t, "https://example.com", true)
	_, err := clusterClientFromCmd(cmd)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be a UDS socket path")
}

func TestClusterClientFromCmd_AcceptsBarePath(t *testing.T) {
	cmd := makeEndpointCmd(t, "/tmp/admin.sock", true)
	client, err := clusterClientFromCmd(cmd)
	require.NoError(t, err)
	require.NotNil(t, client)
}

func TestClusterClientFromCmd_AcceptsUnixPrefix(t *testing.T) {
	cmd := makeEndpointCmd(t, "unix:/tmp/admin.sock", true)
	client, err := clusterClientFromCmd(cmd)
	require.NoError(t, err)
	require.NotNil(t, client)
}
