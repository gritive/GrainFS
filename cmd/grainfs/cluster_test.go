package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
