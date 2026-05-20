package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestServeCmd_RemovesManualECFlags guards against accidental re-introduction
// of the manual erasure-coding flags. EC config is now derived per data-group,
// not driven by global cobra flags.
//
// Split out of serve_cluster_key_test.go in Task 5b (pre-cutover, step 4) —
// that file was relocated to internal/serveruntime, but this test references
// the cmd-private serveCmd symbol so it has to stay in cmd. Per the Task 9
// audit-gate rule (plan line 524), tests bound to cmd-private symbols remain
// in cmd.
func TestServeCmd_RemovesManualECFlags(t *testing.T) {
	require.Nil(t, serveCmd.Flags().Lookup("ec-data"))
	require.Nil(t, serveCmd.Flags().Lookup("ec-parity"))
	require.Nil(t, serveCmd.Flags().Lookup("seed-groups"))
}

// The flag-default tests below were split out of reshard_manager_registry_test.go
// in Task 9e (step 4) — that file's registry test relocated to
// internal/serveruntime, but these reference cmd-private serveCmd /
// defaultReshardInterval and must stay in cmd per the Task 9 audit-gate rule.

func TestServeReshardIntervalDefault(t *testing.T) {
	flag := serveCmd.Flags().Lookup("reshard-interval")
	require.NotNil(t, flag)
	assert.Equal(t, defaultReshardInterval.String(), flag.DefValue)
}

func TestServeRingReshardIntervalDefault(t *testing.T) {
	flag := serveCmd.Flags().Lookup("ring-reshard-interval")
	require.NotNil(t, flag)
	assert.Equal(t, time.Hour.String(), flag.DefValue)
}

func TestServeDatagroupRefreshIntervalDefault(t *testing.T) {
	flag := serveCmd.Flags().Lookup("datagroup-refresh-interval")
	require.NotNil(t, flag)
	assert.Equal(t, time.Minute.String(), flag.DefValue)
}
