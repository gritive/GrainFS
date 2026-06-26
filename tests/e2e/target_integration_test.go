//go:build integration

package e2e

import "testing"

// newDedicatedCluster4NodeS3Target boots a per-test 4-node cluster. All
// runtime flags are caller-controlled via extraArgs (lifecycle-interval,
// etc.) -- mirrors newDedicatedSingleNodeS3Target for single/cluster parity.
// Boot cost is the trade-off for being able to exercise versioning-dependent
// lifecycle behavior (DM reclaim) in a real cluster topology. Vanilla cluster
// lifecycle tests should keep using the package-global shared cluster
// fixture where boot cost dominates.
//
// The fixture's tgt.name is "cluster-4-dedicated" -- sub-tests use this
// to discriminate cluster-only flows (e.g., DM sub-test).
func newDedicatedCluster4NodeS3Target(t testing.TB, extraArgs []string) s3Target {
	t.Helper()
	tgt := newClusterS3TargetWithExtraArgs(t, 4, extraArgs)
	tgt.name = "cluster-4-dedicated"
	return tgt
}
