package cluster

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/metrics"
)

// runBackfillSweepOnce drives one full backfill sweep cycle against backend b,
// mirroring scrubber.BackgroundScrubber.perVersionBackfillSweep. It uses the
// internal walkPerVersionBackfillCandidates + backfillPerVersionBlob methods
// directly (available because this test is in package cluster) so that:
//
//   - The age gate is bypassed (minAge=0) — freshly-created test fixtures pass.
//   - Metrics are incremented exactly as the production sweep does.
//
// The maxPerCycle cap is set to 1000 so the cycle is not artificially bounded
// for the small fixture sets used by integration tests.
func runBackfillSweepOnce(t *testing.T, b *DistributedBackend) int {
	t.Helper()
	ctx := context.Background()
	const maxPerCycle = 1000

	buckets, err := b.ListBackfillBuckets(ctx)
	require.NoError(t, err)

	migrated := 0
	for _, bucket := range buckets {
		walkErr := b.walkPerVersionBackfillCandidates(bucket, nowUnixSec(), 0 /* minAge=0: bypass age gate for test fixtures */, func(c perVersionBackfillCandidate) error {
			if migrated >= maxPerCycle {
				return nil
			}
			metrics.QuorumMetaVersionBackfillFoundTotal.Inc()
			if err := b.backfillPerVersionBlob(ctx, c); err != nil {
				metrics.QuorumMetaVersionBackfillErrorTotal.Inc()
				t.Errorf("backfillPerVersionBlob failed for %s/%s@%s: %v", c.Bucket, c.Key, c.VersionID, err)
				return nil
			}
			metrics.QuorumMetaVersionBackfillMigratedTotal.Inc()
			migrated++
			return nil
		})
		require.NoError(t, walkErr, "walkPerVersionBackfillCandidates must not fail for bucket %s", bucket)
	}
	return migrated
}

// TestPerVersionBackfillSweep_EndToEnd is the end-to-end integration test for
// the Foundation S3 per-version backfill sweep. It proves:
//
//	(a) after removing per-version blobs (simulating a pre-S1 gap), a single
//	    sweep cycle restores every blob (readQuorumMetaVersions returns all VIDs);
//	(b) MigratedTotal increased by exactly the number of gaps filled;
//	(c) a second sweep cycle migrates zero (idempotent);
//	(d) the backfilled delete-marker version decodes with IsDeleteMarker == true.
func TestPerVersionBackfillSweep_EndToEnd(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)

	const bkt, key = "backfill-e2e-bkt", "obj"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))

	// Write three versions: v1, v2 (regular), and a delete marker.
	v1 := putVersioned(t, b, ctx, bkt, key, "content-v1")
	v2 := putVersioned(t, b, ctx, bkt, key, "content-v2")
	mk := deleteVersioned(t, b, bkt, key) // soft-delete → marker version

	// Confirm all three per-version blobs exist (written by S1's fan-out).
	allVIDs := []string{v1, v2, mk}

	// Simulate a pre-S1 gap: remove v1's and the delete-marker's blobs.
	// v2's blob is left intact — the walker must skip it.
	removePerVersionBlob(t, b, bkt, key, v1)
	removePerVersionBlob(t, b, bkt, key, mk)
	const gapCount = 2

	// Baseline metric snapshots (counters are cumulative across tests in the
	// same process — always delta-check).
	beforeMigrated := testutil.ToFloat64(metrics.QuorumMetaVersionBackfillMigratedTotal)

	// --- (a) + (b): First sweep cycle fills the gaps ---
	cycle1 := runBackfillSweepOnce(t, b)

	require.Equal(t, gapCount, cycle1,
		"first sweep must migrate exactly the %d removed blobs", gapCount)

	afterMigrated := testutil.ToFloat64(metrics.QuorumMetaVersionBackfillMigratedTotal)
	require.InDelta(t, float64(gapCount), afterMigrated-beforeMigrated, 0.001,
		"MigratedTotal must increase by exactly %d (the gap count)", gapCount)

	// All three blobs must now be present: readQuorumMetaVersions must return
	// all three VIDs.
	cmds, err := b.readQuorumMetaVersions(bkt, key)
	require.NoError(t, err)
	gotVIDs := make(map[string]bool, len(cmds))
	for _, c := range cmds {
		gotVIDs[c.VersionID] = true
	}
	for _, vid := range allVIDs {
		require.True(t, gotVIDs[vid], "readQuorumMetaVersions must return VID %s after backfill", vid)
	}

	// --- (d): Delete-marker round-trip correctness ---
	// The backfilled marker blob must decode with IsDeleteMarker=true so
	// deriveLatestVersion treats it as a soft-delete, not a live object.
	markerCmd := decodePerVersionBlob(t, b, bkt, key, mk)
	require.True(t, markerCmd.IsDeleteMarker,
		"backfilled delete-marker blob must decode with IsDeleteMarker=true")

	// --- (c): Second sweep cycle must migrate zero (idempotent) ---
	beforeMigrated2 := testutil.ToFloat64(metrics.QuorumMetaVersionBackfillMigratedTotal)
	cycle2 := runBackfillSweepOnce(t, b)

	require.Equal(t, 0, cycle2,
		"second sweep must migrate zero (all blobs already present — idempotent)")
	afterMigrated2 := testutil.ToFloat64(metrics.QuorumMetaVersionBackfillMigratedTotal)
	require.InDelta(t, 0.0, afterMigrated2-beforeMigrated2, 0.001,
		"MigratedTotal must not change on the second (idempotent) sweep cycle")
}
