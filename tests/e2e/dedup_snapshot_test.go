package e2e

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestE2EDedupSnapshot validates the full dedup+snapshot lifecycle via the
// grainfs CLI against an isolated server with --dedup=true.
//
// Historical regression target: before PR-B, the second CreateSnapshot under
// dedup returned "dedup + snapshots not supported in Phase A". This test
// asserts that path works end-to-end (create, list, rollback, delete).
//
// Block content verification is covered at the unit/integration layer
// (TestBadgerSnapshotStoreCreateRollback, TestDedupSnapshotSecondSnapshot).
// Volume clone is not exercised here — the dedup-layer SnapshotClone path is
// covered by unit tests, and `volume clone` has a pre-existing visibility bug
// (cloned volume not returned by `volume info`) that is out of PR-B scope.
//
// The isolated server's data dir is removed on teardown, so no explicit
// volume/snapshot cleanup is needed.
func TestE2EDedupSnapshot(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		dataDir, _, _ := startTestServer(t, "--dedup=true")

		volName := fmt.Sprintf("dedup-snap-e2e-%d", time.Now().UnixNano())
		cowCreateVolume(t, dataDir, volName, 4*1024*1024)

		// First snapshot — pre-PR-B this worked.
		snap1 := cowCreateSnapshot(t, dataDir, volName)
		require.NotEmpty(t, snap1)

		// Second snapshot — pre-PR-B this returned the "dedup + snapshots not
		// supported in Phase A" hard error. After PR-B it must succeed.
		snap2 := cowCreateSnapshot(t, dataDir, volName)
		require.NotEmpty(t, snap2)
		require.NotEqual(t, snap1, snap2)

		// List: both snapshots should appear.
		snaps := cowListSnapshots(t, dataDir, volName)
		require.Len(t, snaps, 2, "expected 2 snapshots under dedup")

		// Rollback to snap1 — must not error; snapshot maps are preserved.
		cowRollback(t, dataDir, volName, snap1)
		snaps = cowListSnapshots(t, dataDir, volName)
		require.GreaterOrEqual(t, len(snaps), 1, "rollback must preserve at least the target snap")

		// Delete snap2 to exercise the dedup DeleteSnapshot path.
		cowDeleteSnapshot(t, dataDir, volName, snap2)
		snaps = cowListSnapshots(t, dataDir, volName)
		for _, s := range snaps {
			require.NotEqual(t, snap2, s.ID, "deleted snap must not reappear")
		}
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		_ = newSharedClusterS3Target(t)
	})
}
