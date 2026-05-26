package e2e

import (
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// Dedup snapshot specs validate the full dedup+snapshot lifecycle via the
// grainfs CLI against an isolated server (dedup is always enabled).
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
var _ = ginkgo.Describe("Dedup snapshots", func() {
	ginkgo.Context("SingleNode", func() {
		var dataDir string

		ginkgo.BeforeEach(func() {
			dataDir, _, _ = startTestServer(ginkgo.GinkgoTB())
		})

		ginkgo.It("supports create, list, rollback, and delete", func() {
			t := ginkgo.GinkgoTB()
			volName := fmt.Sprintf("dedup-snap-e2e-%d", time.Now().UnixNano())
			cowCreateVolume(t, dataDir, volName, 4*1024*1024)

			// First snapshot — pre-PR-B this worked.
			snap1 := cowCreateSnapshot(t, dataDir, volName)
			gomega.Expect(snap1).NotTo(gomega.BeEmpty())

			// Second snapshot — pre-PR-B this returned the "dedup + snapshots not
			// supported in Phase A" hard error. After PR-B it must succeed.
			snap2 := cowCreateSnapshot(t, dataDir, volName)
			gomega.Expect(snap2).NotTo(gomega.BeEmpty())
			gomega.Expect(snap2).NotTo(gomega.Equal(snap1))

			// List: both snapshots should appear.
			snaps := cowListSnapshots(t, dataDir, volName)
			gomega.Expect(snaps).To(gomega.HaveLen(2), "expected 2 snapshots under dedup")

			// Rollback to snap1 — must not error; snapshot maps are preserved.
			cowRollback(t, dataDir, volName, snap1)
			snaps = cowListSnapshots(t, dataDir, volName)
			gomega.Expect(len(snaps)).To(gomega.BeNumerically(">=", 1), "rollback must preserve at least the target snap")

			// Delete snap2 to exercise the dedup DeleteSnapshot path.
			cowDeleteSnapshot(t, dataDir, volName, snap2)
			snaps = cowListSnapshots(t, dataDir, volName)
			for _, s := range snaps {
				gomega.Expect(s.ID).NotTo(gomega.Equal(snap2), "deleted snap must not reappear")
			}
		})
	})
})
