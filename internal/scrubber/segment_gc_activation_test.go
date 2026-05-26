package scrubber_test

import (
	"context"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/storage"
)

// manifestSegmentBackend = segmentBackend + the segmentManifestSource methods,
// so the scrubber runs the real hoisted known-set union (live versions + frozen).
// The segmentManifestSource interface is unexported in package scrubber; we satisfy
// it structurally so the scrubber can type-assert to it.
type manifestSegmentBackend struct {
	*segmentBackend
	liveObjects []storage.SnapshotObject
	frozen      map[string][]string // bucket -> frozen paths
}

func (m *manifestSegmentBackend) ListAllObjects() ([]storage.SnapshotObject, error) {
	return m.liveObjects, nil
}

func (m *manifestSegmentBackend) AllFrozenSegmentPaths() (map[string][]string, error) {
	return m.frozen, nil
}

// TestSegmentGCActivation_PreservesAndReclaims drives two phases of real scrub
// cycles against a real persistent SegmentOrphanLog (badger) to verify that:
//
//  1. Live-version segments (via ListAllObjects) and snapshot-frozen segments
//     (via AllFrozenSegmentPaths) are preserved even when they are old enough to
//     pass the 5m walker age gate — the hoisted known-set must exclude them from
//     candidacy.
//
//  2. A true orphan (absent from all three known-set sources) is reclaimable once
//     the retention window elapses: Phase A (window=1h) tombstones but keeps;
//     Phase B (window=0, same persistent log) finds t_zero already recorded and
//     immediately deletes on the second RunOnce.
//
//  3. Persistence of t_zero across scrubber restarts: scB is a brand-new scrubber
//     with an empty segmentTombstone; the persisted t_zero (written by scA) allows
//     scB to confirm the candidate and delete it.
func TestSegmentGCActivation_PreservesAndReclaims(t *testing.T) {
	b := &manifestSegmentBackend{
		segmentBackend: newSegmentBackend(),
		frozen:         map[string][]string{},
	}
	b.records["bucket"] = nil // bucket exists; no EC records

	// Build canonical paths through storage.SegmentKnownPath so they agree with
	// what the scrubber builds from ListAllObjects and AllFrozenSegmentPaths.
	livePath := storage.SegmentKnownPath("bucket", "live", "Lblob")
	frozenPath := storage.SegmentKnownPath("bucket", "frozenobj", "Fblob")
	orphanPath := storage.SegmentKnownPath("bucket", "orphanobj", "Oblob")

	// All three candidates are older than the walker's 5m age gate, so age alone
	// cannot explain why live+frozen are preserved — only the known-set can.
	b.addOrphanSegment(livePath, 10*time.Minute)
	b.addOrphanSegment(frozenPath, 10*time.Minute)
	b.addOrphanSegment(orphanPath, 10*time.Minute)

	// livePath is referenced by a live object version (via ListAllObjects).
	b.liveObjects = []storage.SnapshotObject{{
		Bucket:    "bucket",
		Key:       "live",
		VersionID: "v1",
		Segments:  []storage.SegmentRef{{BlobID: "Lblob"}},
	}}
	// frozenPath is pinned by a snapshot (via AllFrozenSegmentPaths).
	b.frozen["bucket"] = []string{frozenPath}

	// Real persistent store — proves t_zero survives a scrubber restart.
	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLoggingLevel(badger.ERROR))
	require.NoError(t, err)
	defer db.Close()
	orphanLog := cluster.NewSegmentOrphanLog(db, "group-0")

	// ── Phase A: generous window (1h) ──────────────────────────────────────────
	// Two RunOnce calls:
	//   cycle 1: tombstone loop no-ops (empty segmentTombstone); observe loop
	//            finds Oblob as candidate, writes t_zero to badger.
	//   cycle 2: tombstone loop checks Oblob; t_zero is recent, window=1h →
	//            evalGCCandidate returns false → keep.
	// Live+frozen paths are in the known-set (buildKnownSegments) so they never
	// reach candidacy in WalkOrphanSegments.
	scA := scrubber.New(b, time.Hour, scrubber.WithNoRetry(), scrubber.WithSegmentOrphanLog(orphanLog, time.Hour))
	scA.RunOnce(context.Background()) // tombstone + observe Oblob
	scA.RunOnce(context.Background()) // within window -> keep
	require.Empty(t, b.deletedSegments, "Phase A: nothing deleted within window; live+frozen never candidates")

	// ── Phase B: window=0, same persistent log ─────────────────────────────────
	// scB is a fresh scrubber with an empty in-memory segmentTombstone. The
	// persisted t_zero (written by scA in Phase A) is already in badger.
	//   cycle 1: tombstone loop no-ops (fresh segmentTombstone); observe loop
	//            finds Oblob again, calls Observe — idempotent first-wins, t_zero
	//            unchanged; Oblob enters segmentTombstone.
	//   cycle 2: tombstone loop checks Oblob; window=0 → evalGCCandidate returns
	//            true (now - t_zero > 0) → delete Oblob only.
	// Live+frozen remain in the known-set so they are still never candidates.
	scB := scrubber.New(b, time.Hour, scrubber.WithNoRetry(), scrubber.WithSegmentOrphanLog(orphanLog, 0))
	scB.RunOnce(context.Background()) // re-tombstone (Observe is a no-op first-wins)
	scB.RunOnce(context.Background()) // past window -> delete Oblob only

	require.Equal(t, []string{orphanPath}, b.deletedSegments,
		"Phase B: only the true orphan is deleted; live+frozen preserved via known-set")
}
