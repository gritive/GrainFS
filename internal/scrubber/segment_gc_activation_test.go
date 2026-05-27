package scrubber_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/chunkref"
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
	listErr     error               // when set, ListAllObjectsStrict fails closed
	frozen      map[string][]string // bucket -> frozen paths
	caughtUp    bool                // reported by CaughtUp; false gates off the sweep
}

// CaughtUp implements scrubber's caughtUpReporter. When false the scrubber skips
// the entire orphan-segment sweep for the cycle.
func (m *manifestSegmentBackend) CaughtUp(ctx context.Context) bool { return m.caughtUp }

func (m *manifestSegmentBackend) ListAllObjectsStrict() ([]storage.SnapshotObject, error) {
	return m.liveObjects, m.listErr
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
		caughtUp:       true, // sweep runs; the gate is exercised separately
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

	// Prove t_zero was actually persisted to the orphan-log (else Phase B's
	// deletion would not depend on persistence — the real claim under test).
	tz, ok, err := orphanLog.TombstoneTime(chunkref.ChunkID("Oblob"))
	require.NoError(t, err)
	require.True(t, ok, "Oblob t_zero must be persisted after first observe")
	require.False(t, tz.IsZero())

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

// TestSegmentGCFailsClosedOnKnownSetError proves the fail-closed invariant: when
// the known-set source (ListAllObjectsStrict) errors, hoistSegmentSources returns
// ok=false and the ENTIRE segment sweep is skipped — so even a past-window orphan
// is NOT deleted, rather than risk deleting a still-referenced segment whose
// object record could not be read.
func TestSegmentGCFailsClosedOnKnownSetError(t *testing.T) {
	b := &manifestSegmentBackend{
		segmentBackend: newSegmentBackend(),
		frozen:         map[string][]string{},
		listErr:        errors.New("corrupt object meta"),
		caughtUp:       true, // isolate the known-set fail-closed path from the caught-up gate
	}
	b.records["bucket"] = nil

	// A true orphan, old enough to pass the age gate. With a clean known-set it
	// would be tombstoned then (window=0) deleted. The known-set error must
	// prevent the sweep entirely.
	orphanPath := storage.SegmentKnownPath("bucket", "orphanobj", "Oblob")
	b.addOrphanSegment(orphanPath, 10*time.Minute)

	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLoggingLevel(badger.ERROR))
	require.NoError(t, err)
	defer db.Close()
	orphanLog := cluster.NewSegmentOrphanLog(db, "group-0")

	sc := scrubber.New(b, time.Hour, scrubber.WithNoRetry(), scrubber.WithSegmentOrphanLog(orphanLog, 0))
	sc.RunOnce(context.Background())
	sc.RunOnce(context.Background())

	require.Empty(t, b.deletedSegments,
		"known-set error must skip the whole segment sweep (fail-closed): nothing deleted")
}

// TestSegmentGCActivation_NotCaughtUpSkipsSweep proves the caught-up safety gate:
// when the backend reports CaughtUp(ctx)==false, the ENTIRE orphan-segment sweep
// is skipped for the cycle. A true past-window orphan is therefore never even
// observed (no t_zero recorded) and never deleted — the node's stale local view
// must not be allowed to reclaim a possibly-still-referenced segment.
func TestSegmentGCActivation_NotCaughtUpSkipsSweep(t *testing.T) {
	b := &manifestSegmentBackend{
		segmentBackend: newSegmentBackend(),
		frozen:         map[string][]string{},
		caughtUp:       false, // gate off the whole sweep
	}
	b.records["bucket"] = nil

	// A true orphan old enough to pass the age gate; with the gate open and
	// window=0 it would be tombstoned then deleted. The gate must prevent both.
	orphanPath := storage.SegmentKnownPath("bucket", "orphanobj", "Oblob")
	b.addOrphanSegment(orphanPath, 10*time.Minute)

	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLoggingLevel(badger.ERROR))
	require.NoError(t, err)
	defer db.Close()
	orphanLog := cluster.NewSegmentOrphanLog(db, "group-0")

	sc := scrubber.New(b, time.Hour, scrubber.WithNoRetry(), scrubber.WithSegmentOrphanLog(orphanLog, 0))
	sc.RunOnce(context.Background())
	sc.RunOnce(context.Background())

	require.Empty(t, b.deletedSegments,
		"not caught up: the whole segment sweep is gated off; nothing deleted")
	_, ok, err := orphanLog.TombstoneTime(chunkref.ChunkID("Oblob"))
	require.NoError(t, err)
	require.False(t, ok,
		"not caught up: orphan must never even be observed (no t_zero) because the sweep is gated off")
}
