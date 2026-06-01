package datawal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// writeTestSegment writes a minimal 16-byte header segment file named
// segmentName(firstSeq) into dir. GC operates on filenames only, so a
// header-only file is sufficient. lastSeq is unused by GC (it uses the NEXT
// file's firstSeq to derive the max seq of a sealed segment).
func writeTestSegment(t *testing.T, dir string, firstSeq uint64) {
	t.Helper()
	path := filepath.Join(dir, segmentName(firstSeq))
	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close()
	require.NoError(t, writeHeader(f, fileModePlain, 0))
}

// listSegments returns sorted basenames of all segment files in dir.
func listSegments(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var names []string
	for _, e := range entries {
		if isSegmentName(e.Name()) {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	return names
}

// TestGCMaterializedSegments_DeletesMaterializedKeepsActiveAndUnmaterialized verifies
// that GCMaterializedSegments deletes sealed segments whose max seq ≤ checkpoint
// and keeps the active segment and sealed segments not yet fully materialized.
//
// Layout:
//
//	datawal-0000000001.bin (seqs 1..10, nextFirst=11 → maxSeq=10)
//	datawal-0000000011.bin (seqs 11..20, nextFirst=21 → maxSeq=20)
//	datawal-0000000021.bin (seqs 21..30, nextFirst=31 → maxSeq=30)
//	datawal-0000000031.bin (active — GC never deletes)
//
// checkpoint=20 → segments with maxSeq ≤ 20 (firstSeq 1 and 11) are deletable.
// Segment 21..30 has maxSeq=30 > 20 → keep. Active (31) → never delete.
func TestGCMaterializedSegments_DeletesMaterializedKeepsActiveAndUnmaterialized(t *testing.T) {
	dir := t.TempDir()
	writeTestSegment(t, dir, 1)
	writeTestSegment(t, dir, 11)
	writeTestSegment(t, dir, 21)
	writeTestSegment(t, dir, 31) // active

	deleted, err := GCMaterializedSegments(dir, 20)
	require.NoError(t, err)
	require.Equal(t, 2, deleted)
	remaining := listSegments(t, dir)
	require.Equal(t, []string{"datawal-0000000021.bin", "datawal-0000000031.bin"}, remaining)
}

// TestGCMaterializedSegments_NeverDeletesActiveEvenIfCheckpointAhead ensures the
// active segment (files[last]) is never deleted even when checkpoint is far ahead
// of all segment seqs.
func TestGCMaterializedSegments_NeverDeletesActiveEvenIfCheckpointAhead(t *testing.T) {
	dir := t.TempDir()
	writeTestSegment(t, dir, 1)
	writeTestSegment(t, dir, 11) // active
	// checkpoint far ahead — active must still survive
	deleted, err := GCMaterializedSegments(dir, 1_000_000)
	require.NoError(t, err)
	require.Equal(t, 1, deleted) // only the first sealed segment
	remaining := listSegments(t, dir)
	require.Equal(t, []string{"datawal-0000000011.bin"}, remaining)
}

// TestGCMaterializedSegments_NoCheckpointDeletesNothing verifies that
// checkpoint==0 is a no-op (nothing is materialized).
func TestGCMaterializedSegments_NoCheckpointDeletesNothing(t *testing.T) {
	dir := t.TempDir()
	writeTestSegment(t, dir, 1)
	writeTestSegment(t, dir, 11)
	deleted, err := GCMaterializedSegments(dir, 0)
	require.NoError(t, err)
	require.Equal(t, 0, deleted)
	require.Equal(t, []string{"datawal-0000000001.bin", "datawal-0000000011.bin"}, listSegments(t, dir))
}

// TestGCMaterializedSegments_ContinuesOnError verifies continue-on-error: a
// failure to delete one sealed segment (simulated by placing a non-empty directory
// where a segment file would be — os.Remove fails with ENOTDIR/EISDIR) does not
// prevent deletion of subsequent qualifying segments.
//
// We place an undeletable entry (a non-empty directory named like a segment) as
// the MIDDLE sealed segment, with normal-file segments on either side.
func TestGCMaterializedSegments_ContinuesOnError(t *testing.T) {
	dir := t.TempDir()

	// Segment 1..10: regular file, qualifies (maxSeq=10 ≤ 20).
	writeTestSegment(t, dir, 1)

	// Segment 11..20: undeletable directory (os.Remove fails on non-empty dirs).
	// maxSeq=20 ≤ 20 → qualifies for GC, but the delete will fail.
	dirSegPath := filepath.Join(dir, segmentName(11))
	require.NoError(t, os.Mkdir(dirSegPath, 0o755))
	// Put a file inside so it's non-empty and os.Remove returns ENOTEMPTY.
	require.NoError(t, os.WriteFile(filepath.Join(dirSegPath, "x"), []byte("x"), 0o644))

	// Segment 21..30: regular file, qualifies (maxSeq=30 ≤ 40).
	writeTestSegment(t, dir, 21)

	// Segment 31: active — never deleted.
	writeTestSegment(t, dir, 31)

	deleted, err := GCMaterializedSegments(dir, 40)
	// Should return error (from the directory), but still have deleted 2 files.
	require.Error(t, err)
	require.Equal(t, 2, deleted)

	// datawal-0000000011 dir and active (31) survive; 1 and 21 were deleted.
	remaining := listSegments(t, dir)
	require.Equal(t, []string{"datawal-0000000011.bin", "datawal-0000000031.bin"}, remaining)
}

// TestGCMaterializedSegments_OnlyOneSegment verifies that a WAL with a single
// segment (= active, always) is a no-op regardless of checkpoint.
func TestGCMaterializedSegments_OnlyOneSegment(t *testing.T) {
	dir := t.TempDir()
	writeTestSegment(t, dir, 1)
	deleted, err := GCMaterializedSegments(dir, 1_000_000)
	require.NoError(t, err)
	require.Equal(t, 0, deleted)
}

// TestGCMaterializedSegments_EmptyDir verifies that an empty dir returns (0, nil).
func TestGCMaterializedSegments_EmptyDir(t *testing.T) {
	dir := t.TempDir()
	deleted, err := GCMaterializedSegments(dir, 100)
	require.NoError(t, err)
	require.Equal(t, 0, deleted)
}

// TestRecover_GCsMaterializedSegmentsAndStillReplays is the integration test:
// build a WAL with multiple sealed segments + records, run Recover with a
// materializer that succeeds, assert:
//  1. Materialized (low) segments are GC'd.
//  2. The active segment survives.
//  3. A subsequent Recover replays the remaining records without error (data-loss check).
func TestRecover_GCsMaterializedSegmentsAndStillReplays(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Build layout: 2 sealed segments + 1 active, contiguous naming.
	//
	// writePlainSegmentForTest (from wal_internal_test.go) creates a valid segment
	// file with the given records. We place sealed segment files first, then create
	// a header-only "active" segment (firstSeq=5) so Open picks [1,3,5] and appends
	// into file 5. Records in files 1 and 3 are pre-assigned contiguous seqs.
	writePlainSegmentForTest(t, dir, 1, []Record{
		{Seq: 1, Timestamp: 1, Op: OpSegmentPut, Key: "a"},
		{Seq: 2, Timestamp: 2, Op: OpSegmentPut, Key: "b"},
	}, nil)
	writePlainSegmentForTest(t, dir, 3, []Record{
		{Seq: 3, Timestamp: 3, Op: OpSegmentPut, Key: "c"},
		{Seq: 4, Timestamp: 4, Op: OpSegmentPut, Key: "d"},
	}, nil)
	// Create an empty (header-only) segment 5 so Open picks it as the active.
	writePlainSegmentForTest(t, dir, 5, nil, nil)

	// Open the WAL; it scans segs 1+3 (lastSeq=4), then opens seg 5 as the active.
	w, err := Open(dir, nil, "datawal")
	require.NoError(t, err)
	// Append 2 records into the active segment (seq 5 and 6 assigned by WAL).
	_, err = w.Append(ctx, Record{Op: OpSegmentPut, Key: "e"})
	require.NoError(t, err)
	_, err = w.Append(ctx, Record{Op: OpSegmentPut, Key: "f"})
	require.NoError(t, err)
	require.NoError(t, w.Flush())
	require.NoError(t, w.Close())

	// Verify 3 segment files exist before Recover.
	segs := listSegments(t, dir)
	require.Len(t, segs, 3, "expected 3 segments before Recover")

	// First Recover: materializer records all applied keys.
	m := &localMaterializer{}
	require.NoError(t, Recover(ctx, dir, 0, nil, "datawal", m))

	// All 6 records (a..f) must have been materialized.
	require.Len(t, m.keys, 6, "expected all 6 records materialized on first Recover")

	// GC should have deleted the 2 sealed segments (1 and 3); active (5) survives.
	remaining := listSegments(t, dir)
	require.Len(t, remaining, 1, "expected only the active segment after GC")
	require.Contains(t, remaining[0], "0000000005", "surviving segment should be the active (firstSeq=5)")

	// Second Recover: proves replay-after-GC is safe (data-loss check).
	// checkpoint == lastSeq=6, so no new records to replay.
	m2 := &localMaterializer{}
	require.NoError(t, Recover(ctx, dir, 0, nil, "datawal", m2))
	require.Empty(t, m2.keys, "second Recover should replay nothing (all already checkpointed)")
}

// localMaterializer is a simple local materializer for gc_test.go tests.
// The external-package recordingMaterializer is not accessible here (package datawal).
type localMaterializer struct {
	keys []string
}

func (m *localMaterializer) Materialize(_ context.Context, rec Record) error {
	m.keys = append(m.keys, rec.Key)
	return nil
}

func (m *localMaterializer) HasReplacement(_ context.Context, _ Record) (bool, error) {
	return false, nil
}

// segmentFirstSeqFromPath_exported is a convenience wrapper for tests that
// exercise the helper indirectly. The actual helper is tested via GCMaterializedSegments.
func TestSegmentFirstSeqFromPath(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		wantSeq uint64
		wantOK  bool
	}{
		{"normal", "/some/dir/datawal-0000000001.bin", 1, true},
		{"large", "/dir/datawal-0000012345.bin", 12345, true},
		{"max10digit", "/dir/datawal-9999999999.bin", 9999999999, true},
		{"not-segment", "/dir/checkpoint", 0, false},
		{"junk", "/dir/datawal-junk0000.bin", 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			seq, ok := segmentFirstSeqFromPath(tc.path)
			require.Equal(t, tc.wantOK, ok)
			if ok {
				require.Equal(t, tc.wantSeq, seq)
			}
		})
	}
}

// Compile-time check: GCMaterializedSegments must be accessible (i.e., exported).
var _ = GCMaterializedSegments

// Compile-time check: errors.Join usage compile check.
var _ = errors.Join
