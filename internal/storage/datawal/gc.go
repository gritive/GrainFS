package datawal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

// GCMaterializedSegments deletes sealed datawal segments whose records are all
// materialized (max seq ≤ checkpoint). It NEVER deletes the active (last)
// segment.
//
// A sealed segment's exact max seq is nextSegmentFirstSeq-1. This is exact
// because the ONLY runtime segment roll is RollSegmentOnRotation (gen-driven),
// which names the new segment segmentName(lastSeq+1) — segments are contiguous.
// Header-only active segments re-init in place and never become a distinct
// zero-record sealed segment. So nextFirstSeq-1 is never an underestimate.
//
// Safety: the WAL is opened for append before Recover runs (boot ordering at
// boot_phases_storage_runtime.go). GC is safe because boot is single-threaded
// sequential AND GC excludes files[last] — the exact file the open appender
// holds — so deleting sealed segments never touches the appender's fd.
//
// Continue-on-error: a failure to delete one segment is logged and skipped;
// remaining qualifying segments are still deleted. The aggregate error (if any)
// is returned alongside the deleted count so the caller can log non-fatally.
//
// Returns (deleted, error). An error is non-fatal to the caller — recovery has
// already succeeded; GC is pure cleanup.
func GCMaterializedSegments(dir string, checkpoint uint64) (int, error) {
	if checkpoint == 0 {
		return 0, nil // nothing materialized
	}
	files, err := segmentFiles(dir) // sorted ascending by first-seq
	if err != nil {
		return 0, err
	}
	if len(files) <= 1 {
		return 0, nil // only the active segment (or none) — nothing to GC
	}

	var errs []error
	deleted := 0
	// Iterate sealed segments [0 .. len-2]; files[last] is the active segment.
	for i := 0; i < len(files)-1; i++ {
		nextFirst, ok := segmentFirstSeqFromPath(files[i+1])
		if !ok {
			// Unparseable next name — be conservative, skip.
			continue
		}
		// This segment's exact max seq = nextFirst-1 (contiguous lastSeq+1 naming).
		if nextFirst-1 <= checkpoint {
			if rerr := os.Remove(files[i]); rerr != nil && !os.IsNotExist(rerr) {
				// Continue-on-error: log per-file and keep deleting the rest.
				// A transient failure on one file must not strand the others.
				errs = append(errs, fmt.Errorf("datawal gc: remove %s: %w", filepath.Base(files[i]), rerr))
				continue
			}
			deleted++
		}
		// else: not fully materialized — keep (and all following will also fail
		// the ≤checkpoint test since seqs only increase, but we continue scanning
		// in case of non-contiguous edge cases from unparseable names).
	}
	return deleted, errors.Join(errs...)
}

// segmentFirstSeqFromPath parses the first-sequence number from a segment file
// path. The basename is expected to match isSegmentName (e.g.
// "datawal-0000000001.bin"); the 10-digit decimal at position [8:18] is the
// first seq. Returns (0, false) for any non-conforming name.
//
// NOTE: isSegmentName caps at exactly 10 digits, so seq ≥ 10^10 (ten billion)
// would produce an 11-digit filename that fails isSegmentName — the segment
// would be invisible to GC and sort order would diverge. This is a pre-existing
// WAL limitation; see TODOS.md.
func segmentFirstSeqFromPath(path string) (uint64, bool) {
	name := filepath.Base(path)
	if !isSegmentName(name) {
		return 0, false
	}
	// Name format: "datawal-XXXXXXXXXX.bin"; digits at [8:18].
	seq, err := strconv.ParseUint(name[8:18], 10, 64)
	if err != nil {
		return 0, false
	}
	return seq, true
}
