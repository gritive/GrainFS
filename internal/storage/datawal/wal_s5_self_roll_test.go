package datawal_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage/datawal"
)

// S5: Append self-rolls across a DEK rotation WITHOUT a manual
// RollSegmentOnRotation call. A rotation advances the active gen the sealer seals
// through; the next append seals under the new gen, detects the old-gen-pinned
// header, rolls the segment, and retries — transparently. (The S2 tests above
// exercise RollSegmentOnRotation directly; these prove Append wires it in.)
func TestWAL_AppendSelfRollsAfterRotation(t *testing.T) {
	keeper, sealer := testKeeperAndSealer(t)
	dir := t.TempDir()
	w, err := datawal.Open(dir, sealer, "datawal")
	require.NoError(t, err)

	seq1, err := w.Append(context.Background(), datawal.Record{Op: datawal.OpSegmentPut, Bucket: "b", Key: "k1", Payload: []byte("under-gen-0")})
	require.NoError(t, err)

	require.NoError(t, keeper.Rotate()) // active gen 0 -> 1; NO manual roll

	// Append must succeed by self-rolling — no RollSegmentOnRotation call here.
	seq2, err := w.Append(context.Background(), datawal.Record{Op: datawal.OpSegmentPut, Bucket: "b", Key: "k2", Payload: []byte("under-gen-1")})
	require.NoError(t, err, "append after rotation must self-roll, not fail")
	require.Greater(t, seq2, seq1)
	require.NoError(t, w.Close())

	files := segmentPaths(t, dir)
	require.Len(t, files, 2, "self-roll on a non-empty segment creates a second segment")
	require.Equal(t, uint32(0), segmentHeaderGen(t, files[0]), "prior segment pinned at gen 0")
	require.Equal(t, uint32(1), segmentHeaderGen(t, files[1]), "self-rolled segment pinned at gen 1")

	seqs, recs := replaySeqs(t, dir, sealer)
	require.Equal(t, []uint64{seq1, seq2}, seqs)
	require.Equal(t, []byte("under-gen-0"), recs[0].Payload)
	require.Equal(t, []byte("under-gen-1"), recs[1].Payload)
}

// Anti-regression: with no rotation, Append takes the first-attempt path and
// never rolls (the zero-overhead common case). A single segment, no extra files.
func TestWAL_NoSelfRollWithoutRotation(t *testing.T) {
	_, sealer := testKeeperAndSealer(t)
	dir := t.TempDir()
	w, err := datawal.Open(dir, sealer, "datawal")
	require.NoError(t, err)
	for i := 0; i < 5; i++ {
		_, err := w.Append(context.Background(), datawal.Record{Op: datawal.OpSegmentPut, Bucket: "b", Key: fmt.Sprintf("k%d", i), Payload: []byte("steady")})
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())

	files := segmentPaths(t, dir)
	require.Len(t, files, 1, "no rotation must not roll the segment")
	require.Equal(t, uint32(0), segmentHeaderGen(t, files[0]))
}

// BLOCKER guard (plan gate): concurrent appenders racing rotations + group-commit
// Flush must never produce a duplicate or non-monotonic seq. The earlier inline
// park design corrupted the WAL exactly here (a parked appender held a claimed
// seq across a lock release while another claimed the same seq). Replay is the
// oracle: a logic-invariant break that -race cannot see surfaces as a dup seq.
func TestWAL_ConcurrentAppendersAcrossRotation(t *testing.T) {
	keeper, sealer := testKeeperAndSealer(t)
	dir := t.TempDir()
	w, err := datawal.Open(dir, sealer, "datawal")
	require.NoError(t, err)

	const appenders = 8
	const perAppender = 25
	var wg sync.WaitGroup
	errs := make(chan error, appenders*perAppender)

	for g := 0; g < appenders; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perAppender; i++ {
				_, err := w.Append(context.Background(), datawal.Record{
					Op:      datawal.OpSegmentPut,
					Bucket:  "b",
					Key:     fmt.Sprintf("g%d-%d", g, i),
					Payload: []byte("concurrent"),
				})
				if err != nil {
					errs <- err
					return
				}
			}
		}(g)
	}

	// Concurrent group-commit Flush + a few spaced rotations while appends run.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 60; i++ {
			_ = w.Flush()
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 4; i++ {
			// Space rotations so no single append's bounded retry spans more
			// than one (maxRotationRollRetries headroom).
			for j := 0; j < 12; j++ {
				_ = w.Flush()
			}
			// Don't use require.* off the test goroutine (t.FailNow → Goexit is
			// undefined there); route the error through the same channel.
			if err := keeper.Rotate(); err != nil {
				errs <- err
				return
			}
		}
	}()

	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err, "no append may fail across concurrent rotations")
	}
	require.NoError(t, w.Close())

	seqs, _ := replaySeqs(t, dir, sealer)
	require.Len(t, seqs, appenders*perAppender, "every appended record must replay exactly once")
	seen := make(map[uint64]bool, len(seqs))
	for i, s := range seqs {
		require.Falsef(t, seen[s], "duplicate seq %d at replay index %d (WAL corruption)", s, i)
		seen[s] = true
		if i > 0 {
			require.Greaterf(t, s, seqs[i-1], "seqs must be strictly monotonic at replay index %d", i)
		}
	}
}
