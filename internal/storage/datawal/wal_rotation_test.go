package datawal_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/storage/datawal"
)

// Segment header layout (datawal/wal.go writeHeader): magic[0:4] version[4:8]
// mode[8] reserved[9:12] dek_gen[12:16]. Mirrored here so the external test can
// read the pinned generation off disk without exporting internals.
const (
	testHeaderBytes = 16
	testGenOffset   = 12
)

// testKeeperAndSealer returns a fresh DEK keeper at active gen 0 together with a
// RecordSealer that seals through it. The existing testDEKKeeperAdapterAtGen
// captures the keeper inside the adapter and never returns it, so a test cannot
// advance the gen on the keeper the WAL actually seals through. Production
// advances the gen via InstallReplicatedDEK in the FSM apply; the unit test uses
// keeper.Rotate(), which bumps the same active gen the adapter seals through.
func testKeeperAndSealer(t *testing.T) (*encrypt.DEKKeeper, datawal.RecordSealer) {
	t.Helper()
	kek := bytes.Repeat([]byte{0x51}, encrypt.KEKSize)
	clusterID := bytes.Repeat([]byte{0xC1}, 16)
	keeper, err := encrypt.NewDEKKeeper(kek, clusterID)
	require.NoError(t, err)
	return keeper, storage.NewDEKKeeperAdapter(keeper, clusterID)
}

func segmentPaths(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var files []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "datawal-") && strings.HasSuffix(e.Name(), ".bin") {
			files = append(files, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(files)
	return files
}

func segmentHeaderGen(t *testing.T, path string) uint32 {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(data), testHeaderBytes)
	return binary.BigEndian.Uint32(data[testGenOffset:testHeaderBytes])
}

func replaySeqs(t *testing.T, dir string, sealer datawal.RecordSealer) ([]uint64, []datawal.Record) {
	t.Helper()
	var seqs []uint64
	var recs []datawal.Record
	require.NoError(t, datawal.Replay(context.Background(), dir, 0, sealer, "datawal", func(rec datawal.Record) error {
		seqs = append(seqs, rec.Seq)
		recs = append(recs, rec)
		return nil
	}))
	return seqs, recs
}

// Non-empty segment: rotation rolls to a NEW segment file pinned at the new gen;
// the prior segment stays pinned at the old gen and both records replay in order.
func TestRollSegmentOnRotationNonEmpty(t *testing.T) {
	keeper, sealer := testKeeperAndSealer(t)
	dir := t.TempDir()
	w, err := datawal.Open(dir, sealer, "datawal")
	require.NoError(t, err)

	seq1, err := w.Append(context.Background(), datawal.Record{Op: datawal.OpSegmentPut, Bucket: "b", Key: "k1", Payload: []byte("under-gen-0")})
	require.NoError(t, err)

	require.NoError(t, keeper.Rotate()) // active gen 0 -> 1
	require.NoError(t, w.RollSegmentOnRotation())

	seq2, err := w.Append(context.Background(), datawal.Record{Op: datawal.OpSegmentPut, Bucket: "b", Key: "k2", Payload: []byte("under-gen-1")})
	require.NoError(t, err)
	require.Greater(t, seq2, seq1)
	require.NoError(t, w.Close())

	files := segmentPaths(t, dir)
	require.Len(t, files, 2, "non-empty roll creates a second segment")
	require.Equal(t, uint32(0), segmentHeaderGen(t, files[0]), "prior segment pinned at gen 0")
	require.Equal(t, uint32(1), segmentHeaderGen(t, files[1]), "rolled segment pinned at gen 1")
	// New segment is named for the next firstSeq (seq2), zero-padded so it sorts last.
	require.Equal(t, filepath.Join(dir, fmt.Sprintf("datawal-%010d.bin", seq2)), files[1])

	seqs, recs := replaySeqs(t, dir, sealer)
	require.Equal(t, []uint64{seq1, seq2}, seqs)
	require.Equal(t, []byte("under-gen-0"), recs[0].Payload)
	require.Equal(t, []byte("under-gen-1"), recs[1].Payload)
}

// Empty (header-only) segment: rotation re-inits the segment in place under the
// new gen — no new file, and exactly one header (no double-header corruption).
func TestRollSegmentOnRotationEmpty(t *testing.T) {
	keeper, sealer := testKeeperAndSealer(t)
	dir := t.TempDir()
	w, err := datawal.Open(dir, sealer, "datawal")
	require.NoError(t, err)
	// No append: the active segment is header-only.

	require.NoError(t, keeper.Rotate())
	require.NoError(t, w.RollSegmentOnRotation())

	_, err = w.Append(context.Background(), datawal.Record{Op: datawal.OpSegmentPut, Bucket: "b", Key: "k", Payload: []byte("after-empty-roll")})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	files := segmentPaths(t, dir)
	require.Len(t, files, 1, "empty roll re-inits in place, no new file")
	require.Equal(t, uint32(1), segmentHeaderGen(t, files[0]), "re-init'd segment pinned at new gen")

	seqs, recs := replaySeqs(t, dir, sealer)
	require.Len(t, seqs, 1, "exactly one record (no double-header corruption)")
	require.Equal(t, []byte("after-empty-roll"), recs[0].Payload)
}

// No rotation since the segment was pinned: RollSegmentOnRotation is a no-op.
func TestRollSegmentOnRotationNoOp(t *testing.T) {
	_, sealer := testKeeperAndSealer(t)
	dir := t.TempDir()
	w, err := datawal.Open(dir, sealer, "datawal")
	require.NoError(t, err)
	_, err = w.Append(context.Background(), datawal.Record{Op: datawal.OpSegmentPut, Bucket: "b", Key: "k1", Payload: []byte("a")})
	require.NoError(t, err)

	filesBefore := segmentPaths(t, dir)
	require.Len(t, filesBefore, 1)

	require.NoError(t, w.RollSegmentOnRotation()) // probed gen == pinned gen

	_, err = w.Append(context.Background(), datawal.Record{Op: datawal.OpSegmentPut, Bucket: "b", Key: "k2", Payload: []byte("b")})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	filesAfter := segmentPaths(t, dir)
	require.Equal(t, filesBefore, filesAfter, "no-op roll must not create a new segment")
	require.Equal(t, uint32(0), segmentHeaderGen(t, filesAfter[0]))
}

// Replay across a rolled boundary with MULTIPLE records on each side: the roll
// must preserve w.lastSeq so seqs stay contiguous and monotonic (seqMonotonic
// enforces this on replay). A 1+1 record test would pass even if roll reset seq.
func TestRollSegmentReplayAcrossBoundary(t *testing.T) {
	keeper, sealer := testKeeperAndSealer(t)
	dir := t.TempDir()
	w, err := datawal.Open(dir, sealer, "datawal")
	require.NoError(t, err)

	var want []uint64
	for i := 0; i < 3; i++ {
		seq, err := w.Append(context.Background(), datawal.Record{Op: datawal.OpSegmentPut, Bucket: "b", Key: fmt.Sprintf("g0-%d", i), Payload: []byte("gen0")})
		require.NoError(t, err)
		want = append(want, seq)
	}

	require.NoError(t, keeper.Rotate())
	require.NoError(t, w.RollSegmentOnRotation())

	for i := 0; i < 3; i++ {
		seq, err := w.Append(context.Background(), datawal.Record{Op: datawal.OpSegmentPut, Bucket: "b", Key: fmt.Sprintf("g1-%d", i), Payload: []byte("gen1")})
		require.NoError(t, err)
		want = append(want, seq)
	}
	require.NoError(t, w.Close())

	got, _ := replaySeqs(t, dir, sealer)
	require.Equal(t, want, got, "all records replay in monotonic seq order across the rolled boundary")
	for i := 1; i < len(got); i++ {
		require.Equal(t, got[i-1]+1, got[i], "seq must be contiguous across the roll (lastSeq preserved)")
	}
}

// S5: appending under a rotated keeper WITHOUT a manual RollSegmentOnRotation
// now succeeds — Append self-rolls (the gen-match assertion turned into a
// roll-then-retry). After the self-roll the segment is pinned at the new gen, so
// a subsequent manual RollSegmentOnRotation is a redundant no-op. (Pre-S5 this
// append failed closed; the assertion is now self-healing.)
func TestAppendAfterRotationSelfRolls(t *testing.T) {
	keeper, sealer := testKeeperAndSealer(t)
	dir := t.TempDir()
	w, err := datawal.Open(dir, sealer, "datawal")
	require.NoError(t, err)
	_, err = w.Append(context.Background(), datawal.Record{Op: datawal.OpSegmentPut, Bucket: "b", Key: "k1", Payload: []byte("gen0")})
	require.NoError(t, err)

	require.NoError(t, keeper.Rotate()) // advance active gen WITHOUT a manual roll

	_, err = w.Append(context.Background(), datawal.Record{Op: datawal.OpSegmentPut, Bucket: "b", Key: "k2", Payload: []byte("gen1")})
	require.NoError(t, err, "append under a rotated keeper must self-roll and succeed")

	filesAfterSelfRoll := segmentPaths(t, dir)
	require.NoError(t, w.RollSegmentOnRotation()) // redundant now: gen already matches
	_, err = w.Append(context.Background(), datawal.Record{Op: datawal.OpSegmentPut, Bucket: "b", Key: "k3", Payload: []byte("gen1")})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	require.Equal(t, filesAfterSelfRoll, segmentPaths(t, dir),
		"the redundant manual roll after a self-roll must be a no-op (no new segment)")
}
