package wal_test

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage/wal"
)

func walSegments(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var files []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "wal-") && strings.HasSuffix(e.Name(), ".bin") {
			files = append(files, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(files)
	return files
}

// segHeaderGenV4 reads the dek_gen from a v4 segment header (bytes [8:12]).
func segHeaderGenV4(t *testing.T, path string) uint32 {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(b), 12)
	return binary.BigEndian.Uint32(b[8:12])
}

func replayAllSeqs(t *testing.T, dir string, sealer wal.RecordSealer) []uint64 {
	t.Helper()
	var seqs []uint64
	_, err := wal.ReplayEncrypted(dir, 0, time.Now().Add(time.Hour), sealer, "pitr-wal", func(e wal.Entry) {
		seqs = append(seqs, e.Seq)
	})
	require.NoError(t, err)
	return seqs
}

func requireContiguous(t *testing.T, seqs []uint64) {
	t.Helper()
	for i := 1; i < len(seqs); i++ {
		require.Equal(t, seqs[i-1]+1, seqs[i], "seqs must be contiguous (a gap = a dropped write)")
	}
}

// Core: a DEK rotation mid-session must not drop any entry. Gap-free replay under
// the rotated keeper is the no-drop proof (a dropped write leaves a seq gap; a
// header-gen≠sealed-gen segment would AEAD-fail at replay). Flush does not drain
// the channel, so which gen each entry seals under here is scheduling-dependent;
// the DETERMINISTIC per-segment gen pinning is asserted in
// TestWAL_RotationBoundaryAcrossRestart below.
func TestWAL_SealFirstNoDropAcrossRotation(t *testing.T) {
	dir := t.TempDir()
	keeper := newKeeper(t, 0x42)

	w, err := wal.OpenEncrypted(dir, newSealer(keeper), "pitr-wal")
	require.NoError(t, err)
	for i := 0; i < 4; i++ {
		w.AppendAsync(wal.Entry{Op: wal.OpPut, Bucket: "b", Key: fmt.Sprintf("g0-%d", i)})
	}
	require.NoError(t, w.Flush())

	require.NoError(t, keeper.Rotate()) // active gen 0 -> 1

	for i := 0; i < 4; i++ {
		w.AppendAsync(wal.Entry{Op: wal.OpPut, Bucket: "b", Key: fmt.Sprintf("g1-%d", i)})
	}
	require.NoError(t, w.Close()) // drains all queued entries

	seqs := replayAllSeqs(t, dir, newSealer(keeper))
	require.Len(t, seqs, 8, "all 8 entries replay (none dropped across the rotation)")
	requireContiguous(t, seqs)
}

// Normal appends (no rotation) must NOT roll per-entry — the gen-change clause
// fires only on an actual rotation.
func TestWAL_NoSpuriousRollWithoutRotation(t *testing.T) {
	dir := t.TempDir()
	keeper := newKeeper(t, 0x42)

	w, err := wal.OpenEncrypted(dir, newSealer(keeper), "pitr-wal")
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		w.AppendAsync(wal.Entry{Op: wal.OpPut, Bucket: "b", Key: fmt.Sprintf("k-%d", i)})
	}
	require.NoError(t, w.Close())

	require.Len(t, walSegments(t, dir), 1, "no rotation → exactly one segment")
	require.Len(t, replayAllSeqs(t, dir, newSealer(keeper)), 10)
}

// Deterministic gen-per-segment + restart across a rotated boundary: batch under
// G in one session, then rotate and write batch under G+1 in a second session.
// Both segments replay gap-free, each pinned at its own gen, and the post-rotation
// batch lands in a single segment (only the first entry rolls).
func TestWAL_RotationBoundaryAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	keeper := newKeeper(t, 0x42)

	w, err := wal.OpenEncrypted(dir, newSealer(keeper), "pitr-wal")
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		w.AppendAsync(wal.Entry{Op: wal.OpPut, Bucket: "b", Key: fmt.Sprintf("g0-%d", i)})
	}
	require.NoError(t, w.Close())
	segsAfterG0 := walSegments(t, dir)
	require.Len(t, segsAfterG0, 1)
	require.Equal(t, uint32(0), segHeaderGenV4(t, segsAfterG0[0]))

	require.NoError(t, keeper.Rotate()) // 0 -> 1

	w2, err := wal.OpenEncrypted(dir, newSealer(keeper), "pitr-wal")
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		w2.AppendAsync(wal.Entry{Op: wal.OpPut, Bucket: "b", Key: fmt.Sprintf("g1-%d", i)})
	}
	require.NoError(t, w2.Close())

	files := walSegments(t, dir)
	require.Len(t, files, 2, "one segment per generation")
	require.Equal(t, uint32(0), segHeaderGenV4(t, files[0]), "pre-rotation segment pinned at gen 0")
	require.Equal(t, uint32(1), segHeaderGenV4(t, files[1]), "post-rotation segment pinned at gen 1")

	seqs := replayAllSeqs(t, dir, newSealer(keeper))
	require.Len(t, seqs, 6, "all entries replay gap-free across the rotated boundary")
	requireContiguous(t, seqs)
}

// Plaintext WAL is unaffected by the seal-first restructure (gen 0, 8-byte
// header, size/count rotation only).
func TestWAL_PlaintextUnaffected(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(dir)
	require.NoError(t, err)
	for i := 0; i < 5; i++ {
		w.AppendAsync(wal.Entry{Op: wal.OpPut, Bucket: "b", Key: fmt.Sprintf("k-%d", i)})
	}
	require.NoError(t, w.Close())

	require.Len(t, walSegments(t, dir), 1)
	var seqs []uint64
	n, err := wal.Replay(dir, 0, time.Now().Add(time.Hour), func(e wal.Entry) {
		seqs = append(seqs, e.Seq)
	})
	require.NoError(t, err)
	require.Equal(t, 5, n)
	requireContiguous(t, seqs)
}
