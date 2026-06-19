package snapshot

import (
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage"
)

func testSnapshot(seq uint64) *Snapshot {
	return &Snapshot{
		Seq:         seq,
		Timestamp:   time.Unix(1700000000, 0).UTC(),
		Reason:      "test",
		ObjectCount: 0,
		SizeBytes:   0,
	}
}

func writeLegacyGzipSnapshotFile(t *testing.T, path string, snap *Snapshot) {
	t.Helper()

	f, err := os.Create(path)
	require.NoError(t, err)
	gz := gzip.NewWriter(f)
	require.NoError(t, json.NewEncoder(gz).Encode(snap))
	require.NoError(t, gz.Close())
	require.NoError(t, f.Close())
}

func writeFutureSnapshotFile(t *testing.T, path string, snap *Snapshot, minReader uint32) {
	t.Helper()

	// Write directly as a GFSNAP01 plaintext legacy file (bypassing envelope).
	f, err := os.Create(path)
	require.NoError(t, err)
	_, err = f.Write([]byte("GFSNAP01"))
	require.NoError(t, err)
	require.NoError(t, binary.Write(f, binary.BigEndian, minReader))
	require.NoError(t, binary.Write(f, binary.BigEndian, uint32(999)))
	require.NoError(t, binary.Write(f, binary.BigEndian, time.Now().UnixNano()))
	zw, err := zstd.NewWriter(f, zstd.WithEncoderLevel(zstd.SpeedDefault))
	require.NoError(t, err)
	require.NoError(t, json.NewEncoder(zw).Encode(snap))
	require.NoError(t, zw.Close())
	require.NoError(t, f.Close())
}

func TestWriteSnapshotAddsHeaderAndRoundTrips(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot-00000000000000000001.json.zst")
	snap := testSnapshot(1)
	m := NewTestManager(t, dir, &formatTestBackend{}, "")

	require.NoError(t, m.writeSnapshot(path, snap))

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(raw), 24)
	// After Phase D-snap Slice 2, the file is sealed — starts with GSNE envelope.
	require.True(t, encrypt.IsSnapshotEnvelope(raw), "snapshot file must be sealed with GSNE envelope")

	got, err := m.readSnapshot(path)
	require.NoError(t, err)
	require.Equal(t, snap.Seq, got.Seq)
}

func TestReadLegacyGzipSnapshotIsUnsupported(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot-00000000000000000001.json.gz")
	writeLegacyGzipSnapshotFile(t, path, testSnapshot(1))
	m := NewTestManager(t, dir, &formatTestBackend{}, "")

	_, err := m.readSnapshot(path)
	require.Error(t, err, "gzip snapshots must be rejected")
}

type formatTestBackend struct {
	restoreObjectsCalled bool
	restoreBucketsCalled bool
}

func (b *formatTestBackend) ListAllObjects() ([]storage.SnapshotObject, error) {
	return nil, nil
}

func (b *formatTestBackend) RestoreObjects(objects []storage.SnapshotObject) (int, []storage.StaleBlob, error) {
	b.restoreObjectsCalled = true
	return len(objects), nil, nil
}

func (b *formatTestBackend) ListAllBuckets() ([]storage.SnapshotBucket, error) {
	return nil, nil
}

func (b *formatTestBackend) RestoreBuckets(buckets []storage.SnapshotBucket) error {
	b.restoreBucketsCalled = true
	return nil
}

func TestRestoreRejectsFutureSnapshotFormatBeforeBackendMutation(t *testing.T) {
	dir := t.TempDir()
	backend := &formatTestBackend{}
	mgr := NewTestManager(t, dir, backend, "")
	// Write a legacy plaintext GFSNAP01 file with a future minReader version.
	writeFutureSnapshotFile(t, mgr.path(1), testSnapshot(1), currentSnapshotReaderFormat+1)

	_, _, err := mgr.Restore(1)
	require.ErrorIs(t, err, ErrUnsupportedSnapshotFormat)
	require.False(t, backend.restoreObjectsCalled)
	require.False(t, backend.restoreBucketsCalled)
}

func TestListSkipsUnknownSnapshotEnvelope(t *testing.T) {
	dir := t.TempDir()
	backend := &formatTestBackend{}
	mgr := NewTestManager(t, dir, backend, "")
	require.NoError(t, os.WriteFile(mgr.path(1), []byte("not-a-snapshot"), 0o644))

	snaps, err := mgr.List()
	require.NoError(t, err)
	require.Empty(t, snaps)
}

func TestManagerUsesZstdSuffixAndIgnoresGzipSnapshots(t *testing.T) {
	dir := t.TempDir()
	backend := &formatTestBackend{}
	mgr := NewTestManager(t, dir, backend, "")

	snap, err := mgr.Create("suffix")
	require.NoError(t, err)

	_, err = os.Stat(filepath.Join(dir, "snapshot-1.json.zst"))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(dir, "snapshot-1.json.gz"))
	require.True(t, errors.Is(err, os.ErrNotExist))

	writeLegacyGzipSnapshotFile(t, filepath.Join(dir, "snapshot-99.json.gz"), testSnapshot(99))

	snaps, err := mgr.List()
	require.NoError(t, err)
	require.Len(t, snaps, 1)
	require.Equal(t, snap.Seq, snaps[0].Seq)
}

func TestRestoreLegacyGzipSnapshotIsUnsupported(t *testing.T) {
	dir := t.TempDir()
	backend := &formatTestBackend{}
	mgr := NewTestManager(t, dir, backend, "")
	writeLegacyGzipSnapshotFile(t, filepath.Join(dir, "snapshot-1.json.gz"), testSnapshot(1))

	_, _, err := mgr.Restore(1)
	require.ErrorIs(t, err, ErrUnsupportedSnapshotFormat)
	require.False(t, backend.restoreObjectsCalled)
	require.False(t, backend.restoreBucketsCalled)
}

func TestManagerSeedsNextSeqFromLegacyGzipSnapshots(t *testing.T) {
	dir := t.TempDir()
	writeLegacyGzipSnapshotFile(t, filepath.Join(dir, "snapshot-99.json.gz"), testSnapshot(99))
	backend := &formatTestBackend{}
	mgr := NewTestManager(t, dir, backend, "")

	snap, err := mgr.Create("after-legacy")
	require.NoError(t, err)
	require.Equal(t, uint64(100), snap.Seq)
	_, err = os.Stat(filepath.Join(dir, "snapshot-100.json.zst"))
	require.NoError(t, err)
}

func TestEncodeSnapshotFramed_SoleAuthOn_BumpsMinReader(t *testing.T) {
	normal := &Snapshot{Seq: 1, BucketMeta: []storage.SnapshotBucket{{Name: "b"}}}
	on := &Snapshot{Seq: 2, BucketMeta: []storage.SnapshotBucket{{Name: "b", SoleAuthState: "on"}}}
	epochOnly := &Snapshot{Seq: 3, BucketMeta: []storage.SnapshotBucket{{Name: "b", SoleAuthEpoch: 5}}}

	nb, err := encodeSnapshotFramed(normal)
	require.NoError(t, err)
	require.Equal(t, uint32(1), binary.BigEndian.Uint32(nb[8:12]), "normal snapshot must have minReader=1")

	ob, err := encodeSnapshotFramed(on)
	require.NoError(t, err)
	require.Equal(t, uint32(2), binary.BigEndian.Uint32(ob[8:12]), "soleauth-on snapshot must have minReader=2")

	eb, err := encodeSnapshotFramed(epochOnly)
	require.NoError(t, err)
	require.Equal(t, uint32(2), binary.BigEndian.Uint32(eb[8:12]), "soleauth epoch>0 snapshot must have minReader=2")
}

func TestReadSnapshotRefusesSoleAuthOnMinReader2(t *testing.T) {
	// Frame a snapshot with minReader=2 and verify it's rejected when read by a
	// reader that only supports format 1. We achieve this by constructing a raw
	// GFSNAP01 header with minReader=2 against a reader whose currentSnapshotReaderFormat
	// is still 1 (pre-bump). Since the production code now supports format 2 as
	// well, we directly exercise the refusal path by crafting a header with
	// minReader = currentSnapshotReaderFormat + 1.
	dir := t.TempDir()
	backend := &formatTestBackend{}
	mgr := NewTestManager(t, dir, backend, "")
	// Write a plaintext GFSNAP01 file with minReader = currentSnapshotReaderFormat+1.
	// This simulates a future binary writing a snapshot that an old binary would refuse.
	writeFutureSnapshotFile(t, mgr.path(1), testSnapshot(1), currentSnapshotReaderFormat+1)

	_, _, err := mgr.Restore(1)
	require.ErrorIs(t, err, ErrUnsupportedSnapshotFormat)
	require.False(t, backend.restoreObjectsCalled)
	require.False(t, backend.restoreBucketsCalled)
}
