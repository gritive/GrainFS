package wal_test

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage/wal"
)

// TestWAL_VersionIDRoundTrip writes entries with VersionID and verifies that
// Replay preserves the field through the v2 wire format.
func TestWAL_VersionIDRoundTrip(t *testing.T) {
	dir := t.TempDir()

	w, err := wal.Open(dir)
	require.NoError(t, err)

	w.AppendAsync(wal.Entry{
		Op: wal.OpPut, Bucket: "b", Key: "k", ETag: "e1", Size: 10,
		VersionID: "01J000000000000000000000V1",
	})
	w.AppendAsync(wal.Entry{
		Op: wal.OpPut, Bucket: "b", Key: "k", ETag: "e2", Size: 20,
		VersionID: "01J000000000000000000000V2",
	})
	w.AppendAsync(wal.Entry{
		Op: wal.OpDeleteVersion, Bucket: "b", Key: "k",
		VersionID: "01J000000000000000000000V1",
	})
	require.NoError(t, w.Flush())
	require.NoError(t, w.Close())

	var got []wal.Entry
	n, err := wal.Replay(dir, 0, time.Now().Add(time.Second), func(e wal.Entry) {
		got = append(got, e)
	})
	require.NoError(t, err)
	require.Equal(t, 3, n)
	require.Equal(t, "01J000000000000000000000V1", got[0].VersionID)
	require.Equal(t, "01J000000000000000000000V2", got[1].VersionID)
	require.Equal(t, wal.OpDeleteVersion, got[2].Op)
	require.Equal(t, "01J000000000000000000000V1", got[2].VersionID)
}

// TestWAL_LegacyV1SegmentReadable writes a synthetic v1 segment (no VersionID)
// and verifies Replay accepts it with an empty VersionID.
func TestWAL_LegacyV1SegmentReadable(t *testing.T) {
	dir := t.TempDir()

	// Hand-craft a v1 segment: header (magic + version=1) + one legacy entry.
	path := filepath.Join(dir, "wal-0000000001.bin")
	f, err := os.Create(path)
	require.NoError(t, err)

	// Header: magic(4) + version(4)
	header := make([]byte, 8)
	binary.BigEndian.PutUint32(header[0:4], 0x57414C31) // "WAL1"
	binary.BigEndian.PutUint32(header[4:8], 1)          // v1
	_, err = f.Write(header)
	require.NoError(t, err)

	// Entry (v1): seq=1, ts=now, op=Put, bucket="b", key="k", etag="e",
	// contentType="", size=42 — no VersionID trailer.
	ts := uint64(time.Now().UnixNano())
	bucket := []byte("b")
	key := []byte("k")
	etag := []byte("e")
	ct := []byte("")
	entry := make([]byte, 0, 64)
	entry = append(entry, pack64(1)...)  // seq
	entry = append(entry, pack64(ts)...) // ts
	entry = append(entry, wal.OpPut)     // op
	entry = append(entry, pack16(len(bucket))...)
	entry = append(entry, bucket...)
	entry = append(entry, pack16(len(key))...)
	entry = append(entry, key...)
	entry = append(entry, pack16(len(etag))...)
	entry = append(entry, etag...)
	entry = append(entry, pack16(len(ct))...)
	entry = append(entry, ct...)
	entry = append(entry, pack64(42)...) // size
	_, err = f.Write(entry)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	var got []wal.Entry
	n, err := wal.Replay(dir, 0, time.Now().Add(time.Second), func(e wal.Entry) {
		got = append(got, e)
	})
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, wal.OpPut, got[0].Op)
	require.Equal(t, "b", got[0].Bucket)
	require.Equal(t, "k", got[0].Key)
	require.Equal(t, int64(42), got[0].Size)
	require.Equal(t, "", got[0].VersionID, "v1 entries must decode with empty VersionID")
}

// TestWAL_MixedV1V2Replay verifies that a directory containing one v1 segment
// and one v2 segment replays cleanly in ascending seq order.
func TestWAL_MixedV1V2Replay(t *testing.T) {
	dir := t.TempDir()

	// Write a v1 segment manually with seq=1.
	v1Path := filepath.Join(dir, "wal-0000000001.bin")
	f, err := os.Create(v1Path)
	require.NoError(t, err)
	header := make([]byte, 8)
	binary.BigEndian.PutUint32(header[0:4], 0x57414C31)
	binary.BigEndian.PutUint32(header[4:8], 1)
	_, err = f.Write(header)
	require.NoError(t, err)
	ts := uint64(time.Now().UnixNano())
	entry := make([]byte, 0)
	entry = append(entry, pack64(1)...)
	entry = append(entry, pack64(ts)...)
	entry = append(entry, wal.OpPut)
	entry = append(entry, pack16(1)...)
	entry = append(entry, []byte("b")...)
	entry = append(entry, pack16(1)...)
	entry = append(entry, []byte("k")...)
	entry = append(entry, pack16(0)...) // etag empty
	entry = append(entry, pack16(0)...) // ct empty
	entry = append(entry, pack64(100)...)
	_, err = f.Write(entry)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Open the WAL so it resumes from seq=1 and writes subsequent entries in v2.
	w, err := wal.Open(dir)
	require.NoError(t, err)
	require.Equal(t, uint64(1), w.CurrentSeq())
	w.AppendAsync(wal.Entry{
		Op: wal.OpPut, Bucket: "b", Key: "k", ETag: "etag2", Size: 200,
		VersionID: "01J00000000000000000000002",
	})
	require.NoError(t, w.Flush())
	require.NoError(t, w.Close())

	var got []wal.Entry
	n, err := wal.Replay(dir, 0, time.Now().Add(time.Second), func(e wal.Entry) {
		got = append(got, e)
	})
	require.NoError(t, err)
	require.Equal(t, 2, n)
	// v1 entry: empty VersionID
	require.Equal(t, uint64(1), got[0].Seq)
	require.Equal(t, "", got[0].VersionID)
	require.Equal(t, int64(100), got[0].Size)
	// v2 entry: VersionID populated
	require.Equal(t, uint64(2), got[1].Seq)
	require.Equal(t, "01J00000000000000000000002", got[1].VersionID)
	require.Equal(t, int64(200), got[1].Size)
}

func pack64(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func pack16(v int) []byte {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, uint16(v))
	return b
}
