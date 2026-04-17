package wal_test

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage/wal"
)

func TestWAL_AppendAndReplay(t *testing.T) {
	dir := t.TempDir()

	w, err := wal.Open(dir)
	require.NoError(t, err)

	t0 := time.Now()
	w.AppendAsync(wal.Entry{Op: wal.OpPut, Bucket: "b", Key: "k1", ETag: "e1", Size: 100})
	w.AppendAsync(wal.Entry{Op: wal.OpPut, Bucket: "b", Key: "k2", ETag: "e2", Size: 200})
	w.AppendAsync(wal.Entry{Op: wal.OpDelete, Bucket: "b", Key: "k1"})

	require.NoError(t, w.Flush())
	require.NoError(t, w.Close())

	// Replay all entries (fromSeq=0, targetTime=now+1s)
	var entries []wal.Entry
	n, err := wal.Replay(dir, 0, t0.Add(time.Second), func(e wal.Entry) {
		entries = append(entries, e)
	})
	require.NoError(t, err)
	require.Equal(t, 3, n)
	require.Len(t, entries, 3)
	require.Equal(t, wal.OpPut, entries[0].Op)
	require.Equal(t, "k1", entries[0].Key)
	require.Equal(t, wal.OpPut, entries[1].Op)
	require.Equal(t, "k2", entries[1].Key)
	require.Equal(t, wal.OpDelete, entries[2].Op)
	require.Equal(t, "k1", entries[2].Key)
}

func TestWAL_FilterBySeq(t *testing.T) {
	dir := t.TempDir()

	w, err := wal.Open(dir)
	require.NoError(t, err)

	w.AppendAsync(wal.Entry{Op: wal.OpPut, Bucket: "b", Key: "k1"}) // seq=1
	w.AppendAsync(wal.Entry{Op: wal.OpPut, Bucket: "b", Key: "k2"}) // seq=2
	w.AppendAsync(wal.Entry{Op: wal.OpPut, Bucket: "b", Key: "k3"}) // seq=3

	require.NoError(t, w.Flush())
	fromSeq := w.CurrentSeq() // = 3
	require.NoError(t, w.Close())

	// Replay from seq>3 → no entries
	var entries []wal.Entry
	n, err := wal.Replay(dir, fromSeq, time.Now().Add(time.Second), func(e wal.Entry) {
		entries = append(entries, e)
	})
	require.NoError(t, err)
	require.Equal(t, 0, n)
	require.Empty(t, entries)
}

func TestWAL_FilterByTimestamp(t *testing.T) {
	dir := t.TempDir()

	w, err := wal.Open(dir)
	require.NoError(t, err)

	// Append entries: first batch before pivot, second batch after
	w.AppendAsync(wal.Entry{Op: wal.OpPut, Bucket: "b", Key: "early1"})
	w.AppendAsync(wal.Entry{Op: wal.OpPut, Bucket: "b", Key: "early2"})
	require.NoError(t, w.Flush())

	pivot := time.Now()
	time.Sleep(50 * time.Millisecond)

	w.AppendAsync(wal.Entry{Op: wal.OpPut, Bucket: "b", Key: "late1"})
	w.AppendAsync(wal.Entry{Op: wal.OpPut, Bucket: "b", Key: "late2"})
	require.NoError(t, w.Flush())
	require.NoError(t, w.Close())

	var entries []wal.Entry
	n, err := wal.Replay(dir, 0, pivot, func(e wal.Entry) {
		entries = append(entries, e)
	})
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Len(t, entries, 2)
	require.Equal(t, "early1", entries[0].Key)
	require.Equal(t, "early2", entries[1].Key)
}

func TestWAL_PersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()

	w, err := wal.Open(dir)
	require.NoError(t, err)
	w.AppendAsync(wal.Entry{Op: wal.OpPut, Bucket: "b", Key: "k1", ETag: "e1", Size: 42})
	w.AppendAsync(wal.Entry{Op: wal.OpDelete, Bucket: "b", Key: "k1"})
	require.NoError(t, w.Flush())
	firstSeq := w.CurrentSeq()
	require.NoError(t, w.Close())

	// Reopen: should continue from firstSeq
	w2, err := wal.Open(dir)
	require.NoError(t, err)
	require.Equal(t, firstSeq, w2.CurrentSeq())

	w2.AppendAsync(wal.Entry{Op: wal.OpPut, Bucket: "b", Key: "k2"})
	require.NoError(t, w2.Flush())
	require.Equal(t, firstSeq+1, w2.CurrentSeq())
	require.NoError(t, w2.Close())

	// Replay all 3 entries
	var keys []string
	n, err := wal.Replay(dir, 0, time.Now().Add(time.Second), func(e wal.Entry) {
		keys = append(keys, e.Key)
	})
	require.NoError(t, err)
	require.Equal(t, 3, n)
	require.Equal(t, []string{"k1", "k1", "k2"}, keys)
}

func TestWAL_EmptyDir(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(dir)
	require.NoError(t, err)
	require.Equal(t, uint64(0), w.CurrentSeq())
	require.NoError(t, w.Close())

	// Replay empty WAL → no entries
	n, err := wal.Replay(dir, 0, time.Now().Add(time.Second), func(e wal.Entry) {})
	require.NoError(t, err)
	require.Equal(t, 0, n)
}

func TestWAL_EntryFields(t *testing.T) {
	dir := t.TempDir()

	w, err := wal.Open(dir)
	require.NoError(t, err)

	w.AppendAsync(wal.Entry{
		Op:          wal.OpPut,
		Bucket:      "my-bucket",
		Key:         "path/to/file.txt",
		ETag:        "abc123def456",
		ContentType: "text/plain; charset=utf-8",
		Size:        999,
	})
	require.NoError(t, w.Flush())
	require.NoError(t, w.Close())

	var got wal.Entry
	_, err = wal.Replay(dir, 0, time.Now().Add(time.Second), func(e wal.Entry) {
		got = e
	})
	require.NoError(t, err)
	require.Equal(t, wal.OpPut, got.Op)
	require.Equal(t, "my-bucket", got.Bucket)
	require.Equal(t, "path/to/file.txt", got.Key)
	require.Equal(t, "abc123def456", got.ETag)
	require.Equal(t, "text/plain; charset=utf-8", got.ContentType)
	require.Equal(t, int64(999), got.Size)
}

func TestWAL_NonexistentDirReturnsError(t *testing.T) {
	// dir with no write permission → Open should fail to MkdirAll
	// Use a file path as dir (can't MkdirAll over a regular file)
	f, err := os.CreateTemp("", "wal-test-file")
	require.NoError(t, err)
	f.Close()
	defer os.Remove(f.Name())

	_, err = wal.Open(f.Name() + "/subdir") // f.Name() is a file, not a dir
	require.Error(t, err)
}
