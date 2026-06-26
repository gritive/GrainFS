package storage

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func setupThreeSegmentObject(t *testing.T) (*LocalBackend, *Object) {
	t.Helper()
	b := newTestLocalBackend(t)
	body := bytes.Repeat([]byte("S"), 10<<20)
	var obj *Object
	off := int64(0)
	for i := 0; i < 3; i++ {
		o, err := b.AppendObject(context.Background(), "test", "k", off, bytes.NewReader(body))
		require.NoError(t, err, "append %d", i)
		obj = o
		off = o.Size
	}
	var err error
	obj, err = b.HeadObject(context.Background(), "test", "k")
	require.NoError(t, err, "HeadObject")
	return b, obj
}

func TestSegmentedReaderFullStitch(t *testing.T) {
	b, obj := setupThreeSegmentObject(t)
	r, err := b.OpenSegmentedReader("test", "k", obj, 0, obj.Size-1)
	require.NoError(t, err, "OpenSegmentedReader")
	defer r.Close()
	got, err := io.ReadAll(r)
	require.NoError(t, err, "ReadAll")
	require.Equal(t, obj.Size, int64(len(got)))
	for i, c := range got {
		require.Equal(t, byte('S'), c, "byte %d", i)
	}
}

func TestSegmentedReaderRangeWithinSingleSegment(t *testing.T) {
	b, obj := setupThreeSegmentObject(t)
	// Range: bytes=100-200 (within seg1)
	r, err := b.OpenSegmentedReader("test", "k", obj, 100, 200)
	require.NoError(t, err, "OpenSegmentedReader")
	defer r.Close()
	got, err := io.ReadAll(r)
	require.NoError(t, err, "ReadAll")
	require.Len(t, got, 101)
}

func TestSegmentedReaderRangeAcrossSegments(t *testing.T) {
	b, obj := setupThreeSegmentObject(t)
	// Range: bytes=5MiB - 15MiB → seg1[5MiB..10MiB) + seg2[0..5MiB]
	start := int64(5 << 20)
	end := int64(15<<20) - 1
	r, err := b.OpenSegmentedReader("test", "k", obj, start, end)
	require.NoError(t, err, "OpenSegmentedReader")
	defer r.Close()
	got, err := io.ReadAll(r)
	require.NoError(t, err, "ReadAll")
	want := end - start + 1
	require.Equal(t, want, int64(len(got)))
}

func TestSegmentedReaderRangeAtBoundary(t *testing.T) {
	b, obj := setupThreeSegmentObject(t)
	// 정확히 segment 경계: 1 byte from seg1 + 1 byte from seg2
	start := int64(10<<20) - 1
	end := int64(10 << 20)
	r, err := b.OpenSegmentedReader("test", "k", obj, start, end)
	require.NoError(t, err, "OpenSegmentedReader")
	defer r.Close()
	got, err := io.ReadAll(r)
	require.NoError(t, err, "ReadAll")
	require.Equal(t, []byte{'S', 'S'}, got)
}

func TestSegmentedReaderEncryptedTamperRejected(t *testing.T) {
	b := newDEKLocalBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "test"), "CreateBucket")

	body := bytes.Repeat([]byte("Z"), 1<<20)
	obj, err := b.AppendObject(ctx, "test", "k", 0, bytes.NewReader(body))
	require.NoError(t, err, "append")

	// segment blob 1바이트 tamper → AES-GCM tag mismatch.
	// Flip the existing byte (XOR 0xff) rather than overwriting with a fixed
	// value: a fixed 0xff would be a no-op ~1/256 of the time when the random
	// ciphertext already holds 0xff at this offset, making the test flaky.
	path := b.segmentPath("test", "k", obj.Segments[0].BlobID)
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err, "open")
	var orig [1]byte
	_, err = f.ReadAt(orig[:], 100)
	require.NoError(t, err, "read tamper byte")
	_, err = f.WriteAt([]byte{orig[0] ^ 0xff}, 100)
	require.NoError(t, err, "tamper")
	require.NoError(t, f.Close(), "close")

	r, err := b.OpenSegmentedReader("test", "k", obj, 0, obj.Size-1)
	require.NoError(t, err, "OpenSegmentedReader")
	defer r.Close()
	_, err = io.ReadAll(r)
	require.Error(t, err, "expected decrypt error")
}
