package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/gritive/GrainFS/internal/chunkref"
	"github.com/stretchr/testify/require"
)

func newTestLocalBackend(t *testing.T) *LocalBackend {
	t.Helper()
	dir := t.TempDir()
	b, err := NewLocalBackend(dir)
	require.NoError(t, err, "NewLocalBackend")
	t.Cleanup(func() { _ = b.Close() })
	require.NoError(t, b.CreateBucket(context.Background(), "test"), "CreateBucket")
	return b
}

func TestAppendObjectRejectsMismatchedOffset(t *testing.T) {
	b := newTestLocalBackend(t)
	ctx := context.Background()
	_, err := b.AppendObject(ctx, "test", "k", 0, strings.NewReader("0123456789"))
	require.NoError(t, err, "initial")
	_, err = b.AppendObject(ctx, "test", "k", 5, bytes.NewReader([]byte("xxx")))
	require.ErrorIs(t, err, ErrAppendOffsetMismatch)
}

func TestAppendObjectInitialCreates10MiBSegment(t *testing.T) {
	b := newTestLocalBackend(t)
	obj, err := b.AppendObject(context.Background(), "test", "k", 0, newRepeatByteReader('A', 10<<20)) // 10 MiB
	require.NoError(t, err, "append")
	require.Equal(t, int64(10<<20), obj.Size)
	require.Len(t, obj.Segments, 1)
	require.True(t, obj.IsAppendable, "IsAppendable")
	// Until Task 3.1 wires real per-call MD5s, the prefix is an MD5 of segment-checksum bytes — assert structure only.
	require.True(t, strings.HasSuffix(obj.ETag, "-1"), "etag=%q, want suffix -1", obj.ETag)
	require.Equal(t, 32, strings.IndexByte(obj.ETag, '-'), "etag=%q, want 32 hex chars before '-'", obj.ETag)
}

func TestAppendObjectSequentialThreeSegments(t *testing.T) {
	b := newTestLocalBackend(t)

	off := int64(0)
	var obj *Object
	for i := 0; i < 3; i++ {
		var err error
		obj, err = b.AppendObject(context.Background(), "test", "k", off, newRepeatByteReader('X', 10<<20)) // 10 MiB
		require.NoError(t, err, "append %d", i)
		off = obj.Size
	}
	require.Equal(t, int64(30<<20), obj.Size)
	require.Len(t, obj.Segments, 3)
	// Until Task 3.1 wires real per-call MD5s, the prefix is an MD5 of segment-checksum bytes — assert structure only.
	require.True(t, strings.HasSuffix(obj.ETag, "-3"), "etag=%q, want suffix -3", obj.ETag)
	require.Equal(t, 32, strings.IndexByte(obj.ETag, '-'), "etag=%q, want 32 hex chars before '-'", obj.ETag)
}

func TestAppendObjectStoresSegmentsInSideRecords(t *testing.T) {
	b := newTestLocalBackend(t)
	ctx := context.Background()

	obj, err := b.AppendObject(ctx, "test", "k", 0, strings.NewReader("hello"))
	require.NoError(t, err, "initial append")
	obj, err = b.AppendObject(ctx, "test", "k", obj.Size, strings.NewReader(" world"))
	require.NoError(t, err, "second append")
	require.Len(t, obj.Segments, 2)

	if err := b.db.View(func(txn *badger.Txn) error {
		raw, err := b.readObjectInTxn(txn, b.objectMetaKey("test", "k"))
		if err != nil {
			return err
		}
		require.Empty(t, raw.Segments, "raw manifest segments")
		summary, err := b.readAppendSummaryInTxn(txn, "test", "k", raw.VersionID)
		if err != nil {
			return err
		}
		require.Equal(t, appendSummary{Size: obj.Size, SegmentCount: len(obj.Segments)}, summary)
		return nil
	}); err != nil {
		require.NoError(t, err, "view")
	}
}

func TestAppendObjectConvertsBrownfieldAppendManifestToSideRecords(t *testing.T) {
	b := newTestLocalBackend(t)
	ctx := context.Background()

	seg, err := b.WriteSegmentBlob("test", "k", strings.NewReader("hello"))
	require.NoError(t, err, "WriteSegmentBlob")
	obj := &Object{
		Key:            "k",
		Size:           seg.Size,
		ContentType:    "application/octet-stream",
		ETag:           CompositeETag([][]byte{append([]byte(nil), seg.Checksum...)}),
		LastModified:   1,
		Segments:       []SegmentRef{seg},
		AppendCallMD5s: [][]byte{append([]byte(nil), seg.Checksum...)},
		IsAppendable:   true,
	}
	require.NoError(t, b.PutObjectRecord(ctx, "test", "k", obj), "PutObjectRecord")
	obj, err = b.AppendObject(ctx, "test", "k", obj.Size, strings.NewReader(" world"))
	require.NoError(t, err, "AppendObject")

	rc, _, err := b.GetObject(ctx, "test", "k")
	require.NoError(t, err, "GetObject")
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err, "ReadAll")
	require.Equal(t, "hello world", string(got))

	if err := b.db.View(func(txn *badger.Txn) error {
		raw, err := b.readObjectInTxn(txn, b.objectMetaKey("test", "k"))
		if err != nil {
			return err
		}
		require.Empty(t, raw.Segments, "raw manifest segments after conversion")
		summary, err := b.readAppendSummaryInTxn(txn, "test", "k", raw.VersionID)
		if err != nil {
			return err
		}
		require.Equal(t, appendSummary{Size: obj.Size, SegmentCount: len(obj.Segments)}, summary)
		return nil
	}); err != nil {
		require.NoError(t, err, "view")
	}
}

type repeatByteReader struct {
	b byte
	n int64
}

func newRepeatByteReader(b byte, n int64) *repeatByteReader {
	return &repeatByteReader{b: b, n: n}
}

func (r *repeatByteReader) Read(p []byte) (int, error) {
	if r.n == 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > r.n {
		p = p[:r.n]
	}
	for i := range p {
		p[i] = r.b
	}
	r.n -= int64(len(p))
	return len(p), nil
}

func TestAppendObjectRejectsAtCap(t *testing.T) {
	// Save and restore cap for fast test
	orig := MaxAppendSegments
	t.Cleanup(func() { MaxAppendSegments = orig })
	MaxAppendSegments = 4 // local override

	b := newTestLocalBackend(t)
	ctx := context.Background()
	body := []byte("ABC")

	off := int64(0)
	for i := 0; i < 4; i++ {
		obj, err := b.AppendObject(ctx, "test", "k", off, bytes.NewReader(body))
		require.NoError(t, err, "append %d", i)
		off = obj.Size
	}
	_, err := b.AppendObject(ctx, "test", "k", off, bytes.NewReader(body))
	require.ErrorIs(t, err, ErrAppendCapExceeded)
}

func TestAppendObjectConvertsPlainPutAtCurrentOffset(t *testing.T) {
	b := newTestLocalBackend(t)
	ctx := context.Background()

	_, err := b.PutObject(ctx, "test", "k", strings.NewReader("hello"), "text/plain")
	require.NoError(t, err, "PutObject")

	obj, err := b.AppendObject(ctx, "test", "k", 5, bytes.NewReader([]byte("world")))
	require.NoError(t, err, "AppendObject")
	require.True(t, obj.IsAppendable, "IsAppendable")
	require.Equal(t, int64(10), obj.Size)

	rc, _, err := b.GetObject(ctx, "test", "k")
	require.NoError(t, err, "GetObject")
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err, "ReadAll")
	require.Equal(t, "helloworld", string(got))
}

func TestAppendObjectConvertsPlainPutAddsBaseAndAppendRefs(t *testing.T) {
	b := newTestLocalBackend(t)
	ctx := context.Background()

	_, err := b.PutObject(ctx, "test", "k", strings.NewReader("hello"), "text/plain")
	require.NoError(t, err, "PutObject")
	obj, err := b.AppendObject(ctx, "test", "k", 5, bytes.NewReader([]byte("world")))
	require.NoError(t, err, "AppendObject")
	require.Len(t, obj.Segments, 2)
	m := chunkref.ObjectVersionID("test", "k", obj.VersionID)
	if err := b.db.View(func(txn *badger.Txn) error {
		s := NewChunkRefStore(txn)
		for _, seg := range obj.Segments {
			c := chunkref.ChunkID(ParseLocator(seg.BlobID).String())
			n, err := s.RefCount(c)
			require.NoError(t, err)
			require.Equal(t, 1, n, "RefCount(%s)", c)
			_, err = txn.Get(refMembershipKey(m, c))
			require.NoError(t, err, "missing ref (%v, %s)", m, c)
		}
		return nil
	}); err != nil {
		require.NoError(t, err, "view")
	}
}

func TestWriteSegmentBlob_PopulatesChecksum(t *testing.T) {
	b := newTestLocalBackend(t)

	data := []byte("hello segment world")
	ref, err := b.WriteSegmentBlob("test", "key-a", bytes.NewReader(data))
	require.NoError(t, err, "write")
	require.Len(t, ref.Checksum, ChecksumLen)
	want := ChecksumOf(data)
	require.True(t, bytes.Equal(ref.Checksum, want), "checksum mismatch: want %x, got %x", want, ref.Checksum)
}

func TestErrAppendObjectTooLargeSentinel(t *testing.T) {
	require.ErrorIs(t, ErrAppendObjectTooLarge, ErrAppendObjectTooLarge)
	require.False(t, errors.Is(ErrAppendObjectTooLarge, ErrAppendCapExceeded), "ErrAppendObjectTooLarge must not alias ErrAppendCapExceeded")
}
