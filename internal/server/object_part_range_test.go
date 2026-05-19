package server

import (
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

func TestPartRange_Multipart(t *testing.T) {
	obj := &storage.Object{
		Size: 12,
		Parts: []storage.MultipartPartEntry{
			{PartNumber: 1, Size: 5, ETag: "p1"},
			{PartNumber: 2, Size: 7, ETag: "p2"},
		},
	}
	s, e, etag, cnt, ok := partRange(obj, 1)
	require.True(t, ok)
	require.Equal(t, int64(0), s)
	require.Equal(t, int64(4), e)
	require.Equal(t, "p1", etag)
	require.Equal(t, 2, cnt)

	s, e, etag, cnt, ok = partRange(obj, 2)
	require.True(t, ok)
	require.Equal(t, int64(5), s)
	require.Equal(t, int64(11), e)
	require.Equal(t, "p2", etag)
	require.Equal(t, 2, cnt)

	_, _, _, _, ok = partRange(obj, 0)
	require.False(t, ok)
	_, _, _, _, ok = partRange(obj, 3)
	require.False(t, ok)
}

func TestPartRange_SinglePutBackcompat(t *testing.T) {
	obj := &storage.Object{Size: 10, ETag: "single"}
	s, e, etag, cnt, ok := partRange(obj, 1)
	require.True(t, ok)
	require.Equal(t, int64(0), s)
	require.Equal(t, int64(9), e)
	require.Equal(t, "single", etag)
	require.Equal(t, 1, cnt)
	_, _, _, _, ok = partRange(obj, 2)
	require.False(t, ok)
}
