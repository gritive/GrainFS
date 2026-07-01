package storage

import (
	"crypto/md5"
	"testing"

	"github.com/stretchr/testify/require"
)

// Backend-independent pin for the composite-ETag contract, proving the
// extraction out of append.go (ahead of LocalBackend removal) preserves the
// exact behavior that production cluster code depends on.
func TestCompositeETag_SingleAndMulti(t *testing.T) {
	d1 := md5.Sum([]byte("part-1"))
	d2 := md5.Sum([]byte("part-2"))

	// CompositeETag always carries the S3 multipart-style "-<count>" suffix.
	single := CompositeETag([][]byte{d1[:]})
	require.Contains(t, single, "-1")

	multi := CompositeETag([][]byte{d1[:], d2[:]})
	require.Contains(t, multi, "-2")
	require.NotEqual(t, single, multi)

	// Incremental state accumulation must match the all-at-once form.
	st, n, err := AppendETagStateAppend(nil, 0, d1[:])
	require.NoError(t, err)
	st, n, err = AppendETagStateAppend(st, n, d2[:])
	require.NoError(t, err)
	require.Equal(t, 2, n)
	fromState, err := CompositeETagFromState(st, n)
	require.NoError(t, err)

	// AppendETagStateFromDigests is the batch equivalent of repeated appends.
	stDigests, nDigests, err := AppendETagStateFromDigests([][]byte{d1[:], d2[:]})
	require.NoError(t, err)
	fromDigests, err := CompositeETagFromState(stDigests, nDigests)
	require.NoError(t, err)

	require.Equal(t, fromDigests, fromState)
	// The stateful path and the one-shot CompositeETag must agree.
	require.Equal(t, multi, fromState)
}
