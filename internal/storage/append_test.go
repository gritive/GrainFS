package storage

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAppendSummaryETagStateMatchesCompositeETag(t *testing.T) {
	digests := [][]byte{
		bytes.Repeat([]byte{0x11}, 16),
		bytes.Repeat([]byte{0x22}, 16),
		bytes.Repeat([]byte{0x33}, 16),
	}
	var (
		state []byte
		count int
		err   error
	)
	for i, digest := range digests {
		state, count, err = appendETagStateAppend(state, count, digest)
		require.NoError(t, err, "append state %d", i)
		got, err := compositeETagFromState(state, count)
		require.NoError(t, err, "etag from state %d", i)
		require.Equal(t, CompositeETag(digests[:i+1]), got)
	}
}

func TestErrAppendObjectTooLargeSentinel(t *testing.T) {
	require.ErrorIs(t, ErrAppendObjectTooLarge, ErrAppendObjectTooLarge)
	require.False(t, errors.Is(ErrAppendObjectTooLarge, ErrAppendCapExceeded), "ErrAppendObjectTooLarge must not alias ErrAppendCapExceeded")
}
