package storage

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnsupportedOperationErrorMatchesSentinel(t *testing.T) {
	err := UnsupportedOperationError{Op: "DeleteObjectVersion", Reason: UnsupportedReasonNoAdapter}

	require.ErrorIs(t, err, ErrUnsupportedOperation)
	require.Equal(t, "DeleteObjectVersion", err.Op)
	require.Equal(t, UnsupportedReasonNoAdapter, err.Reason)
	require.Contains(t, err.Error(), "DeleteObjectVersion")
	require.Contains(t, err.Error(), string(UnsupportedReasonNoAdapter))
}

func TestUnsupportedOperationErrorsAsTyped(t *testing.T) {
	err := error(UnsupportedOperationError{Op: "CopyObject", Reason: UnsupportedReasonUnsafeFallback})
	var typed UnsupportedOperationError

	require.True(t, errors.As(err, &typed))
	require.Equal(t, "CopyObject", typed.Op)
	require.Equal(t, UnsupportedReasonUnsafeFallback, typed.Reason)
}
