package execution

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestNewRequestIDIsUUIDV7(t *testing.T) {
	id, err := NewRequestID()
	require.NoError(t, err)
	require.NotEmpty(t, id)

	parsed, err := uuid.Parse(id)
	require.NoError(t, err)
	require.Equal(t, uuid.Version(7), parsed.Version())
}

func TestScrubOperationValidateRequiresBucket(t *testing.T) {
	err := (Operation{Kind: OperationScrub, Scrub: ScrubOperation{}}).Validate()
	require.True(t, errors.Is(err, ErrInvalidOperation))
	require.Equal(t, CodeInvalid, CodeOf(err))
}

func TestScrubOperationValidateAllowsFullLiveAndDefaultScopes(t *testing.T) {
	for _, scope := range []ScrubScope{"", ScrubScopeFull, ScrubScopeLive} {
		op := Operation{Kind: OperationScrub, Scrub: ScrubOperation{Bucket: "b1", Scope: scope}}
		require.NoError(t, op.Validate())
	}
}

func TestScrubOperationValidateRejectsUnknownScope(t *testing.T) {
	err := (Operation{Kind: OperationScrub, Scrub: ScrubOperation{Bucket: "b1", Scope: "bogus"}}).Validate()
	require.True(t, errors.Is(err, ErrInvalidOperation))
	require.Equal(t, CodeInvalid, CodeOf(err))
}

func TestResultCarriesStableScrubResponseFields(t *testing.T) {
	result := Result{Scrub: ScrubResult{SessionID: "sid-1", Created: false}}
	require.Equal(t, "sid-1", result.Scrub.SessionID)
	require.False(t, result.Scrub.Created)
}

func TestNewErrorSupportsTypedErrorAsAndSentinelIs(t *testing.T) {
	err := NewError(CodeInvalid, ErrInvalidOperation)
	typedErr := NewError(CodeInvalid, ErrInvalidOperation)

	var typed *Error
	require.True(t, errors.As(err, &typed))
	require.Equal(t, CodeInvalid, typed.Code)
	require.True(t, errors.Is(err, ErrInvalidOperation))
	require.Equal(t, CodeInvalid, typedErr.Code)
}

func TestCodeOfSupportsTypedErrorPointer(t *testing.T) {
	require.Equal(t, CodeRetry, CodeOf(&Error{Code: CodeRetry}))
}

func TestCodeOfMapsBoundedSentinels(t *testing.T) {
	tests := []struct {
		name string
		err  error
		code Code
	}{
		{name: "invalid operation", err: ErrInvalidOperation, code: CodeInvalid},
		{name: "execution unsupported", err: ErrExecutionUnsupported, code: CodeUnsupported},
		{name: "admission rejected", err: ErrAdmissionRejected, code: CodeRetry},
		{name: "job timed out", err: ErrJobTimedOut, code: CodeJobTimeout},
		{name: "job cancelled", err: ErrJobCancelled, code: CodeJobCancelled},
		{name: "partition failed", err: ErrPartitionFailed, code: CodeJobFailed},
		{name: "aggregation failed", err: ErrAggregationFailed, code: CodeAggregationFailed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.code, CodeOf(tt.err))
		})
	}
}
