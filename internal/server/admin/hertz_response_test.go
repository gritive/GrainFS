package admin

import (
	"testing"

	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/server/execution"
)

func TestStatusForExecutionCodes(t *testing.T) {
	require.Equal(t, consts.StatusGatewayTimeout, statusForCode("job_timeout"))
	require.Equal(t, consts.StatusConflict, statusForCode("job_cancelled"))
	require.Equal(t, consts.StatusInternalServerError, statusForCode("job_failed"))
	require.Equal(t, consts.StatusInternalServerError, statusForCode("aggregation_failed"))
}

func TestExecutionErrorToAdminError(t *testing.T) {
	tests := []struct {
		name       string
		code       execution.Code
		err        error
		adminCode  string
		httpStatus int
	}{
		{
			name:       "invalid",
			code:       execution.CodeInvalid,
			err:        execution.ErrInvalidOperation,
			adminCode:  "invalid",
			httpStatus: consts.StatusBadRequest,
		},
		{
			name:       "unsupported",
			code:       execution.CodeUnsupported,
			err:        execution.ErrExecutionUnsupported,
			adminCode:  "unsupported",
			httpStatus: consts.StatusUnprocessableEntity,
		},
		{
			name:       "retry",
			code:       execution.CodeRetry,
			err:        execution.ErrAdmissionRejected,
			adminCode:  "retry",
			httpStatus: consts.StatusServiceUnavailable,
		},
		{
			name:       "job timeout",
			code:       execution.CodeJobTimeout,
			err:        execution.ErrJobTimedOut,
			adminCode:  "job_timeout",
			httpStatus: consts.StatusGatewayTimeout,
		},
		{
			name:       "job cancelled",
			code:       execution.CodeJobCancelled,
			err:        execution.ErrJobCancelled,
			adminCode:  "job_cancelled",
			httpStatus: consts.StatusConflict,
		},
		{
			name:       "job failed",
			code:       execution.CodeJobFailed,
			err:        execution.ErrPartitionFailed,
			adminCode:  "job_failed",
			httpStatus: consts.StatusInternalServerError,
		},
		{
			name:       "aggregation failed",
			code:       execution.CodeAggregationFailed,
			err:        execution.ErrAggregationFailed,
			adminCode:  "aggregation_failed",
			httpStatus: consts.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := executionErrorToAdmin(execution.NewError(tt.code, tt.err))
			require.Equal(t, tt.adminCode, got.Code)
			require.Equal(t, tt.httpStatus, statusForCode(got.Code))
		})
	}
}
