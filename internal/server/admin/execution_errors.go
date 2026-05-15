package admin

import "github.com/gritive/GrainFS/internal/server/execution"

func executionErrorToAdmin(err error) *Error {
	switch execution.CodeOf(err) {
	case execution.CodeInvalid:
		return NewInvalid(err.Error())
	case execution.CodeUnsupported:
		return NewUnsupported(err.Error(), nil)
	case execution.CodeRetry:
		return NewRetry(err.Error())
	case execution.CodeJobTimeout:
		return &Error{Code: "job_timeout", Message: err.Error()}
	case execution.CodeJobCancelled:
		return &Error{Code: "job_cancelled", Message: err.Error()}
	case execution.CodeJobFailed:
		return &Error{Code: "job_failed", Message: err.Error()}
	case execution.CodeAggregationFailed:
		return &Error{Code: "aggregation_failed", Message: err.Error()}
	default:
		return NewInternal(err.Error())
	}
}
