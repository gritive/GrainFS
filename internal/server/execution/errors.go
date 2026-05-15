package execution

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidOperation     = errors.New("invalid operation")
	ErrExecutionUnsupported = errors.New("execution unsupported")
	ErrAdmissionRejected    = errors.New("admission rejected")
	ErrJobTimedOut          = errors.New("job timed out")
	ErrJobCancelled         = errors.New("job cancelled")
	ErrPartitionFailed      = errors.New("partition failed")
	ErrAggregationFailed    = errors.New("aggregation failed")
)

type Code string

const (
	CodeInvalid           Code = "invalid"
	CodeUnsupported       Code = "unsupported"
	CodeRetry             Code = "retry"
	CodeJobTimeout        Code = "job_timeout"
	CodeJobCancelled      Code = "job_cancelled"
	CodeJobFailed         Code = "job_failed"
	CodeAggregationFailed Code = "aggregation_failed"
)

type Error struct {
	Code Code
	Err  error
}

func NewError(code Code, err error) *Error {
	return &Error{Code: code, Err: err}
}

func (e *Error) Error() string {
	if e.Err == nil {
		return string(e.Code)
	}
	return fmt.Sprintf("%s: %v", e.Code, e.Err)
}

func (e *Error) Unwrap() error { return e.Err }

func CodeOf(err error) Code {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	switch {
	case errors.Is(err, ErrInvalidOperation):
		return CodeInvalid
	case errors.Is(err, ErrExecutionUnsupported):
		return CodeUnsupported
	case errors.Is(err, ErrAdmissionRejected):
		return CodeRetry
	case errors.Is(err, ErrJobTimedOut):
		return CodeJobTimeout
	case errors.Is(err, ErrJobCancelled):
		return CodeJobCancelled
	case errors.Is(err, ErrPartitionFailed):
		return CodeJobFailed
	case errors.Is(err, ErrAggregationFailed):
		return CodeAggregationFailed
	default:
		return ""
	}
}
