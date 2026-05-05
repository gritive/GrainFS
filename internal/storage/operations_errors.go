package storage

import (
	"errors"
	"fmt"
)

var ErrUnsupportedOperation = errors.New("unsupported storage operation")

type UnsupportedReason string

const (
	UnsupportedReasonNoAdapter      UnsupportedReason = "no_adapter"
	UnsupportedReasonUnsafeFallback UnsupportedReason = "unsafe_fallback"
	UnsupportedReasonRollbackFailed UnsupportedReason = "rollback_failed"
)

type UnsupportedOperationError struct {
	Op     string
	Reason UnsupportedReason
}

func (e UnsupportedOperationError) Error() string {
	return fmt.Sprintf("%s: %s: %s", ErrUnsupportedOperation, e.Op, e.Reason)
}

func (e UnsupportedOperationError) Unwrap() error {
	return ErrUnsupportedOperation
}
