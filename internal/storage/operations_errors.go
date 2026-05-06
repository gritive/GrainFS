package storage

import (
	"errors"
	"fmt"
)

var ErrUnsupportedOperation = errors.New("unsupported storage operation")
var ErrPreconditionFailed = errors.New("storage precondition failed")
var ErrInvalidCopySource = errors.New("invalid copy source")

type UnsupportedReason string

const (
	UnsupportedReasonNoAdapter           UnsupportedReason = "no_adapter"
	UnsupportedReasonUnsafeFallback      UnsupportedReason = "unsafe_fallback"
	UnsupportedReasonRollbackFailed      UnsupportedReason = "rollback_failed"
	UnsupportedReasonMetadataUnsupported UnsupportedReason = "metadata_unsupported"
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

type CopyCondition string

const (
	CopyConditionIfMatch           CopyCondition = "if_match"
	CopyConditionIfNoneMatch       CopyCondition = "if_none_match"
	CopyConditionIfModifiedSince   CopyCondition = "if_modified_since"
	CopyConditionIfUnmodifiedSince CopyCondition = "if_unmodified_since"
)

type PreconditionFailedError struct {
	Op        string
	Condition CopyCondition
}

func (e PreconditionFailedError) Error() string {
	return fmt.Sprintf("%s: %s: %s", ErrPreconditionFailed, e.Op, e.Condition)
}

func (e PreconditionFailedError) Unwrap() error {
	return ErrPreconditionFailed
}

type InvalidCopySourceReason string

const (
	CopySourceIsDeleteMarker          InvalidCopySourceReason = "source_is_delete_marker"
	CopySourceSameAsDestinationNoop   InvalidCopySourceReason = "same_as_destination_noop"
	CopySourceUnsupportedETagSelector InvalidCopySourceReason = "unsupported_etag_selector"
)

type InvalidCopySourceError struct {
	Reason InvalidCopySourceReason
}

func (e InvalidCopySourceError) Error() string {
	return fmt.Sprintf("%s: %s", ErrInvalidCopySource, e.Reason)
}

func (e InvalidCopySourceError) Unwrap() error {
	return ErrInvalidCopySource
}
