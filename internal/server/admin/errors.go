package admin

import (
	"encoding/json"

	"github.com/gritive/GrainFS/internal/adminapi"
)

type Error = adminapi.Error

func NewNotFound(msg string) *Error  { return &Error{Code: "not_found", Message: msg} }
func NewInvalid(msg string) *Error   { return &Error{Code: "invalid", Message: msg} }
func NewForbidden(msg string) *Error { return &Error{Code: "forbidden", Message: msg} }
func NewInternal(msg string) *Error  { return &Error{Code: "internal", Message: msg} }
func NewConflict(msg string, details map[string]any) *Error {
	raw, _ := json.Marshal(details)
	return &Error{Code: "conflict", Message: msg, Details: raw}
}
func NewUnsupported(msg string, details map[string]any) *Error {
	raw, _ := json.Marshal(details)
	return &Error{Code: "unsupported", Message: msg, Details: raw}
}
func NewRetry(msg string) *Error       { return &Error{Code: "retry", Message: msg} }
func NewUnavailable(msg string) *Error { return &Error{Code: "unavailable", Message: msg} }
func NewBucketNotFound(bucket string) *Error {
	return (&Error{Code: "bucket_not_found", Message: "bucket '" + bucket + "' does not exist"}).
		WithParam("bucket").
		WithHelp("Create the bucket first with 'grainfs bucket create " + bucket + "', then re-run.").
		WithDocs("https://github.com/gritive/GrainFS/docs/operators/nfs-export-lifecycle.md#bucket-not-found")
}
func NewExportNotFound(bucket string) *Error {
	return (&Error{Code: "export_not_found", Message: "NFS export '" + bucket + "' is not registered"}).
		WithParam("bucket").
		WithHelp("List existing exports with 'grainfs nfs export list'.").
		WithDocs("https://github.com/gritive/GrainFS/docs/operators/nfs-export-lifecycle.md#export-not-found")
}
func NewExportPropagationTimeout(bucket string) *Error {
	return (&Error{Code: "export_propagation_timeout", Message: "NFS export '" + bucket + "' did not propagate before the admin timeout"}).
		WithParam("bucket").
		WithHelp("Check cluster health and retry with a longer --timeout if nodes are healthy.").
		WithDocs("https://github.com/gritive/GrainFS/docs/operators/nfs-export-lifecycle.md#propagation-timeout")
}
