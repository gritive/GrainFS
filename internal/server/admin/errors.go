package admin

import (
	"encoding/json"

	"github.com/gritive/GrainFS/internal/adminapi"
)

type Error = adminapi.Error

func NewNotFound(msg string) *Error { return &Error{Code: "not_found", Message: msg} }
func NewInvalid(msg string) *Error  { return &Error{Code: "invalid", Message: msg} }
func NewUnauthorized(msg string) *Error {
	return &Error{Code: "unauthorized", Message: msg}
}
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
