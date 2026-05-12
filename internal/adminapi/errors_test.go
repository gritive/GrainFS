package adminapi

import (
	"context"
	"errors"
	"net/http"
	"testing"
)

func TestError_StatusAndUnwrap(t *testing.T) {
	cause := context.Canceled
	e := &Error{Status: http.StatusConflict, Code: "conflict", Message: "boom"}
	if e.Error() != "boom" {
		t.Fatalf("Error()=%q, want %q", e.Error(), "boom")
	}
	if e.Status != http.StatusConflict {
		t.Fatalf("Status=%d, want %d", e.Status, http.StatusConflict)
	}
	withCause := &Error{Code: "transient", Message: "x", cause: cause}
	if !errors.Is(withCause, context.Canceled) {
		t.Fatal("errors.Is should see wrapped cause")
	}
}

func TestError_IsCode(t *testing.T) {
	e := &Error{Code: "remove_peer_blocked", Message: "x"}
	if !IsCode(e, "remove_peer_blocked") {
		t.Fatal("IsCode should match")
	}
	if IsCode(e, "other") {
		t.Fatal("IsCode should not match other")
	}
	if IsCode(nil, "remove_peer_blocked") {
		t.Fatal("IsCode(nil) should be false")
	}
}

func TestError_Error_FallbackToCode(t *testing.T) {
	e := &Error{Code: "x", Message: ""}
	if e.Error() != "x" {
		t.Fatalf("Error()=%q, want code as fallback", e.Error())
	}
}
