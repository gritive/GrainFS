package serveruntime

import (
	"errors"
	"strings"
	"testing"
)

func TestHandleRuntimeGroupInstantiationError_Nil(t *testing.T) {
	if got := HandleRuntimeGroupInstantiationError("group-3", nil); got != nil {
		t.Fatalf("nil err should pass through, got %v", got)
	}
}

func TestHandleRuntimeGroupInstantiationError_WrapsWithGroupID(t *testing.T) {
	cause := errors.New("badger lock held")
	got := HandleRuntimeGroupInstantiationError("group-3", cause)
	if got == nil {
		t.Fatalf("non-nil err should be wrapped")
	}
	if !errors.Is(got, cause) {
		t.Fatalf("wrap broke errors.Is chain: %v", got)
	}
	if !strings.Contains(got.Error(), "group-3") {
		t.Fatalf("group id missing from message: %q", got.Error())
	}
	if !strings.Contains(got.Error(), "instantiation failed") {
		t.Fatalf("stable prefix missing: %q", got.Error())
	}
}
