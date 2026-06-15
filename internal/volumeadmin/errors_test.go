package volumeadmin

import (
	"errors"
	"testing"
)

func TestErrorUnwrap(t *testing.T) {
	cause := errors.New("underlying")
	e := (&Error{Code: "transient", Message: "wrapped"}).WithCause(cause)
	if !errors.Is(e, cause) {
		t.Errorf("errors.Is should see through Unwrap")
	}
}

func TestErrorImplementsError(t *testing.T) {
	tests := []struct {
		name string
		e    *Error
		want string
	}{
		{"message wins", &Error{Code: "x", Message: "hi"}, "hi"},
		{"code fallback", &Error{Code: "internal"}, "internal"},
	}
	for _, tc := range tests {
		if got := tc.e.Error(); got != tc.want {
			t.Errorf("%s: got %q want %q", tc.name, got, tc.want)
		}
	}
}

func TestIsCode(t *testing.T) {
	e := &Error{Code: "conflict"}
	if !IsCode(e, "conflict") {
		t.Errorf("expected IsCode(conflict)=true")
	}
	if IsCode(e, "internal") {
		t.Errorf("expected IsCode(internal)=false")
	}
	if IsCode(nil, "conflict") {
		t.Errorf("expected IsCode(nil)=false")
	}
}
