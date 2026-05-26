package volumeadmin

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
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

func TestAsResizeUnsupported(t *testing.T) {
	e := &Error{
		Code: "unsupported",
		Details: json.RawMessage(`{
			"current_size": 2147483648,
			"requested": 1073741824,
			"hint": "create a smaller new volume and copy the data instead"
		}`),
	}
	d := AsResizeUnsupported(e)
	if d == nil {
		t.Fatal("expected typed details")
	}
	if d.CurrentSize != 2<<30 || d.Requested != 1<<30 {
		t.Errorf("sizes: %+v", d)
	}
	if !strings.Contains(d.Hint, "smaller") {
		t.Errorf("hint missing: %q", d.Hint)
	}
}

func TestFormatResizeUnsupported(t *testing.T) {
	e := &Error{
		Code:    "unsupported",
		Message: "shrink not supported",
		Details: json.RawMessage(`{
			"hint": "create a smaller new volume and copy the data instead"
		}`),
	}
	var buf bytes.Buffer
	FormatResizeUnsupported(&buf, e)
	out := buf.String()
	for _, want := range []string{"shrink not supported", "Hint:", "create a smaller"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n---\n%s", want, out)
		}
	}
}
