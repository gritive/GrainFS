package volumeadmin

import (
	"bytes"
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

func TestAsDeleteConflict(t *testing.T) {
	e := &Error{
		Code: "conflict",
		Details: map[string]any{
			"snapshot_count":  float64(3),
			"recent":          []any{map[string]any{"id": "snap-1", "created_at": "2026-01-01T00:00:00Z", "block_count": float64(42)}},
			"cascade_command": "grainfs volume delete v1 --force",
			"list_command":    "grainfs volume snapshot list v1",
		},
	}
	d := AsDeleteConflict(e)
	if d == nil {
		t.Fatal("expected typed details")
	}
	if d.SnapshotCount != 3 {
		t.Errorf("SnapshotCount=%d want 3", d.SnapshotCount)
	}
	if len(d.Recent) != 1 || d.Recent[0].ID != "snap-1" || d.Recent[0].BlockCount != 42 {
		t.Errorf("Recent unexpected: %+v", d.Recent)
	}
	if d.CascadeCommand == "" || d.ListCommand == "" {
		t.Errorf("commands missing: %+v", d)
	}
}

func TestAsDeleteConflict_WrongCode(t *testing.T) {
	if AsDeleteConflict(&Error{Code: "internal"}) != nil {
		t.Errorf("non-conflict should return nil")
	}
	if AsDeleteConflict(&Error{Code: "conflict"}) != nil {
		t.Errorf("conflict without details should return nil")
	}
}

func TestAsResizeUnsupported(t *testing.T) {
	e := &Error{
		Code: "unsupported",
		Details: map[string]any{
			"current_size":  float64(2 << 30),
			"requested":     float64(1 << 30),
			"hint":          "clone to a smaller new volume instead",
			"clone_command": "grainfs volume clone v1 <new>",
		},
	}
	d := AsResizeUnsupported(e)
	if d == nil {
		t.Fatal("expected typed details")
	}
	if d.CurrentSize != 2<<30 || d.Requested != 1<<30 {
		t.Errorf("sizes: %+v", d)
	}
	if !strings.Contains(d.Hint, "clone") {
		t.Errorf("hint missing: %q", d.Hint)
	}
}

func TestFormatDeleteConflict(t *testing.T) {
	e := &Error{
		Code:    "conflict",
		Message: "volume \"v1\" has 3 snapshots; refused without --force",
		Details: map[string]any{
			"snapshot_count":  float64(3),
			"recent":          []any{map[string]any{"id": "snap-1", "created_at": "2026-01-01T00:00:00Z", "block_count": float64(42)}},
			"cascade_command": "grainfs volume delete v1 --force",
			"list_command":    "grainfs volume snapshot list v1",
		},
	}
	var buf bytes.Buffer
	FormatDeleteConflict(&buf, e)
	out := buf.String()
	for _, want := range []string{
		"refused without --force",
		"Recent snapshots:",
		"snap-1",
		"blocks=42",
		"Cascade:",
		"--force",
		"Or list:",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n---\n%s", want, out)
		}
	}
}

func TestFormatDeleteConflict_NoDetails(t *testing.T) {
	var buf bytes.Buffer
	FormatDeleteConflict(&buf, &Error{Code: "conflict", Message: "raw"})
	if got := buf.String(); got != "raw\n" {
		t.Errorf("expected fallback message only, got %q", got)
	}
}

func TestFormatResizeUnsupported(t *testing.T) {
	e := &Error{
		Code:    "unsupported",
		Message: "shrink not supported",
		Details: map[string]any{
			"hint":          "clone to a smaller new volume instead",
			"clone_command": "grainfs volume clone v1 <new>",
		},
	}
	var buf bytes.Buffer
	FormatResizeUnsupported(&buf, e)
	out := buf.String()
	for _, want := range []string{"shrink not supported", "Hint:", "clone to a smaller", "Try:", "<new>"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n---\n%s", want, out)
		}
	}
}
