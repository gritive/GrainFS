package volumeadmin

import (
	"strings"
	"testing"
)

func TestResolveEndpoint_Flag(t *testing.T) {
	got, err := ResolveEndpoint("/from/flag/admin.sock")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got != "/from/flag/admin.sock" {
		t.Errorf("got %q, want flag value", got)
	}
}

func TestResolveEndpoint_FailFastWithHint(t *testing.T) {
	_, err := ResolveEndpoint("")
	if err == nil {
		t.Fatal("want error")
	}
	msg := err.Error()
	for _, want := range []string{"admin endpoint not configured", "--endpoint"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error missing %q: %s", want, msg)
		}
	}
	if strings.Contains(msg, "GRAINFS_ENDPOINT") {
		t.Errorf("hint should not mention env var: %s", msg)
	}
}

func TestResolveEndpoint_TrimsWhitespace(t *testing.T) {
	got, err := ResolveEndpoint("  /trim/admin.sock  ")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got != "/trim/admin.sock" {
		t.Errorf("got %q, want trimmed", got)
	}
}
