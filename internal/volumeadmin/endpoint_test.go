package volumeadmin

import (
	"strings"
	"testing"
)

func TestResolveEndpoint_FlagWins(t *testing.T) {
	t.Setenv("GRAINFS_ENDPOINT", "/from/env/admin.sock")

	got, err := ResolveEndpoint("/from/flag/admin.sock")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got != "/from/flag/admin.sock" {
		t.Errorf("got %q, want flag value", got)
	}
}

func TestResolveEndpoint_EnvWhenFlagEmpty(t *testing.T) {
	t.Setenv("GRAINFS_ENDPOINT", "/from/env/admin.sock")

	got, err := ResolveEndpoint("")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got != "/from/env/admin.sock" {
		t.Errorf("got %q, want env value", got)
	}
}

func TestResolveEndpoint_FailFastWithHint(t *testing.T) {
	t.Setenv("GRAINFS_ENDPOINT", "")

	_, err := ResolveEndpoint("")
	if err == nil {
		t.Fatal("want error")
	}
	msg := err.Error()
	for _, want := range []string{"admin endpoint not configured", "--endpoint", "GRAINFS_ENDPOINT"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error missing %q: %s", want, msg)
		}
	}
}

func TestResolveEndpoint_TrimsWhitespace(t *testing.T) {
	t.Setenv("GRAINFS_ENDPOINT", "")

	got, err := ResolveEndpoint("  /trim/admin.sock  ")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got != "/trim/admin.sock" {
		t.Errorf("got %q, want trimmed", got)
	}
}
