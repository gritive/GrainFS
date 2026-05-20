package iamadmin

import (
	"strings"
	"testing"
)

func TestResolveEndpoint_FromFlag(t *testing.T) {
	t.Setenv("GRAINFS_ADMIN_SOCKET", "")
	got, err := ResolveEndpoint("/tmp/x.sock")
	if err != nil {
		t.Fatal(err)
	}
	if got != "/tmp/x.sock" {
		t.Errorf("got %q", got)
	}
}

func TestResolveEndpoint_FromEnv(t *testing.T) {
	t.Setenv("GRAINFS_ADMIN_SOCKET", "/tmp/env.sock")
	got, err := ResolveEndpoint("")
	if err != nil {
		t.Fatal(err)
	}
	if got != "/tmp/env.sock" {
		t.Errorf("got %q", got)
	}
}

func TestResolveEndpoint_FlagOverridesEnv(t *testing.T) {
	t.Setenv("GRAINFS_ADMIN_SOCKET", "/tmp/env.sock")
	got, err := ResolveEndpoint("/tmp/flag.sock")
	if err != nil {
		t.Fatal(err)
	}
	if got != "/tmp/flag.sock" {
		t.Errorf("got %q", got)
	}
}

func TestResolveEndpoint_StripsUnixPrefix(t *testing.T) {
	t.Setenv("GRAINFS_ADMIN_SOCKET", "")
	got, err := ResolveEndpoint("unix:/tmp/x.sock")
	if err != nil {
		t.Fatal(err)
	}
	if got != "/tmp/x.sock" {
		t.Errorf("got %q", got)
	}
}

func TestResolveEndpoint_RejectsHTTP(t *testing.T) {
	t.Setenv("GRAINFS_ADMIN_SOCKET", "")
	_, err := ResolveEndpoint("http://localhost:9000")
	if err == nil || !strings.Contains(err.Error(), "UDS socket path") {
		t.Errorf("err = %v", err)
	}
}

func TestResolveEndpoint_EmptyErrors(t *testing.T) {
	t.Setenv("GRAINFS_ADMIN_SOCKET", "")
	_, err := ResolveEndpoint("")
	if err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Errorf("err = %v", err)
	}
}
