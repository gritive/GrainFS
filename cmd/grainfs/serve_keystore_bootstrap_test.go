package main

import (
	"strings"
	"testing"

	"github.com/gritive/GrainFS/internal/transport"
)

// TestResolveClusterKey_DiskWinsOverFlagMismatch (D10): when --cluster-key flag
// and keys.d/current.key both present and differ, disk wins. Refuse-to-start
// path is explicitly NOT used.
func TestResolveClusterKey_DiskWinsOverFlagMismatch(t *testing.T) {
	dir := t.TempDir()
	ks := transport.NewKeystore(dir)
	diskKey := strings.Repeat("d", 64)
	if err := ks.WriteCurrent(diskKey); err != nil {
		t.Fatal(err)
	}

	flagKey := strings.Repeat("f", 64)
	resolved, warn, err := resolveClusterKey(dir, flagKey)
	if err != nil {
		t.Fatalf("resolve should not error on mismatch: %v", err)
	}
	if resolved != diskKey {
		t.Fatalf("disk should win: want %q, got %q", diskKey, resolved)
	}
	if warn == "" {
		t.Fatal("expected warning string on mismatch")
	}
}

func TestResolveClusterKey_DiskOnly(t *testing.T) {
	dir := t.TempDir()
	ks := transport.NewKeystore(dir)
	diskKey := strings.Repeat("d", 64)
	if err := ks.WriteCurrent(diskKey); err != nil {
		t.Fatal(err)
	}
	resolved, warn, err := resolveClusterKey(dir, "")
	if err != nil {
		t.Fatal(err)
	}
	if resolved != diskKey {
		t.Fatalf("want disk key, got %q", resolved)
	}
	if warn != "" {
		t.Fatalf("no warn expected, got %q", warn)
	}
}

func TestResolveClusterKey_FlagOnly_MirrorsToDisk(t *testing.T) {
	dir := t.TempDir()
	flagKey := strings.Repeat("f", 64)
	resolved, _, err := resolveClusterKey(dir, flagKey)
	if err != nil {
		t.Fatal(err)
	}
	if resolved != flagKey {
		t.Fatalf("want flag key, got %q", resolved)
	}
	ks := transport.NewKeystore(dir)
	disk, err := ks.ReadCurrent()
	if err != nil {
		t.Fatal(err)
	}
	if disk != flagKey {
		t.Fatalf("flag should mirror to disk: want %q, got %q", flagKey, disk)
	}
}

func TestResolveClusterKey_BothMatch_NoWarn(t *testing.T) {
	dir := t.TempDir()
	ks := transport.NewKeystore(dir)
	key := strings.Repeat("k", 64)
	if err := ks.WriteCurrent(key); err != nil {
		t.Fatal(err)
	}
	resolved, warn, err := resolveClusterKey(dir, key)
	if err != nil {
		t.Fatal(err)
	}
	if resolved != key {
		t.Fatalf("want %q, got %q", key, resolved)
	}
	if warn != "" {
		t.Fatalf("matching values should not warn, got %q", warn)
	}
}

func TestResolveClusterKey_BothEmpty(t *testing.T) {
	dir := t.TempDir()
	resolved, warn, err := resolveClusterKey(dir, "")
	if err != nil {
		t.Fatal(err)
	}
	if resolved != "" {
		t.Fatalf("both empty: want empty resolved, got %q", resolved)
	}
	if warn != "" {
		t.Fatalf("both empty: no warn, got %q", warn)
	}
}
