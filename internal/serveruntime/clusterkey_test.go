package serveruntime

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gritive/GrainFS/internal/transport"
)

func TestResolveClusterKey_BothEmpty(t *testing.T) {
	dir := t.TempDir()
	got, warn, err := ResolveClusterKey(dir, "")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got != "" || warn != "" {
		t.Fatalf("want empty/empty, got %q/%q", got, warn)
	}
}

func TestResolveClusterKey_FlagOnlyMirrorsToDisk(t *testing.T) {
	dir := t.TempDir()
	flag := strings.Repeat("a", 64)
	got, warn, err := ResolveClusterKey(dir, flag)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got != flag {
		t.Fatalf("want flag, got %q", got)
	}
	if warn != "" {
		t.Fatalf("unexpected warn: %q", warn)
	}
	disk, err := transport.NewKeystore(dir).ReadCurrent()
	if err != nil {
		t.Fatalf("disk read: %v", err)
	}
	if disk != flag {
		t.Fatalf("flag was not mirrored to disk: got %q", disk)
	}
}

func TestResolveClusterKey_DiskOnly(t *testing.T) {
	dir := t.TempDir()
	disk := strings.Repeat("b", 64)
	if err := transport.NewKeystore(dir).WriteCurrent(disk); err != nil {
		t.Fatalf("seed disk: %v", err)
	}
	got, warn, err := ResolveClusterKey(dir, "")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got != disk {
		t.Fatalf("want disk, got %q", got)
	}
	if warn != "" {
		t.Fatalf("unexpected warn: %q", warn)
	}
}

func TestResolveClusterKey_DiskWinsOnConflict(t *testing.T) {
	dir := t.TempDir()
	disk := strings.Repeat("c", 64)
	flag := strings.Repeat("d", 64)
	if err := transport.NewKeystore(dir).WriteCurrent(disk); err != nil {
		t.Fatalf("seed disk: %v", err)
	}
	got, warn, err := ResolveClusterKey(dir, flag)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got != disk {
		t.Fatalf("disk should win, got %q", got)
	}
	if warn == "" {
		t.Fatalf("expected mismatch warning")
	}
}

func TestResolveClusterKey_BothMatchNoWarn(t *testing.T) {
	dir := t.TempDir()
	key := strings.Repeat("f", 64)
	if err := transport.NewKeystore(dir).WriteCurrent(key); err != nil {
		t.Fatalf("seed disk: %v", err)
	}
	got, warn, err := ResolveClusterKey(dir, key)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got != key {
		t.Fatalf("want key, got %q", got)
	}
	if warn != "" {
		t.Fatalf("matching values should not warn, got %q", warn)
	}
}

func TestResolveClusterKey_MirrorWriteFails(t *testing.T) {
	// transport.NewKeystore writes under <dataDir>/keys.d/. If that path is a
	// regular file instead of a dir, MkdirAll fails and ResolveClusterKey
	// surfaces the error.
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "keys.d"), []byte("nope"), 0o600); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	_, _, err := ResolveClusterKey(dir, strings.Repeat("e", 64))
	if err == nil {
		t.Fatalf("expected error when keys.d cannot be created")
	}
}

func TestGenerateEphemeralClusterKey(t *testing.T) {
	a, err := GenerateEphemeralClusterKey()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(a) != 64 {
		t.Fatalf("want 64-char hex, got %d", len(a))
	}
	if _, err := hex.DecodeString(a); err != nil {
		t.Fatalf("not hex: %v", err)
	}
	b, err := GenerateEphemeralClusterKey()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if a == b {
		t.Fatalf("two calls returned same key — entropy source broken?")
	}
}
