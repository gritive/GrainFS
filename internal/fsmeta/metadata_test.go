package fsmeta

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
)

func newLocalBackendForTest(t *testing.T) storage.Backend {
	t.Helper()
	backend, err := storage.NewLocalBackend(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	return backend
}

func TestSaveSymlinkRoundTrip(t *testing.T) {
	ctx := context.Background()
	backend := newLocalBackendForTest(t)
	if err := backend.CreateBucket(ctx, "bkt"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	if err := SaveSymlink(ctx, backend, "bkt", "link.txt", "../target.txt", 0777, 123); err != nil {
		t.Fatalf("SaveSymlink: %v", err)
	}
	meta, err := LoadStrict(ctx, backend, "bkt", "link.txt")

	if err != nil {
		t.Fatalf("LoadStrict: %v", err)
	}
	if meta.Kind != KindSymlink {
		t.Fatalf("Kind = %q, want %q", meta.Kind, KindSymlink)
	}
	if meta.Target != "../target.txt" {
		t.Fatalf("Target = %q, want %q", meta.Target, "../target.txt")
	}
	if meta.Mode != 0777 {
		t.Fatalf("Mode = %#o, want %#o", meta.Mode, uint32(0777))
	}
	if meta.Mtime != 123 {
		t.Fatalf("Mtime = %d, want %d", meta.Mtime, int64(123))
	}
	if !meta.IsSymlink() {
		t.Fatal("IsSymlink = false, want true")
	}
}

func TestLoadStrictRejectsSymlinkWithoutTarget(t *testing.T) {
	ctx := context.Background()
	backend := newLocalBackendForTest(t)
	if err := backend.CreateBucket(ctx, "bkt"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	raw, err := json.Marshal(FileMeta{Mode: 0777, Kind: KindSymlink})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	_, err = backend.PutObject(ctx, "bkt", SidecarKey("link.txt"), strings.NewReader(string(raw)), "application/json")
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	meta, err := LoadStrict(ctx, backend, "bkt", "link.txt")

	if err == nil {
		t.Fatal("LoadStrict error = nil, want non-nil")
	}
	if meta != (FileMeta{Mode: 0644}) {
		t.Fatalf("meta = %+v, want default regular mode", meta)
	}
}

func TestLoadDefaultsRegularMode(t *testing.T) {
	ctx := context.Background()
	backend := newLocalBackendForTest(t)
	if err := backend.CreateBucket(ctx, "bkt"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	meta, err := LoadStrict(ctx, backend, "bkt", "missing.txt")
	if err != nil {
		t.Fatalf("LoadStrict missing: %v", err)
	}
	if meta != (FileMeta{Mode: 0644}) {
		t.Fatalf("missing meta = %+v, want default regular mode", meta)
	}

	_, err = backend.PutObject(ctx, "bkt", SidecarKey("zero.txt"), strings.NewReader(`{"mode":0}`), "application/json")
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if meta := Load(ctx, backend, "bkt", "zero.txt"); meta != (FileMeta{Mode: 0644}) {
		t.Fatalf("zero mode meta = %+v, want default regular mode", meta)
	}
}

func TestReservedNamespace(t *testing.T) {
	if !IsReservedName("__meta") {
		t.Fatal(`IsReservedName("__meta") = false, want true`)
	}
	if IsReservedName("__metadata") {
		t.Fatal(`IsReservedName("__metadata") = true, want false`)
	}

	if !IsReservedKey("__meta") {
		t.Fatal(`IsReservedKey("__meta") = false, want true`)
	}
	if !IsReservedKey("__meta/file.txt") {
		t.Fatal(`IsReservedKey("__meta/file.txt") = false, want true`)
	}
	if !IsReservedKey("__meta/dir/file.txt") {
		t.Fatal(`IsReservedKey("__meta/dir/file.txt") = false, want true`)
	}
	if IsReservedKey("__metadata/file.txt") {
		t.Fatal(`IsReservedKey("__metadata/file.txt") = true, want false`)
	}
	if IsReservedKey("dir/__meta/file.txt") {
		t.Fatal(`IsReservedKey("dir/__meta/file.txt") = true, want false`)
	}

	if got := SidecarKey("dir/file.txt"); got != "__meta/dir/file.txt" {
		t.Fatalf("SidecarKey = %q, want %q", got, "__meta/dir/file.txt")
	}
}
