package volumeadmin

import (
	"bytes"
	"context"
	"net/http/httptest"
	"strings"
	"testing"
)

// optsForServer builds BaseOptions targeting srv.URL with capture buffers.
func optsForServer(srv *httptest.Server) (BaseOptions, *bytes.Buffer, *bytes.Buffer) {
	out := &bytes.Buffer{}
	errBuf := &bytes.Buffer{}
	return BaseOptions{Endpoint: srv.URL, Stdout: out, Stderr: errBuf}, out, errBuf
}

func TestRunList_Table(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{{
		method: "GET", path: "/v1/volumes",
		body: ListVolumesResp{Volumes: []VolumeInfo{
			{Name: "v1", Size: 1 << 30, AllocatedBytes: 1 << 20, SnapshotCount: 2, Health: "ok"},
		}},
	}})
	defer srv.Close()
	base, out, _ := optsForServer(srv)
	if err := RunList(context.Background(), ListOptions{BaseOptions: base}); err != nil {
		t.Fatalf("err: %v", err)
	}
	got := out.String()
	for _, want := range []string{"NAME", "v1", "1.0 GiB", "1.0 MiB", "ok"} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}
}

func TestRunList_JSON(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{{
		method: "GET", path: "/v1/volumes",
		body: ListVolumesResp{Volumes: []VolumeInfo{{Name: "v1"}}},
	}})
	defer srv.Close()
	base, out, _ := optsForServer(srv)
	base.JSONOut = true
	if err := RunList(context.Background(), ListOptions{BaseOptions: base}); err != nil {
		t.Fatalf("err: %v", err)
	}
	if !strings.Contains(out.String(), `"name": "v1"`) {
		t.Errorf("expected JSON, got:\n%s", out.String())
	}
}

func TestRunList_Empty(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{{
		method: "GET", path: "/v1/volumes",
		body: ListVolumesResp{Volumes: nil},
	}})
	defer srv.Close()
	base, out, _ := optsForServer(srv)
	if err := RunList(context.Background(), ListOptions{BaseOptions: base}); err != nil {
		t.Fatalf("err: %v", err)
	}
	if !strings.Contains(out.String(), "(no volumes)") {
		t.Errorf("expected '(no volumes)' in:\n%s", out.String())
	}
}

func TestRunCreate_RejectsEmptyName(t *testing.T) {
	err := RunCreate(context.Background(), CreateOptions{Size: 1 << 30})
	if err == nil {
		t.Fatal("expected error for empty name")
	}
}

func TestRunCreate_RejectsZeroSize(t *testing.T) {
	err := RunCreate(context.Background(), CreateOptions{Name: "v1"})
	if err == nil {
		t.Fatal("expected error for zero size")
	}
}

func TestRunCreate_HappyPath(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{{
		method: "POST", path: "/v1/volumes",
		body: VolumeInfo{Name: "v1", Size: 1 << 30, BlockSize: 4096},
	}})
	defer srv.Close()
	base, out, _ := optsForServer(srv)
	if err := RunCreate(context.Background(), CreateOptions{BaseOptions: base, Name: "v1", Size: 1 << 30}); err != nil {
		t.Fatalf("err: %v", err)
	}
	if !strings.Contains(out.String(), `created "v1"`) {
		t.Errorf("unexpected:\n%s", out.String())
	}
}

func TestRunInfo(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{{
		method: "GET", path: "/v1/volumes/v1",
		body: VolumeInfo{Name: "v1", Size: 1 << 30, BlockSize: 4096, Health: "degraded", HealthReasons: []string{"replica_missing"}},
	}})
	defer srv.Close()
	base, out, _ := optsForServer(srv)
	if err := RunInfo(context.Background(), InfoOptions{BaseOptions: base, Name: "v1"}); err != nil {
		t.Fatalf("err: %v", err)
	}
	for _, want := range []string{"name:", "v1", "size:", "1.0 GiB", "health:", "degraded", "health_reasons:", "replica_missing"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("missing %q in:\n%s", want, out.String())
		}
	}
}

func TestRunStat(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{{
		method: "GET", path: "/v1/volumes/v1/stat",
		body: VolumeStatResp{
			Volume:          VolumeInfo{Name: "v1", Size: 1 << 30, AllocatedBytes: 100, SnapshotCount: 3, Health: "ok"},
			RecentIncidents: []map[string]any{{"id": "i-1"}, {"id": "i-2"}},
		},
	}})
	defer srv.Close()
	base, out, _ := optsForServer(srv)
	if err := RunStat(context.Background(), StatOptions{BaseOptions: base, Name: "v1"}); err != nil {
		t.Fatalf("err: %v", err)
	}
	for _, want := range []string{"volume:", "snapshots:", "recent incidents: 2"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("missing %q in:\n%s", want, out.String())
		}
	}
}

func TestRunDelete_HappyPath(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{{
		method: "DELETE", path: "/v1/volumes/v1",
		body: DeleteResp{Deleted: true},
	}})
	defer srv.Close()
	base, out, _ := optsForServer(srv)
	if err := RunDelete(context.Background(), DeleteOptions{BaseOptions: base, Name: "v1"}); err != nil {
		t.Fatalf("err: %v", err)
	}
	if !strings.Contains(out.String(), `deleted "v1"`) {
		t.Errorf("unexpected:\n%s", out.String())
	}
}

func TestRunDelete_ConflictPrintsBlockToStderr(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{{
		method: "DELETE", path: "/v1/volumes/v1",
		status: 409, errResp: &Error{
			Code: "conflict", Message: "v1 has 3 snapshots; refused without --force",
			Details: map[string]any{
				"snapshot_count":  3,
				"recent":          []map[string]any{{"id": "snap-1", "created_at": "t", "block_count": 1}},
				"cascade_command": "grainfs volume delete v1 --force",
				"list_command":    "grainfs volume snapshot list v1",
			},
		},
	}})
	defer srv.Close()
	base, _, errBuf := optsForServer(srv)
	err := RunDelete(context.Background(), DeleteOptions{BaseOptions: base, Name: "v1"})
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"refused without --force", "Recent snapshots:", "snap-1", "Cascade:", "Or list:"} {
		if !strings.Contains(errBuf.String(), want) {
			t.Errorf("stderr missing %q:\n%s", want, errBuf.String())
		}
	}
}

func TestRunResize_RejectsZeroSize(t *testing.T) {
	if err := RunResize(context.Background(), ResizeOptions{Name: "v1"}); err == nil {
		t.Fatal("expected error")
	}
}

func TestRunResize_HappyPath(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{{
		method: "POST", path: "/v1/volumes/v1/resize",
		body: ResizeResp{Name: "v1", OldSize: 1 << 30, NewSize: 2 << 30, Changed: true},
	}})
	defer srv.Close()
	base, out, _ := optsForServer(srv)
	if err := RunResize(context.Background(), ResizeOptions{BaseOptions: base, Name: "v1", Size: 2 << 30}); err != nil {
		t.Fatalf("err: %v", err)
	}
	if !strings.Contains(out.String(), "resized") {
		t.Errorf("unexpected:\n%s", out.String())
	}
}

func TestRunResize_NoChange(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{{
		method: "POST", path: "/v1/volumes/v1/resize",
		body: ResizeResp{Name: "v1", OldSize: 1 << 30, NewSize: 1 << 30, Changed: false},
	}})
	defer srv.Close()
	base, out, _ := optsForServer(srv)
	if err := RunResize(context.Background(), ResizeOptions{BaseOptions: base, Name: "v1", Size: 1 << 30}); err != nil {
		t.Fatalf("err: %v", err)
	}
	if !strings.Contains(out.String(), "no change") {
		t.Errorf("unexpected:\n%s", out.String())
	}
}

func TestRunResize_UnsupportedShrink_PrintsHint(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{{
		method: "POST", path: "/v1/volumes/v1/resize",
		status: 422, errResp: &Error{
			Code: "unsupported", Message: "shrink not supported",
			Details: map[string]any{"hint": "clone", "clone_command": "grainfs volume clone v1 <new>"},
		},
	}})
	defer srv.Close()
	base, _, errBuf := optsForServer(srv)
	err := RunResize(context.Background(), ResizeOptions{BaseOptions: base, Name: "v1", Size: 1 << 30})
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"shrink not supported", "Hint:", "Try:"} {
		if !strings.Contains(errBuf.String(), want) {
			t.Errorf("stderr missing %q:\n%s", want, errBuf.String())
		}
	}
}

func TestRunRecalculate_Fixed(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{{
		method: "POST", path: "/v1/volumes/v1/recalculate",
		body: RecalculateResp{Volume: "v1", Before: 100, After: 90, Fixed: true},
	}})
	defer srv.Close()
	base, out, _ := optsForServer(srv)
	if err := RunRecalculate(context.Background(), RecalculateOptions{BaseOptions: base, Name: "v1"}); err != nil {
		t.Fatalf("err: %v", err)
	}
	if !strings.Contains(out.String(), "fixed") {
		t.Errorf("unexpected:\n%s", out.String())
	}
}

func TestRunClone(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{{
		method: "POST", path: "/v1/volumes/clone",
	}})
	defer srv.Close()
	base, out, _ := optsForServer(srv)
	if err := RunClone(context.Background(), CloneOptions{BaseOptions: base, Src: "v1", Dst: "v2"}); err != nil {
		t.Fatalf("err: %v", err)
	}
	if !strings.Contains(out.String(), `"v1" → "v2"`) {
		t.Errorf("unexpected:\n%s", out.String())
	}
}

func TestRunRollback(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{{
		method: "POST", path: "/v1/volumes/v1/snapshots/snap-1/rollback",
	}})
	defer srv.Close()
	base, out, _ := optsForServer(srv)
	if err := RunRollback(context.Background(), RollbackOptions{BaseOptions: base, Name: "v1", SnapshotID: "snap-1"}); err != nil {
		t.Fatalf("err: %v", err)
	}
	if !strings.Contains(out.String(), "rolled back") {
		t.Errorf("unexpected:\n%s", out.String())
	}
}

func TestRunWriteAt(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{{
		method: "POST", path: "/v1/volumes/v1/write-at",
		body: WriteAtResp{Bytes: 5},
	}})
	defer srv.Close()
	base, out, _ := optsForServer(srv)
	err := RunWriteAt(context.Background(), WriteAtOptions{BaseOptions: base, Name: "v1", Offset: 0, Content: []byte("hello")})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !strings.Contains(out.String(), "wrote 5 bytes") {
		t.Errorf("unexpected:\n%s", out.String())
	}
}

func TestRunReadAt(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{{
		method: "POST", path: "/v1/volumes/v1/read-at",
		body: ReadAtResp{Data: []byte("hello")},
	}})
	defer srv.Close()
	base, out, _ := optsForServer(srv)
	err := RunReadAt(context.Background(), ReadAtOptions{BaseOptions: base, Name: "v1", Offset: 0, Length: 5})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out.String() != "hello" {
		t.Errorf("expected raw bytes, got %q", out.String())
	}
}
