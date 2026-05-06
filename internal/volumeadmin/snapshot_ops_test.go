package volumeadmin

import (
	"context"
	"strings"
	"testing"
)

func TestRunSnapshotCreate(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{{
		method: "POST", path: "/v1/volumes/v1/snapshots",
		body: SnapshotCreateResp{ID: "snap-1", BlockCount: 7},
	}})
	defer srv.Close()
	base, out, _ := optsForServer(srv)
	if err := RunSnapshotCreate(context.Background(), SnapshotCreateOptions{BaseOptions: base, Volume: "v1"}); err != nil {
		t.Fatalf("err: %v", err)
	}
	if !strings.Contains(out.String(), `snapshot "snap-1" created`) {
		t.Errorf("unexpected:\n%s", out.String())
	}
}

func TestRunSnapshotList_Empty(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{{
		method: "GET", path: "/v1/volumes/v1/snapshots",
		body: []SnapshotInfo{},
	}})
	defer srv.Close()
	base, out, _ := optsForServer(srv)
	if err := RunSnapshotList(context.Background(), SnapshotListOptions{BaseOptions: base, Volume: "v1"}); err != nil {
		t.Fatalf("err: %v", err)
	}
	if !strings.Contains(out.String(), "no snapshots") {
		t.Errorf("unexpected:\n%s", out.String())
	}
}

func TestRunSnapshotList_Table(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{{
		method: "GET", path: "/v1/volumes/v1/snapshots",
		body: []SnapshotInfo{{ID: "snap-1", CreatedAt: "2026-01-01T00:00:00Z", BlockCount: 42}},
	}})
	defer srv.Close()
	base, out, _ := optsForServer(srv)
	if err := RunSnapshotList(context.Background(), SnapshotListOptions{BaseOptions: base, Volume: "v1"}); err != nil {
		t.Fatalf("err: %v", err)
	}
	for _, want := range []string{"ID", "CREATED AT", "BLOCKS", "snap-1", "42"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("missing %q:\n%s", want, out.String())
		}
	}
}

func TestRunSnapshotDelete(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{{
		method: "DELETE", path: "/v1/volumes/v1/snapshots/snap-1",
	}})
	defer srv.Close()
	base, out, _ := optsForServer(srv)
	if err := RunSnapshotDelete(context.Background(), SnapshotDeleteOptions{BaseOptions: base, Volume: "v1", SnapshotID: "snap-1"}); err != nil {
		t.Fatalf("err: %v", err)
	}
	if !strings.Contains(out.String(), `snapshot "snap-1" deleted from "v1"`) {
		t.Errorf("unexpected:\n%s", out.String())
	}
}
