package volumeadmin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunScrub_DetachPrintsTriggeredAndExits(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{{
		method: "POST", path: "/v1/volumes/v1/scrub",
		body: ScrubTriggerResp{SessionID: "sess-1", Created: true},
	}})
	defer srv.Close()
	base, out, _ := optsForServer(srv)
	err := RunScrub(context.Background(), ScrubOptions{
		BaseOptions: base, Name: "v1", Scope: "full", Detach: true,
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !strings.Contains(out.String(), "Triggered scrub") {
		t.Errorf("expected trigger line:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "(created)") {
		t.Errorf("expected (created):\n%s", out.String())
	}
}

func TestRunScrub_JSONShortCircuits(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{{
		method: "POST", path: "/v1/volumes/v1/scrub",
		body: ScrubTriggerResp{SessionID: "sess-1", Created: true},
	}})
	defer srv.Close()
	base, out, _ := optsForServer(srv)
	base.JSONOut = true
	err := RunScrub(context.Background(), ScrubOptions{BaseOptions: base, Name: "v1"})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !strings.Contains(out.String(), `"session_id": "sess-1"`) {
		t.Errorf("expected JSON:\n%s", out.String())
	}
}

func TestRunScrub_FollowReachesDone(t *testing.T) {
	// Returns "running" then "done" on subsequent GETs.
	var calls atomic.Int32
	statusBody := func() ScrubJobInfo {
		n := calls.Add(1)
		if n < 2 {
			return ScrubJobInfo{SessionID: "sess-1", Status: "running"}
		}
		return ScrubJobInfo{
			SessionID: "sess-1", Status: "done",
			Checked: 100, Healthy: 95, Detected: 5, Repaired: 4, Unrepairable: 1,
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/volumes/v1/scrub", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"session_id":"sess-1","created":true}`))
	})
	mux.HandleFunc("/v1/scrub/jobs/sess-1", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		j := statusBody()
		_ = json.NewEncoder(w).Encode(j)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	base, out, _ := optsForServer(srv)
	err := RunScrub(context.Background(), ScrubOptions{
		BaseOptions: base, Name: "v1", Scope: "full",
		PollInterval: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	for _, want := range []string{"Triggered scrub", "Done. Checked=100", "Detected=5", "Repaired=4", "Unrepairable=1"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("missing %q:\n%s", want, out.String())
		}
	}
}

func TestRunScrub_FollowGracefulOnContextCancel(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/volumes/v1/scrub", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"session_id":"sess-1","created":true}`))
	})
	mux.HandleFunc("/v1/scrub/jobs/sess-1", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"session_id":"sess-1","status":"running"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	base, out, _ := optsForServer(srv)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	err := RunScrub(ctx, ScrubOptions{
		BaseOptions: base, Name: "v1", Scope: "full",
		PollInterval: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !strings.Contains(out.String(), "Follow stopped") {
		t.Errorf("expected graceful stop:\n%s", out.String())
	}
}

func TestFollowScrubSession_NetworkErrorBubblesUp(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/scrub/jobs/sess-1", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(&Error{Code: "internal", Message: "boom"})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewClientForURL(srv.URL)
	out := &strings.Builder{}
	err := FollowScrubSession(context.Background(), c, out, "sess-1", 5*time.Millisecond)
	if err == nil {
		t.Fatal("expected error from poll, got nil")
	}
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("want *Error, got %T (%v)", err, err)
	}
	if e.Code != "internal" || e.Status != http.StatusInternalServerError {
		t.Errorf("unexpected: %+v", e)
	}
	if strings.Contains(out.String(), "Follow stopped") {
		t.Errorf("must not print graceful-stop on non-ctx error:\n%s", out.String())
	}
}

func TestRunScrubStatus_ShowsPartialPeerFailures(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{{
		method: "GET", path: "/v1/scrub/jobs/sess-1",
		body: ScrubJobInfo{
			SessionID: "sess-1", Status: "running", Scope: "full",
			Checked: 10, Partial: true, PeerFailures: []string{"n2"},
		},
	}})
	defer srv.Close()
	base, out, _ := optsForServer(srv)
	if err := RunScrubStatus(context.Background(), ScrubStatusOptions{BaseOptions: base, SessionID: "sess-1"}); err != nil {
		t.Fatalf("err: %v", err)
	}
	for _, want := range []string{"Session: sess-1", "Status:", "Partial: yes", "n2"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("missing %q:\n%s", want, out.String())
		}
	}
}

func TestRunScrubList_Empty(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{{
		method: "GET", path: "/v1/scrub/jobs",
		body: ListScrubJobsResp{},
	}})
	defer srv.Close()
	base, out, _ := optsForServer(srv)
	if err := RunScrubList(context.Background(), ScrubListOptions{BaseOptions: base}); err != nil {
		t.Fatalf("err: %v", err)
	}
	if !strings.Contains(out.String(), "(no scrub sessions)") {
		t.Errorf("unexpected:\n%s", out.String())
	}
}

func TestRunScrubList_Table(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{{
		method: "GET", path: "/v1/scrub/jobs",
		body: ListScrubJobsResp{Jobs: []ScrubJobInfo{
			{SessionID: "sess-1", Status: "running", Scope: "full", Checked: 10, Detected: 1, Repaired: 0},
		}},
	}})
	defer srv.Close()
	base, out, _ := optsForServer(srv)
	if err := RunScrubList(context.Background(), ScrubListOptions{BaseOptions: base}); err != nil {
		t.Fatalf("err: %v", err)
	}
	for _, want := range []string{"SESSION", "STATUS", "sess-1", "running"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("missing %q:\n%s", want, out.String())
		}
	}
}

func TestRunScrubCancel(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{{
		method: "DELETE", path: "/v1/scrub/jobs/sess-1",
	}})
	defer srv.Close()
	base, out, _ := optsForServer(srv)
	if err := RunScrubCancel(context.Background(), ScrubCancelOptions{BaseOptions: base, SessionID: "sess-1"}); err != nil {
		t.Fatalf("err: %v", err)
	}
	if !strings.Contains(out.String(), "Cancelled sess-1") {
		t.Errorf("unexpected:\n%s", out.String())
	}
}
