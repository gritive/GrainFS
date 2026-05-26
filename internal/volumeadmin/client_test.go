package volumeadmin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// fakeRoute returns a handler that asserts method+path and returns one of
// (json body, status, error envelope).
type fakeRoute struct {
	method  string
	path    string
	status  int
	body    any    // marshalled to JSON if non-nil
	errResp *Error // marshalled instead of body when set
	verify  func(*http.Request)
}

func newFakeServer(t *testing.T, routes []fakeRoute) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	registered := map[string]bool{}
	for _, r := range routes {
		if registered[r.path] {
			continue
		}
		registered[r.path] = true
		mux.HandleFunc(r.path, func(w http.ResponseWriter, req *http.Request) {
			for _, rt := range routes {
				if rt.path != req.URL.Path || rt.method != req.Method {
					continue
				}
				if rt.verify != nil {
					rt.verify(req)
				}
				w.Header().Set("Content-Type", "application/json")
				status := rt.status
				if status == 0 {
					status = 200
				}
				w.WriteHeader(status)
				if rt.errResp != nil {
					_ = json.NewEncoder(w).Encode(rt.errResp)
					return
				}
				if rt.body != nil {
					_ = json.NewEncoder(w).Encode(rt.body)
				}
				return
			}
			http.NotFound(w, req)
		})
	}
	return httptest.NewServer(mux)
}

func TestClient_ListVolumes(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{{
		method: "GET", path: "/v1/volumes",
		body: ListVolumesResp{Volumes: []VolumeInfo{{Name: "v1", Size: 1 << 30}}},
	}})
	defer srv.Close()
	c := NewClientForURL(srv.URL)
	resp, err := c.ListVolumes(context.Background())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(resp.Volumes) != 1 || resp.Volumes[0].Name != "v1" {
		t.Errorf("unexpected: %+v", resp)
	}
}

func TestClient_CreateVolume_RoundTripsBody(t *testing.T) {
	var captured CreateVolumeReq
	srv := newFakeServer(t, []fakeRoute{{
		method: "POST", path: "/v1/volumes",
		body: VolumeInfo{Name: "v1", Size: 1 << 30},
		verify: func(r *http.Request) {
			_ = json.NewDecoder(r.Body).Decode(&captured)
		},
	}})
	defer srv.Close()
	c := NewClientForURL(srv.URL)
	got, err := c.CreateVolume(context.Background(), CreateVolumeReq{Name: "v1", Size: 1 << 30})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if captured.Name != "v1" || captured.Size != 1<<30 {
		t.Errorf("server did not receive body: %+v", captured)
	}
	if got.Name != "v1" {
		t.Errorf("unexpected response: %+v", got)
	}
}

func TestClient_GetVolume_NotFound(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{{
		method: "GET", path: "/v1/volumes/missing",
		status: 404, errResp: &Error{Code: "not_found", Message: "missing"},
	}})
	defer srv.Close()
	c := NewClientForURL(srv.URL)
	_, err := c.GetVolume(context.Background(), "missing")
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("expected *Error, got %T (%v)", err, err)
	}
	if e.Code != "not_found" || e.Status != 404 {
		t.Errorf("unexpected: %+v", e)
	}
}

func TestClient_DeleteVolume(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{{
		method: "DELETE", path: "/v1/volumes/v1",
		body: DeleteResp{Deleted: true},
	}})
	defer srv.Close()
	c := NewClientForURL(srv.URL)
	resp, err := c.DeleteVolume(context.Background(), "v1")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !resp.Deleted {
		t.Errorf("expected Deleted=true, got %+v", resp)
	}
}

func TestClient_ResizeVolume_UnsupportedShrink(t *testing.T) {
	unsupportedDetails, _ := json.Marshal(map[string]any{
		"current_size": 2 << 30,
		"requested":    1 << 30,
		"hint":         "create a smaller new volume and copy the data instead",
	})
	srv := newFakeServer(t, []fakeRoute{{
		method: "POST", path: "/v1/volumes/v1/resize",
		status: 422, errResp: &Error{
			Code: "unsupported", Message: "shrink not supported",
			Details: unsupportedDetails,
		},
	}})
	defer srv.Close()
	c := NewClientForURL(srv.URL)
	_, err := c.ResizeVolume(context.Background(), "v1", 1<<30)
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("expected *Error: %v", err)
	}
	d := AsResizeUnsupported(e)
	if d == nil || d.Hint == "" {
		t.Errorf("typed details missing: %+v", d)
	}
}

func TestClient_RecalculateVolume(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{{
		method: "POST", path: "/v1/volumes/v1/recalculate",
		body: RecalculateResp{Volume: "v1", Before: 100, After: 90, Fixed: true},
	}})
	defer srv.Close()
	c := NewClientForURL(srv.URL)
	resp, err := c.RecalculateVolume(context.Background(), "v1")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !resp.Fixed || resp.Before != 100 || resp.After != 90 {
		t.Errorf("unexpected: %+v", resp)
	}
}

func TestClient_WriteAt_ReadAt(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{
		{method: "POST", path: "/v1/volumes/v1/write-at", body: WriteAtResp{Bytes: 5}},
		{method: "POST", path: "/v1/volumes/v1/read-at", body: ReadAtResp{Data: []byte("hello")}},
	})
	defer srv.Close()
	c := NewClientForURL(srv.URL)
	w, err := c.WriteAtVolume(context.Background(), "v1", 0, []byte("hello"))
	if err != nil || w.Bytes != 5 {
		t.Errorf("write: %+v err=%v", w, err)
	}
	r, err := c.ReadAtVolume(context.Background(), "v1", 0, 5)
	if err != nil || string(r.Data) != "hello" {
		t.Errorf("read: %q err=%v", r.Data, err)
	}
}

func TestClient_Scrub_Lifecycle(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{
		{method: "POST", path: "/v1/volumes/v1/scrub", body: ScrubTriggerResp{SessionID: "sess-1", Created: true}},
		{method: "GET", path: "/v1/scrub/jobs/sess-1", body: ScrubJobInfo{SessionID: "sess-1", Status: "running"}},
		{method: "GET", path: "/v1/scrub/jobs", body: ListScrubJobsResp{Jobs: []ScrubJobInfo{{SessionID: "sess-1"}}}},
		{method: "DELETE", path: "/v1/scrub/jobs/sess-1"},
	})
	defer srv.Close()
	c := NewClientForURL(srv.URL)
	tr, err := c.TriggerScrub(context.Background(), ScrubTriggerReq{Name: "v1", Scope: "full"})
	if err != nil || tr.SessionID != "sess-1" {
		t.Errorf("trigger: %+v err=%v", tr, err)
	}
	job, err := c.GetScrubJob(context.Background(), "sess-1")
	if err != nil || job.Status != "running" {
		t.Errorf("get: %+v err=%v", job, err)
	}
	jobs, err := c.ListScrubJobs(context.Background())
	if err != nil || len(jobs.Jobs) != 1 {
		t.Errorf("list: %+v err=%v", jobs, err)
	}
	if err := c.CancelScrub(context.Background(), "sess-1"); err != nil {
		t.Errorf("cancel: %v", err)
	}
}

func TestClient_Do_MalformedJSONOn2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{not json"))
	}))
	defer srv.Close()
	c := NewClientForURL(srv.URL)
	_, err := c.ListVolumes(context.Background())
	if err == nil {
		t.Fatal("expected decode error, got nil")
	}
	if !strings.Contains(err.Error(), "decode response") {
		t.Errorf("err=%q, want substring %q", err.Error(), "decode response")
	}
}

func TestClient_TransientOnDial(t *testing.T) {
	c := NewClientForURL("http://127.0.0.1:1") // refused
	_, err := c.ListVolumes(context.Background())
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("want *Error, got %T (%v)", err, err)
	}
	if e.Code != "transient" {
		t.Errorf("code=%q want transient", e.Code)
	}
}

func TestClient_TransientOnContextCancel(t *testing.T) {
	// Server sleeps long enough that ctx-cancel always wins.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()
	c := NewClientForURL(srv.URL)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel
	_, err := c.ListVolumes(ctx)
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("want *Error envelope, got %T (%v)", err, err)
	}
	if e.Code != "transient" {
		t.Errorf("code=%q want transient", e.Code)
	}
	// Unwrap must still expose the underlying context error.
	if !errors.Is(err, context.Canceled) {
		t.Errorf("errors.Is(err, context.Canceled) = false; want true (Unwrap broken)")
	}
}
