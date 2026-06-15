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

func TestClient_Scrub_GetAndList(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{
		{method: "GET", path: "/v1/scrub/jobs/sess-1", body: ScrubJobInfo{SessionID: "sess-1", Status: "running"}},
		{method: "GET", path: "/v1/scrub/jobs", body: ListScrubJobsResp{Jobs: []ScrubJobInfo{{SessionID: "sess-1"}}}},
	})
	defer srv.Close()
	c := NewClientForURL(srv.URL)
	job, err := c.GetScrubJob(context.Background(), "sess-1")
	if err != nil || job.Status != "running" {
		t.Errorf("get: %+v err=%v", job, err)
	}
	jobs, err := c.ListScrubJobs(context.Background())
	if err != nil || len(jobs.Jobs) != 1 {
		t.Errorf("list: %+v err=%v", jobs, err)
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
	_, err := c.ListScrubJobs(context.Background())
	if err == nil {
		t.Fatal("expected decode error, got nil")
	}
	if !strings.Contains(err.Error(), "decode response") {
		t.Errorf("err=%q, want substring %q", err.Error(), "decode response")
	}
}

func TestClient_TransientOnDial(t *testing.T) {
	c := NewClientForURL("http://127.0.0.1:1") // refused
	_, err := c.ListScrubJobs(context.Background())
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
	_, err := c.ListScrubJobs(ctx)
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
