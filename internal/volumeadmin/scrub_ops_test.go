package volumeadmin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

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
