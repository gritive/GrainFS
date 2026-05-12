package adminapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gritive/GrainFS/internal/adminapi"
)

func TestTransport_HTTPGet(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/x" {
			t.Fatalf("path=%s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()
	tp, err := adminapi.NewTransport(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	var out struct {
		OK bool `json:"ok"`
	}
	if err := tp.Get(context.Background(), "/v1/x", &out); err != nil {
		t.Fatal(err)
	}
	if !out.OK {
		t.Fatal("expected ok=true")
	}
}

func TestTransport_PostBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var in map[string]any
		_ = json.NewDecoder(r.Body).Decode(&in)
		if in["n"].(float64) != 7 {
			t.Fatalf("body=%v", in)
		}
		_, _ = w.Write([]byte(`{"echo":7}`))
	}))
	defer srv.Close()
	tp, _ := adminapi.NewTransport(srv.URL)
	var out struct {
		Echo int `json:"echo"`
	}
	if err := tp.Post(context.Background(), "/v1/x", map[string]int{"n": 7}, &out); err != nil {
		t.Fatal(err)
	}
	if out.Echo != 7 {
		t.Fatalf("echo=%d", out.Echo)
	}
}

func TestTransport_NonJSONErrorResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`plain text`))
	}))
	defer srv.Close()
	tp, _ := adminapi.NewTransport(srv.URL)
	err := tp.Get(context.Background(), "/x", nil)
	var ae *adminapi.Error
	if !errors.As(err, &ae) {
		t.Fatalf("expected *adminapi.Error, got %T", err)
	}
	if ae.Status != http.StatusInternalServerError {
		t.Fatalf("Status=%d", ae.Status)
	}
	if ae.Code != "internal" {
		t.Fatalf("Code=%s", ae.Code)
	}
}

func TestTransport_JSONErrorEnvelope(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"code":"conflict","error":"x","details":{"k":1}}`))
	}))
	defer srv.Close()
	tp, _ := adminapi.NewTransport(srv.URL)
	err := tp.Delete(context.Background(), "/v1/v/a", nil)
	var ae *adminapi.Error
	if !errors.As(err, &ae) {
		t.Fatalf("expected *adminapi.Error, got %T", err)
	}
	if ae.Code != "conflict" || ae.Status != http.StatusConflict {
		t.Fatalf("got %+v", ae)
	}
	if ae.Details["k"].(float64) != 1 {
		t.Fatalf("details=%v", ae.Details)
	}
}

func TestTransport_CancelledContext(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()
	tp, _ := adminapi.NewTransport(srv.URL)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := tp.Get(ctx, "/x", nil)
	var ae *adminapi.Error
	if !errors.As(err, &ae) {
		t.Fatalf("expected *adminapi.Error, got %T", err)
	}
	if ae.Code != "transient" {
		t.Fatalf("Code=%s", ae.Code)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatal("errors.Is(context.Canceled) should hold via Unwrap")
	}
}

func TestTransport_UDSDispatch_ENOENT(t *testing.T) {
	tp, err := adminapi.NewTransport("/tmp/nonexistent-grainfs-deepening.sock")
	if err != nil {
		t.Fatal(err)
	}
	err = tp.Get(context.Background(), "/x", nil)
	if err == nil {
		t.Fatal("expected dial error")
	}
	if !strings.Contains(err.Error(), "admin socket not found") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// shortTempSock returns a short-enough unix socket path. macOS limits unix
// socket paths to 104 chars, and t.TempDir() can blow past that.
func shortTempSock(t *testing.T, name string) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "ga-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, name)
}

func TestTransport_UDSDispatch_ECONNREFUSED(t *testing.T) {
	sock := shortTempSock(t, "a.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	_ = l.Close()
	// macOS removes the socket file on Close; recreate it as a regular file
	// so the dial reaches connect() and produces ECONNREFUSED-like behavior.
	if _, statErr := os.Stat(sock); os.IsNotExist(statErr) {
		if err := os.WriteFile(sock, nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	tp, err := adminapi.NewTransport(sock)
	if err != nil {
		t.Fatal(err)
	}
	err = tp.Get(context.Background(), "/x", nil)
	if err == nil {
		t.Fatal("expected dial error")
	}
	msg := err.Error()
	// Accept ECONNREFUSED (Linux: socket file kept after Close) and ENOTSOCK
	// (macOS: file recreated as regular file -> "socket operation on non-socket").
	if !strings.Contains(msg, "no listener") &&
		!strings.Contains(msg, "connection refused") &&
		!strings.Contains(msg, "non-socket") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestTransport_UDSDispatch_EACCES(t *testing.T) {
	sock := shortTempSock(t, "a.sock")
	if err := os.WriteFile(sock, nil, 0o000); err != nil {
		t.Fatal(err)
	}
	tp, err := adminapi.NewTransport(sock)
	if err != nil {
		t.Fatal(err)
	}
	err = tp.Get(context.Background(), "/x", nil)
	if err == nil {
		t.Fatal("expected dial error")
	}
	if !strings.Contains(err.Error(), "permission denied") {
		t.Skipf("running as root or perms differ: %v", err)
	}
}

func TestTransport_EmptyEndpoint_DialsAtRequestTime(t *testing.T) {
	tp, err := adminapi.NewTransport("")
	if err != nil {
		t.Fatalf("empty endpoint should not error: %v", err)
	}
	err = tp.Get(context.Background(), "/x", nil)
	if err == nil {
		t.Fatal("expected dial error on empty endpoint")
	}
	var ae *adminapi.Error
	if !errors.As(err, &ae) {
		t.Fatalf("expected *adminapi.Error, got %T", err)
	}
}
