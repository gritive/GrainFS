package auditadmin

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// newTestServer returns a started httptest.Server backed by mux, and a
// cleanup function. Call defer stop() in the test.
func newTestServer(t *testing.T, mux *http.ServeMux) (url string, stop func()) {
	t.Helper()
	srv := httptest.NewServer(mux)
	return srv.URL, srv.Close
}

// TestRunAuditQuery_HappyPath verifies table output contains the returned value.
func TestRunAuditQuery_HappyPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/audit/query", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		var req QueryRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode body: %v", err)
		}
		if req.SQL != "SELECT 1" {
			t.Errorf("sql = %q, want SELECT 1", req.SQL)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"columns":["c"],"rows":[["v"]]}`))
	})
	url, stop := newTestServer(t, mux)
	defer stop()

	var out bytes.Buffer
	err := RunAuditQuery(context.Background(), QueryOptions{
		BaseOptions: BaseOptions{Endpoint: url, Stdout: &out},
		SQL:         "SELECT 1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out.String(), "v") {
		t.Errorf("output = %q; want to contain 'v'", out.String())
	}
}

// TestRunAuditQuery_JSONOut verifies --json flag emits valid JSON.
func TestRunAuditQuery_JSONOut(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/audit/query", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"columns":["col"],"rows":[["val"]]}`))
	})
	url, stop := newTestServer(t, mux)
	defer stop()

	var out bytes.Buffer
	err := RunAuditQuery(context.Background(), QueryOptions{
		BaseOptions: BaseOptions{Endpoint: url, Stdout: &out, JSONOut: true},
		SQL:         "SELECT 1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var got QueryResponse
	if err := json.Unmarshal([]byte(strings.TrimSpace(out.String())), &got); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, out.String())
	}
	if len(got.Columns) == 0 || got.Columns[0] != "col" {
		t.Errorf("columns = %v", got.Columns)
	}
}

// TestRunAuditQuery_NoRows verifies "(no rows)" is printed when the server
// returns an empty rows slice.
func TestRunAuditQuery_NoRows(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/audit/query", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"columns":[],"rows":[]}`))
	})
	url, stop := newTestServer(t, mux)
	defer stop()

	var out bytes.Buffer
	err := RunAuditQuery(context.Background(), QueryOptions{
		BaseOptions: BaseOptions{Endpoint: url, Stdout: &out},
		SQL:         "SELECT 1 WHERE FALSE",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out.String(), "no rows") {
		t.Errorf("output = %q; want '(no rows)'", out.String())
	}
}

// TestRunAuditQuery_ServerError verifies a 400 response yields an error.
func TestRunAuditQuery_ServerError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/audit/query", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"code":"invalid_sql","error":"bad sql"}`))
	})
	url, stop := newTestServer(t, mux)
	defer stop()

	var out bytes.Buffer
	err := RunAuditQuery(context.Background(), QueryOptions{
		BaseOptions: BaseOptions{Endpoint: url, Stdout: &out},
		SQL:         "DROP TABLE x",
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TestRunAuditRecentDenies_HappyPath verifies table output for recent-denies.
func TestRunAuditRecentDenies_HappyPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/audit/recent-denies", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"columns":["sa_id","action"],"rows":[["sa-1","s3:DeleteObject"]]}`))
	})
	url, stop := newTestServer(t, mux)
	defer stop()

	var out bytes.Buffer
	err := RunAuditRecentDenies(context.Background(), RecentDeniesOptions{
		BaseOptions: BaseOptions{Endpoint: url, Stdout: &out},
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out.String(), "sa-1") {
		t.Errorf("output = %q; want 'sa-1'", out.String())
	}
	if !strings.Contains(out.String(), "s3:DeleteObject") {
		t.Errorf("output = %q; want 's3:DeleteObject'", out.String())
	}
}

// TestRunAuditRecentDenies_ServerError verifies 503 yields an error.
func TestRunAuditRecentDenies_ServerError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/audit/recent-denies", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"code":"unavailable","error":"audit db offline"}`))
	})
	url, stop := newTestServer(t, mux)
	defer stop()

	err := RunAuditRecentDenies(context.Background(), RecentDeniesOptions{
		BaseOptions: BaseOptions{Endpoint: url, Stdout: &bytes.Buffer{}},
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TestRunAuditBySA_HappyPath verifies the SA path is included in the request URL.
func TestRunAuditBySA_HappyPath(t *testing.T) {
	const saID = "sa-abc"
	var hitPath string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/audit/by-sa/", func(w http.ResponseWriter, r *http.Request) {
		hitPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"columns":["sa_id"],"rows":[["sa-abc"]]}`))
	})
	url, stop := newTestServer(t, mux)
	defer stop()

	var out bytes.Buffer
	err := RunAuditBySA(context.Background(), BySAOptions{
		BaseOptions: BaseOptions{Endpoint: url, Stdout: &out},
		SAID:        saID,
		Limit:       5,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.HasSuffix(hitPath, "/"+saID) {
		t.Errorf("path = %q; want suffix '/%s'", hitPath, saID)
	}
	if !strings.Contains(out.String(), "sa-abc") {
		t.Errorf("output = %q; want 'sa-abc'", out.String())
	}
}

// TestRunAuditBySA_NotFound verifies 404 yields an error.
func TestRunAuditBySA_NotFound(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/audit/by-sa/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"code":"not_found","error":"sa not found"}`))
	})
	url, stop := newTestServer(t, mux)
	defer stop()

	err := RunAuditBySA(context.Background(), BySAOptions{
		BaseOptions: BaseOptions{Endpoint: url, Stdout: &bytes.Buffer{}},
		SAID:        "sa-missing",
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TestRunAuditByRequestID_HappyPath verifies the request-id path segment.
func TestRunAuditByRequestID_HappyPath(t *testing.T) {
	const rid = "req-123"
	var hitPath string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/audit/by-request-id/", func(w http.ResponseWriter, r *http.Request) {
		hitPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"columns":["request_id"],"rows":[["req-123"]]}`))
	})
	url, stop := newTestServer(t, mux)
	defer stop()

	var out bytes.Buffer
	err := RunAuditByRequestID(context.Background(), ByRequestIDOptions{
		BaseOptions: BaseOptions{Endpoint: url, Stdout: &out},
		RequestID:   rid,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.HasSuffix(hitPath, "/"+rid) {
		t.Errorf("path = %q; want suffix '/%s'", hitPath, rid)
	}
	if !strings.Contains(out.String(), rid) {
		t.Errorf("output = %q; want %q", out.String(), rid)
	}
}

// TestRunAuditByRequestID_NotFound verifies 404 yields an error.
func TestRunAuditByRequestID_NotFound(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/audit/by-request-id/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"code":"not_found","error":"request id not found"}`))
	})
	url, stop := newTestServer(t, mux)
	defer stop()

	err := RunAuditByRequestID(context.Background(), ByRequestIDOptions{
		BaseOptions: BaseOptions{Endpoint: url, Stdout: &bytes.Buffer{}},
		RequestID:   "req-missing",
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}
