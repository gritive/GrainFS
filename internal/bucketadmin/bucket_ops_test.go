package bucketadmin

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newSrv(t *testing.T, mux *http.ServeMux) (string, func()) {
	t.Helper()
	srv := httptest.NewServer(mux)
	return srv.URL, srv.Close
}

func TestRunCreate_Text_NoAttach(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if _, has := body["attach"]; has {
			t.Errorf("expected no attach when AttachSA empty; body=%v", body)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"name":"b","created_at":"2026-05-20T12:00:00Z"}`))
	})
	url, stop := newSrv(t, mux)
	defer stop()

	var out bytes.Buffer
	err := RunCreate(context.Background(), CreateOptions{
		BaseOptions: BaseOptions{Endpoint: url, Stdout: &out},
		Name:        "b",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != "Created bucket b\n" {
		t.Errorf("got %q", got)
	}
}

func TestRunCreate_Text_WithAttach(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"name":"b","created_at":"2026-05-20T12:00:00Z"}`))
	})
	url, stop := newSrv(t, mux)
	defer stop()

	err := RunCreate(context.Background(), CreateOptions{
		BaseOptions: BaseOptions{Endpoint: url, Stdout: &bytes.Buffer{}},
		Name:        "b",
		AttachSA:    "sa-x",
		AttachRole:  "bucket-admin",
	})
	if err != nil {
		t.Fatal(err)
	}
	attach, _ := gotBody["attach"].(map[string]any)
	if attach["sa"] != "sa-x" || attach["policy"] != "bucket-admin" {
		t.Errorf("attach = %v", attach)
	}
}

func TestRunCreate_JSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"name":"b","created_at":"2026-05-20T12:00:00Z"}`))
	})
	url, stop := newSrv(t, mux)
	defer stop()

	var out bytes.Buffer
	err := RunCreate(context.Background(), CreateOptions{
		BaseOptions: BaseOptions{Endpoint: url, Stdout: &out, JSONOut: true},
		Name:        "b",
	})
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(out.String())), &got); err != nil {
		t.Fatalf("output not JSON: %v\n%s", err, out.String())
	}
	if got["name"] != "b" {
		t.Errorf("name = %v", got["name"])
	}
}

func TestRunList_Text(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"buckets":[{"name":"b1","has_upstream":false},{"name":"b2","has_upstream":true}]}`))
	})
	url, stop := newSrv(t, mux)
	defer stop()

	var out bytes.Buffer
	err := RunList(context.Background(), ListOptions{
		BaseOptions: BaseOptions{Endpoint: url, Stdout: &out},
	})
	if err != nil {
		t.Fatal(err)
	}
	s := out.String()
	if !strings.Contains(s, "NAME") || !strings.Contains(s, "HAS_UPSTREAM") {
		t.Errorf("missing header in:\n%s", s)
	}
	if !strings.Contains(s, "b1") || !strings.Contains(s, "b2") {
		t.Errorf("missing rows in:\n%s", s)
	}
	if !strings.Contains(s, "yes") || !strings.Contains(s, "no") {
		t.Errorf("expected yes/no for has_upstream:\n%s", s)
	}
}

func TestRunInfo_Text(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/b", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"name":"b","object_count":42,"versioning":"Enabled","has_upstream":true}`))
	})
	url, stop := newSrv(t, mux)
	defer stop()

	var out bytes.Buffer
	err := RunInfo(context.Background(), InfoOptions{
		BaseOptions: BaseOptions{Endpoint: url, Stdout: &out},
		Name:        "b",
	})
	if err != nil {
		t.Fatal(err)
	}
	s := out.String()
	if !strings.Contains(s, "NAME") || !strings.Contains(s, "OBJECTS") || !strings.Contains(s, "VERSIONING") || !strings.Contains(s, "HAS_UPSTREAM") {
		t.Errorf("missing headers:\n%s", s)
	}
	if !strings.Contains(s, "42") || !strings.Contains(s, "Enabled") || !strings.Contains(s, "yes") {
		t.Errorf("missing values:\n%s", s)
	}
}

func TestRunInfo_NilObjectCount(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/b", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// object_count omitted → nil pointer
		_, _ = w.Write([]byte(`{"name":"b","versioning":"","has_upstream":false}`))
	})
	url, stop := newSrv(t, mux)
	defer stop()

	var out bytes.Buffer
	err := RunInfo(context.Background(), InfoOptions{
		BaseOptions: BaseOptions{Endpoint: url, Stdout: &out},
		Name:        "b",
	})
	if err != nil {
		t.Fatal(err)
	}
	s := out.String()
	if !strings.Contains(s, "<unknown>") {
		t.Errorf("expected '<unknown>' for nil object_count; got:\n%s", s)
	}
	if !strings.Contains(s, "-") {
		t.Errorf("expected '-' for empty versioning; got:\n%s", s)
	}
}

func TestRunDelete_QueryParams_BothFlags(t *testing.T) {
	var sawURL string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/b", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("method = %s, want DELETE", r.Method)
		}
		sawURL = r.URL.String()
		w.WriteHeader(http.StatusOK)
	})
	url, stop := newSrv(t, mux)
	defer stop()

	var out bytes.Buffer
	err := RunDelete(context.Background(), DeleteOptions{
		BaseOptions: BaseOptions{Endpoint: url, Stdout: &out},
		Name:        "b",
		Force:       true,
		Recursive:   true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sawURL, "force=true") || !strings.Contains(sawURL, "recursive=true") {
		t.Errorf("url = %s", sawURL)
	}
	if got := out.String(); got != "Deleted bucket b\n" {
		t.Errorf("got %q", got)
	}
}

func TestRunDelete_NoFlags(t *testing.T) {
	var sawURL string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/b", func(w http.ResponseWriter, r *http.Request) {
		sawURL = r.URL.String()
		w.WriteHeader(http.StatusOK)
	})
	url, stop := newSrv(t, mux)
	defer stop()

	err := RunDelete(context.Background(), DeleteOptions{
		BaseOptions: BaseOptions{Endpoint: url, Stdout: &bytes.Buffer{}},
		Name:        "b",
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sawURL, "?") {
		t.Errorf("expected no query string; url=%s", sawURL)
	}
}
