package iamadmin

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

func TestRunSACreate_Text(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iam/sa", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"sa_id":"sa-x","name":"alice","access_key":"AK","secret_key":"SK"}`))
	})
	url, stop := newSrv(t, mux)
	defer stop()

	var out bytes.Buffer
	err := RunSACreate(context.Background(), SACreateOptions{
		BaseOptions: BaseOptions{Endpoint: url, Stdout: &out},
		Name:        "alice",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "Created service account alice") {
		t.Errorf("output:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "secret_key:") {
		t.Errorf("missing secret_key line; output:\n%s", out.String())
	}
}

func TestRunSACreate_JSON(t *testing.T) {
	body := `{"sa_id":"sa-x","name":"alice","access_key":"AK","secret_key":"SK"}`
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iam/sa", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	})
	url, stop := newSrv(t, mux)
	defer stop()

	var out bytes.Buffer
	err := RunSACreate(context.Background(), SACreateOptions{
		BaseOptions: BaseOptions{Endpoint: url, Stdout: &out, JSONOut: true},
		Name:        "alice",
	})
	if err != nil {
		t.Fatal(err)
	}
	// JSON mode: re-marshalled. Decode both sides and compare structurally.
	var got, want map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(out.String())), &got); err != nil {
		t.Fatalf("output is not json: %v\n%s", err, out.String())
	}
	_ = json.Unmarshal([]byte(body), &want)
	if got["sa_id"] != want["sa_id"] || got["access_key"] != want["access_key"] {
		t.Errorf("got %v want %v", got, want)
	}
}

func TestRunSAList_Text(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iam/sa", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"sa_id":"sa-1","name":"alice","description":"","created_at":"2026-05-20T12:00:00Z","num_keys":1,"num_grants":0}]`))
	})
	url, stop := newSrv(t, mux)
	defer stop()

	var out bytes.Buffer
	err := RunSAList(context.Background(), SAListOptions{
		BaseOptions: BaseOptions{Endpoint: url, Stdout: &out},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "SA ID") || !strings.Contains(out.String(), "alice") {
		t.Errorf("output:\n%s", out.String())
	}
}

func TestRunSAGet_Text(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iam/sa/sa-x", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"sa_id":"sa-x","name":"alice","description":"","created_at":"2026-05-20T12:00:00Z","created_by":""}`))
	})
	url, stop := newSrv(t, mux)
	defer stop()

	var out bytes.Buffer
	err := RunSAGet(context.Background(), SAGetOptions{
		BaseOptions: BaseOptions{Endpoint: url, Stdout: &out},
		SAID:        "sa-x",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "sa-x") || !strings.Contains(out.String(), "alice") {
		t.Errorf("output:\n%s", out.String())
	}
}

func TestRunSADelete_Feedback(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iam/sa/sa-x", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	url, stop := newSrv(t, mux)
	defer stop()

	var out bytes.Buffer
	err := RunSADelete(context.Background(), SADeleteOptions{
		BaseOptions: BaseOptions{Endpoint: url, Stdout: &out},
		SAID:        "sa-x",
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.String() != "Deleted service account sa-x\n" {
		t.Errorf("got %q", out.String())
	}
}
