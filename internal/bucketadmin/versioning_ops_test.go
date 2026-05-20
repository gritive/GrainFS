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

func TestRunVersioningGet_Text_Enabled(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/b/versioning", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"Enabled"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	var out bytes.Buffer
	err := RunVersioningGet(context.Background(), VersioningGetOptions{
		BaseOptions: BaseOptions{Endpoint: srv.URL, Stdout: &out},
		Bucket:      "b",
	})
	if err != nil {
		t.Fatal(err)
	}
	s := out.String()
	if !strings.Contains(s, "BUCKET") || !strings.Contains(s, "VERSIONING") {
		t.Errorf("missing header:\n%s", s)
	}
	if !strings.Contains(s, "b") || !strings.Contains(s, "Enabled") {
		t.Errorf("missing values:\n%s", s)
	}
}

func TestRunVersioningGet_Text_EmptyDefaultsToOff(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/b/versioning", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":""}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	var out bytes.Buffer
	err := RunVersioningGet(context.Background(), VersioningGetOptions{
		BaseOptions: BaseOptions{Endpoint: srv.URL, Stdout: &out},
		Bucket:      "b",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "Off") {
		t.Errorf("expected 'Off' for empty status:\n%s", out.String())
	}
}

func TestRunVersioningGet_JSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/b/versioning", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"Suspended"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	var out bytes.Buffer
	err := RunVersioningGet(context.Background(), VersioningGetOptions{
		BaseOptions: BaseOptions{Endpoint: srv.URL, Stdout: &out, JSONOut: true},
		Bucket:      "b",
	})
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(out.String())), &got); err != nil {
		t.Fatalf("output not JSON: %v\n%s", err, out.String())
	}
	if got["status"] != "Suspended" {
		t.Errorf("got %v", got)
	}
}

func TestRunVersioningEnable_BodyAndFeedback(t *testing.T) {
	var gotBody map[string]string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/b/versioning", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("method = %s, want PUT", r.Method)
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	var out bytes.Buffer
	err := RunVersioningEnable(context.Background(), VersioningEnableOptions{
		BaseOptions: BaseOptions{Endpoint: srv.URL, Stdout: &out},
		Bucket:      "b",
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotBody["status"] != "Enabled" {
		t.Errorf("body status = %q, want Enabled", gotBody["status"])
	}
	if got := out.String(); got != "Versioning enabled for bucket b\n" {
		t.Errorf("got %q", got)
	}
}

func TestRunVersioningSuspend_BodyAndFeedback(t *testing.T) {
	var gotBody map[string]string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/b/versioning", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	var out bytes.Buffer
	err := RunVersioningSuspend(context.Background(), VersioningSuspendOptions{
		BaseOptions: BaseOptions{Endpoint: srv.URL, Stdout: &out},
		Bucket:      "b",
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotBody["status"] != "Suspended" {
		t.Errorf("body status = %q", gotBody["status"])
	}
	if got := out.String(); got != "Versioning suspended for bucket b\n" {
		t.Errorf("got %q", got)
	}
}
