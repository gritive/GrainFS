package iamadmin

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRunKeyCreate_Passthrough(t *testing.T) {
	body := `{"access_key":"AK","secret_key":"SK"}`
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iam/sa/sa-x/key", func(w http.ResponseWriter, r *http.Request) {
		var got map[string]any
		_ = json.NewDecoder(r.Body).Decode(&got)
		if _, has := got["buckets"]; has {
			t.Errorf("expected no buckets in body when nil; got %v", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	var out bytes.Buffer
	err := RunKeyCreate(context.Background(), KeyCreateOptions{
		BaseOptions: BaseOptions{Endpoint: srv.URL, Stdout: &out},
		SAID:        "sa-x",
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.String() != body+"\n" {
		t.Errorf("got %q want %q", out.String(), body+"\n")
	}
}

func TestRunKeyCreate_WithBuckets(t *testing.T) {
	var got map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iam/sa/sa-x/key", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	err := RunKeyCreate(context.Background(), KeyCreateOptions{
		BaseOptions: BaseOptions{Endpoint: srv.URL, Stdout: &bytes.Buffer{}},
		SAID:        "sa-x",
		Buckets:     []string{"a", "b"},
	})
	if err != nil {
		t.Fatal(err)
	}
	bs, _ := got["buckets"].([]any)
	if len(bs) != 2 {
		t.Errorf("buckets = %v", got["buckets"])
	}
}

func TestRunKeyRevoke(t *testing.T) {
	var saw string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iam/sa/sa-x/key/AK", func(w http.ResponseWriter, r *http.Request) {
		saw = r.Method
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	var out bytes.Buffer
	err := RunKeyRevoke(context.Background(), KeyRevokeOptions{
		BaseOptions: BaseOptions{Endpoint: srv.URL, Stdout: &out},
		SAID:        "sa-x",
		AccessKey:   "AK",
	})
	if err != nil {
		t.Fatal(err)
	}
	if saw != http.MethodDelete {
		t.Errorf("method = %q", saw)
	}
	if out.String() != "" {
		t.Errorf("revoke should produce no stdout on success, got %q", out.String())
	}
}
