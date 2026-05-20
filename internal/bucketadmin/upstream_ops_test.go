package bucketadmin

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRunUpstreamPut_BodyShape(t *testing.T) {
	var gotBody map[string]string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/upstreams", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("method = %s, want PUT", r.Method)
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	err := RunUpstreamPut(context.Background(), UpstreamPutOptions{
		BaseOptions:  BaseOptions{Endpoint: srv.URL, Stdout: &bytes.Buffer{}},
		Bucket:       "b",
		Scheme:       "s3",
		Endpoint:     "https://s3.example.com",
		AccessKey:    "AK",
		SecretKey:    "SK",
		Region:       "us-east-1",
		RemoteBucket: "rb",
	})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"bucket":        "b",
		"scheme":        "s3",
		"upstream_url":  "https://s3.example.com",
		"access_key":    "AK",
		"secret_key":    "SK",
		"region":        "us-east-1",
		"remote_bucket": "rb",
	}
	for k, v := range want {
		if gotBody[k] != v {
			t.Errorf("body[%s] = %q, want %q", k, gotBody[k], v)
		}
	}
}

func TestRunUpstreamPut_NoStdout(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/upstreams", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	var out bytes.Buffer
	err := RunUpstreamPut(context.Background(), UpstreamPutOptions{
		BaseOptions: BaseOptions{Endpoint: srv.URL, Stdout: &out},
		Bucket:      "b",
		Scheme:      "s3",
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.String() != "" {
		t.Errorf("expected no stdout on success, got %q", out.String())
	}
}

func TestRunUpstreamGet_Passthrough(t *testing.T) {
	body := `{"bucket":"b","scheme":"s3","endpoint":"x","access_key":"AK"}`
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/b/upstream", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	var out bytes.Buffer
	err := RunUpstreamGet(context.Background(), UpstreamGetOptions{
		BaseOptions: BaseOptions{Endpoint: srv.URL, Stdout: &out},
		Bucket:      "b",
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.String() != body+"\n" {
		t.Errorf("got %q want %q", out.String(), body+"\n")
	}
}

func TestRunUpstreamList_Passthrough(t *testing.T) {
	body := `[{"bucket":"b1"},{"bucket":"b2"}]`
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/upstreams", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	var out bytes.Buffer
	err := RunUpstreamList(context.Background(), UpstreamListOptions{
		BaseOptions: BaseOptions{Endpoint: srv.URL, Stdout: &out},
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.String() != body+"\n" {
		t.Errorf("got %q want %q", out.String(), body+"\n")
	}
}

func TestRunUpstreamDelete_NoStdout(t *testing.T) {
	var sawMethod string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/b/upstream", func(w http.ResponseWriter, r *http.Request) {
		sawMethod = r.Method
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	var out bytes.Buffer
	err := RunUpstreamDelete(context.Background(), UpstreamDeleteOptions{
		BaseOptions: BaseOptions{Endpoint: srv.URL, Stdout: &out},
		Bucket:      "b",
	})
	if err != nil {
		t.Fatal(err)
	}
	if sawMethod != http.MethodDelete {
		t.Errorf("method = %s", sawMethod)
	}
	if out.String() != "" {
		t.Errorf("expected no stdout, got %q", out.String())
	}
}
