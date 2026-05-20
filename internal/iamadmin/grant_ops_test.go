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

func TestRunGrantPut(t *testing.T) {
	var got map[string]string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iam/grant", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("method = %s", r.Method)
		}
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	err := RunGrantPut(context.Background(), GrantPutOptions{
		BaseOptions: BaseOptions{Endpoint: srv.URL, Stdout: &bytes.Buffer{}},
		SAID:        "sa-x", Bucket: "b", Role: "Write",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got["sa_id"] != "sa-x" || got["bucket"] != "b" || got["role"] != "Write" {
		t.Errorf("body = %v", got)
	}
}

func TestRunGrantDelete(t *testing.T) {
	var method string
	var got map[string]string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iam/grant", func(w http.ResponseWriter, r *http.Request) {
		method = r.Method
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	err := RunGrantDelete(context.Background(), GrantDeleteOptions{
		BaseOptions: BaseOptions{Endpoint: srv.URL, Stdout: &bytes.Buffer{}},
		SAID:        "sa-x", Bucket: "b",
	})
	if err != nil {
		t.Fatal(err)
	}
	if method != http.MethodDelete {
		t.Errorf("method = %s", method)
	}
	if got["sa_id"] != "sa-x" || got["bucket"] != "b" {
		t.Errorf("body = %v", got)
	}
}

func TestRunGrantList_QueryAndPassthrough(t *testing.T) {
	var sawURL string
	body := `[{"sa_id":"sa-x","bucket":"b","role":"Read"}]`
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iam/grant", func(w http.ResponseWriter, r *http.Request) {
		sawURL = r.URL.String()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	var out bytes.Buffer
	err := RunGrantList(context.Background(), GrantListOptions{
		BaseOptions:  BaseOptions{Endpoint: srv.URL, Stdout: &out},
		SAFilter:     "sa-x",
		BucketFilter: "b",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sawURL, "sa=sa-x") || !strings.Contains(sawURL, "bucket=b") {
		t.Errorf("url = %q", sawURL)
	}
	if out.String() != body+"\n" {
		t.Errorf("output = %q", out.String())
	}
}
