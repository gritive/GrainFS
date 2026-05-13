package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// buildTestBucketRoot returns a fresh root+bucket command tree (no globals touched).
func buildTestBucketRoot() *cobra.Command {
	root := &cobra.Command{Use: "grainfs"}
	bkt := &cobra.Command{Use: "bucket"}
	bkt.PersistentFlags().String("endpoint", "", "")
	bkt.AddCommand(bucketCreateCmd(), bucketListCmd(), bucketDeleteCmd())
	root.AddCommand(bkt)
	return root
}

func TestBucketCreateCmd(t *testing.T) {
	var gotMethod, gotPath string
	var gotBody map[string]string

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets", func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		fmt.Fprint(w, `{"name":"test-bucket"}`)
	})
	sock := startFakeAdminUDS(t, mux)

	root := buildTestBucketRoot()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetErr(&buf)
	root.SetContext(context.Background())
	root.SetArgs([]string{"bucket", "--endpoint", sock, "create", "test-bucket"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, buf.String())
	}
	if gotMethod != "POST" {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/v1/buckets" {
		t.Errorf("path = %q, want /v1/buckets", gotPath)
	}
	if gotBody["name"] != "test-bucket" {
		t.Errorf("body name = %q, want test-bucket", gotBody["name"])
	}
	if !strings.Contains(buf.String(), "test-bucket") {
		t.Errorf("output %q should contain test-bucket", buf.String())
	}
}

func TestBucketListCmd(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"buckets":[{"name":"alpha"},{"name":"beta"}]}`)
	})
	sock := startFakeAdminUDS(t, mux)

	root := buildTestBucketRoot()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetErr(&buf)
	root.SetContext(context.Background())
	root.SetArgs([]string{"bucket", "--endpoint", sock, "list"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, buf.String())
	}
	if !strings.Contains(buf.String(), "alpha") || !strings.Contains(buf.String(), "beta") {
		t.Errorf("output %q should contain alpha and beta", buf.String())
	}
}

func TestBucketDeleteCmd(t *testing.T) {
	var gotMethod, gotPath string

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/del-me", func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusNoContent)
	})
	sock := startFakeAdminUDS(t, mux)

	root := buildTestBucketRoot()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetErr(&buf)
	root.SetContext(context.Background())
	root.SetArgs([]string{"bucket", "--endpoint", sock, "delete", "del-me"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, buf.String())
	}
	if gotMethod != "DELETE" {
		t.Errorf("method = %q, want DELETE", gotMethod)
	}
	if gotPath != "/v1/buckets/del-me" {
		t.Errorf("path = %q, want /v1/buckets/del-me", gotPath)
	}
}

func TestBucketDeleteCmd_Force(t *testing.T) {
	var gotQuery string

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/del-me", func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		w.WriteHeader(http.StatusNoContent)
	})
	sock := startFakeAdminUDS(t, mux)

	root := buildTestBucketRoot()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetErr(&buf)
	root.SetContext(context.Background())
	root.SetArgs([]string{"bucket", "--endpoint", sock, "delete", "--force", "del-me"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, buf.String())
	}
	if gotQuery != "force=true" {
		t.Errorf("query = %q, want force=true", gotQuery)
	}
}
