package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
)

// TestBucketCreate_EndToEnd is a cobra smoke test: drives the live rootCmd
// through cobra → bucketadmin → adminapi against a fake admin UDS server.
// Wire shape, text rendering, and orchestration are covered by
// internal/bucketadmin/*_test.go; this test exists to catch breakage at the
// cmd boundary (flag wiring, command registration).
func TestBucketCreate_EndToEnd(t *testing.T) {
	var gotMethod, gotPath string
	var gotBody map[string]any

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

	var out bytes.Buffer
	rootCmd.SetArgs([]string{"bucket", "--endpoint", sock, "create", "test-bucket"})
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&out)
	rootCmd.SetContext(context.Background())
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, out.String())
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
	if _, has := gotBody["attach"]; has {
		t.Errorf("body should not have attach when --attach-* unset; got %v", gotBody)
	}
}

// TestBucketCreate_AttachWire asserts both --attach-sa and --attach-policy
// reach the wire as a nested attach object (the new bucketadmin shape).
func TestBucketCreate_AttachWire(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		fmt.Fprint(w, `{"name":"test-bucket"}`)
	})
	sock := startFakeAdminUDS(t, mux)

	t.Cleanup(func() {
		_ = bucketCreateCmd.Flags().Set("attach-sa", "")
		_ = bucketCreateCmd.Flags().Set("attach-policy", "")
		bucketCreateCmd.Flags().Lookup("attach-sa").Changed = false
		bucketCreateCmd.Flags().Lookup("attach-policy").Changed = false
	})

	var out bytes.Buffer
	rootCmd.SetArgs([]string{
		"bucket", "--endpoint", sock, "create", "test-bucket",
		"--attach-sa", "sa-bench", "--attach-policy", "bucket-admin",
	})
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&out)
	rootCmd.SetContext(context.Background())
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, out.String())
	}
	attach, ok := gotBody["attach"].(map[string]any)
	if !ok {
		t.Fatalf("body missing 'attach' map; got %v", gotBody)
	}
	if attach["sa"] != "sa-bench" || attach["policy"] != "bucket-admin" {
		t.Errorf("attach = %v, want {sa:sa-bench, policy:bucket-admin}", attach)
	}
}

// TestBucketCreate_AttachValidation verifies the mutual-requirement check
// between --attach-sa and --attach-policy fires before any HTTP call.
func TestBucketCreate_AttachValidation(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"only attach-sa", []string{"bucket", "--endpoint", "/tmp/fake.sock", "create", "b", "--attach-sa", "sa-x"}},
		{"only attach-policy", []string{"bucket", "--endpoint", "/tmp/fake.sock", "create", "b", "--attach-policy", "p"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Cleanup(func() {
				_ = bucketCreateCmd.Flags().Set("attach-sa", "")
				_ = bucketCreateCmd.Flags().Set("attach-policy", "")
				bucketCreateCmd.Flags().Lookup("attach-sa").Changed = false
				bucketCreateCmd.Flags().Lookup("attach-policy").Changed = false
			})
			var out bytes.Buffer
			rootCmd.SetArgs(c.args)
			rootCmd.SetOut(&out)
			rootCmd.SetErr(&out)
			rootCmd.SetContext(context.Background())
			err := rootCmd.Execute()
			if err == nil {
				t.Fatalf("expected mutual-requirement error; output: %s", out.String())
			}
			if !strings.Contains(err.Error(), "--attach-sa and --attach-policy must be provided together") {
				t.Errorf("error = %v, want mutual-requirement message", err)
			}
		})
	}
}

// TestBucketDelete_ForceFlag verifies --force reaches the wire as ?force=true.
func TestBucketDelete_ForceFlag(t *testing.T) {
	var gotQuery string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/del-me", func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		w.WriteHeader(http.StatusNoContent)
	})
	sock := startFakeAdminUDS(t, mux)

	t.Cleanup(func() {
		_ = bucketDeleteCmd.Flags().Set("force", "false")
		_ = bucketDeleteCmd.Flags().Set("recursive", "false")
		bucketDeleteCmd.Flags().Lookup("force").Changed = false
		bucketDeleteCmd.Flags().Lookup("recursive").Changed = false
	})

	var out bytes.Buffer
	rootCmd.SetArgs([]string{"bucket", "--endpoint", sock, "delete", "--force", "del-me"})
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&out)
	rootCmd.SetContext(context.Background())
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, out.String())
	}
	if !strings.Contains(gotQuery, "force=true") {
		t.Errorf("query = %q, want force=true", gotQuery)
	}
}
