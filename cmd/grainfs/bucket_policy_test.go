package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestCLI_PolicySet_FromStdin verifies the bytes piped to stdin reach the
// admin API policy field without re-marshalling.
func TestCLI_PolicySet_FromStdin(t *testing.T) {
	const doc = `{"Version":"2012-10-17","Statement":[]}`
	var gotMethod string
	var gotBody struct {
		Policy json.RawMessage `json:"policy"`
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/my-bucket/policy", func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusNoContent)
	})
	sock := startFakeAdminUDS(t, mux)

	var out bytes.Buffer
	rootCmd.SetArgs([]string{"bucket", "--endpoint", sock, "policy", "set", "my-bucket", "-"})
	rootCmd.SetIn(strings.NewReader(doc))
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&out)
	rootCmd.SetContext(context.Background())
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, out.String())
	}
	if gotMethod != "PUT" {
		t.Errorf("method = %q, want PUT", gotMethod)
	}
	if string(gotBody.Policy) != doc {
		t.Errorf("policy = %q, want %q", string(gotBody.Policy), doc)
	}
}

// TestCLI_PolicySet_FromFile verifies reading the policy from a file path
// argument also lands in the policy field without re-marshalling.
func TestCLI_PolicySet_FromFile(t *testing.T) {
	const doc = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow"}]}`
	var gotBody struct {
		Policy json.RawMessage `json:"policy"`
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/my-bucket/policy", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusNoContent)
	})
	sock := startFakeAdminUDS(t, mux)

	path := filepath.Join(t.TempDir(), "policy.json")
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	rootCmd.SetArgs([]string{"bucket", "--endpoint", sock, "policy", "set", "my-bucket", path})
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&out)
	rootCmd.SetContext(context.Background())
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, out.String())
	}
	if string(gotBody.Policy) != doc {
		t.Errorf("policy = %q, want %q", string(gotBody.Policy), doc)
	}
}
