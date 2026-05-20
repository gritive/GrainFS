package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
)

// TestCLI_VersioningEnable_EndToEnd is a cobra smoke test: drives the live
// rootCmd through `bucket versioning enable` and asserts the server sees
// {"status":"Enabled"}. Wire shape and Suspend variant are covered by
// internal/bucketadmin/versioning_ops_test.go.
func TestCLI_VersioningEnable_EndToEnd(t *testing.T) {
	var gotMethod string
	var gotBody map[string]string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/my-bucket/versioning", func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusNoContent)
	})
	sock := startFakeAdminUDS(t, mux)

	var out bytes.Buffer
	rootCmd.SetArgs([]string{"bucket", "--endpoint", sock, "versioning", "enable", "my-bucket"})
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&out)
	rootCmd.SetContext(context.Background())
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, out.String())
	}
	if gotMethod != "PUT" {
		t.Errorf("method = %q, want PUT", gotMethod)
	}
	if gotBody["status"] != "Enabled" {
		t.Errorf("body status = %q, want Enabled", gotBody["status"])
	}
}
