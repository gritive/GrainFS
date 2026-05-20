package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
)

// TestCLI_UpstreamPut_AllFlags is a cobra smoke test: verifies all six string
// flags (--scheme, --endpoint-url, --access-key, --secret-key, --region,
// --remote-bucket) reach the wire intact. Wire JSON shape is covered by
// internal/bucketadmin/upstream_ops_test.go; this asserts cmd-level flag
// wiring against the new bucketadmin flag surface.
func TestCLI_UpstreamPut_AllFlags(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/upstreams", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "PUT" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusNoContent)
	})
	sock := startFakeAdminUDS(t, mux)

	t.Cleanup(func() {
		for _, name := range []string{"scheme", "endpoint-url", "access-key", "secret-key", "region", "remote-bucket"} {
			_ = bucketUpstreamPutCmd.Flags().Set(name, "")
			bucketUpstreamPutCmd.Flags().Lookup(name).Changed = false
		}
		// Restore default for scheme (it has a default of "s3")
		_ = bucketUpstreamPutCmd.Flags().Set("scheme", "s3")
		bucketUpstreamPutCmd.Flags().Lookup("scheme").Changed = false
	})

	var out bytes.Buffer
	rootCmd.SetArgs([]string{
		"bucket", "--endpoint", sock, "upstream", "put", "my-bucket",
		"--scheme", "r2",
		"--endpoint-url", "https://r2.example.com",
		"--access-key", "AKID",
		"--secret-key", "SK",
		"--region", "auto",
		"--remote-bucket", "upstream-bkt",
	})
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&out)
	rootCmd.SetContext(context.Background())
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, out.String())
	}
	want := map[string]string{
		"bucket":        "my-bucket",
		"scheme":        "r2",
		"upstream_url":  "https://r2.example.com",
		"access_key":    "AKID",
		"secret_key":    "SK",
		"region":        "auto",
		"remote_bucket": "upstream-bkt",
	}
	for k, v := range want {
		if got := gotBody[k]; got != v {
			t.Errorf("body[%q] = %v, want %q", k, got, v)
		}
	}
}
