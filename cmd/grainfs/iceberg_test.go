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

const icebergConfigCanned = `{"catalog_uri":"http://h:9000/iceberg","oauth_token_uri":"http://h:9000/iceberg/v1/oauth/tokens","warehouse":"analytics","client_id":"AK-alice","client_secret":"SECRET-alice"}`
const icebergConfigCannedNoSecret = `{"catalog_uri":"http://h:9000/iceberg","oauth_token_uri":"http://h:9000/iceberg/v1/oauth/tokens","warehouse":"analytics","client_id":"AK-alice","client_secret":""}`

func startIcebergFakeServer(t *testing.T, respondWith string) string {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iceberg/config", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, respondWith)
	})
	return startFakeAdminUDS(t, mux)
}

// TestCLI_IcebergConfig_DefaultReveal verifies that the default (no --no-reveal)
// output contains all 5 fields and the actual secret value.
func TestCLI_IcebergConfig_DefaultReveal(t *testing.T) {
	sock := startIcebergFakeServer(t, icebergConfigCanned)

	var out bytes.Buffer
	rootCmd.SetArgs([]string{"iceberg", "--endpoint", sock, "config", "--warehouse", "analytics", "--sa", "alice"})
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&out)
	rootCmd.SetContext(context.Background())
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, out.String())
	}
	s := out.String()
	for _, want := range []string{
		"catalog_uri", "oauth_token_uri", "warehouse", "client_id", "client_secret", "SECRET-alice",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("missing %q in output:\n%s", want, s)
		}
	}
}

// TestCLI_IcebergConfig_NoReveal verifies that --no-reveal causes the server
// to receive no_reveal=true and the output shows <hidden rather than the secret.
func TestCLI_IcebergConfig_NoReveal(t *testing.T) {
	// The fake server returns response with empty client_secret (server-side zeroing).
	sock := startIcebergFakeServer(t, icebergConfigCannedNoSecret)

	var out bytes.Buffer
	rootCmd.SetArgs([]string{"iceberg", "--endpoint", sock, "config", "--warehouse", "analytics", "--sa", "alice", "--no-reveal"})
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&out)
	rootCmd.SetContext(context.Background())
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, out.String())
	}
	s := out.String()
	if strings.Contains(s, "SECRET-alice") {
		t.Errorf("output must not contain secret in --no-reveal mode:\n%s", s)
	}
	if !strings.Contains(s, "<hidden") {
		t.Errorf("output should contain '<hidden' in --no-reveal mode:\n%s", s)
	}
}

// TestCLI_IcebergConfig_JSON verifies that --json emits valid JSON and
// client_secret equals the real secret returned by the server.
func TestCLI_IcebergConfig_JSON(t *testing.T) {
	sock := startIcebergFakeServer(t, icebergConfigCanned)

	var out bytes.Buffer
	rootCmd.SetArgs([]string{"iceberg", "--json", "--endpoint", sock, "config", "--warehouse", "analytics", "--sa", "alice"})
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&out)
	rootCmd.SetContext(context.Background())
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, out.String())
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(out.String())), &parsed); err != nil {
		t.Fatalf("output is not valid JSON: %v\noutput: %s", err, out.String())
	}
	if parsed["client_secret"] != "SECRET-alice" {
		t.Errorf("client_secret = %v, want SECRET-alice", parsed["client_secret"])
	}
	if parsed["client_id"] != "AK-alice" {
		t.Errorf("client_id = %v, want AK-alice", parsed["client_id"])
	}
}
