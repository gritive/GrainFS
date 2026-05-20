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

// buildTestConfigRoot returns a fresh root + config command tree (no globals touched).
func buildTestConfigRoot() *cobra.Command {
	root := &cobra.Command{Use: "grainfs"}
	root.AddCommand(configCmd())
	return root
}

func TestCLI_ConfigSet_Get_Roundtrip(t *testing.T) {
	var storedValue string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/config/audit.deny-only", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "PUT":
			var body struct {
				Value string `json:"value"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			storedValue = body.Value
			w.WriteHeader(http.StatusNoContent)
		case "GET":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"key":"audit.deny-only","value":%q,"kind":"bool","default":"false","set":true}`, storedValue)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	sock := startFakeAdminUDS(t, mux)

	// set
	root := buildTestConfigRoot()
	root.SetArgs([]string{"config", "--endpoint", sock, "set", "audit.deny-only", "true"})
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetContext(context.Background())
	if err := root.Execute(); err != nil {
		t.Fatalf("set: %v\noutput: %s", err, out.String())
	}
	if storedValue != "true" {
		t.Errorf("storedValue = %q, want %q", storedValue, "true")
	}

	// get
	root2 := buildTestConfigRoot()
	root2.SetArgs([]string{"config", "--endpoint", sock, "--json", "get", "audit.deny-only"})
	var out2 bytes.Buffer
	root2.SetOut(&out2)
	root2.SetErr(&out2)
	root2.SetContext(context.Background())
	if err := root2.Execute(); err != nil {
		t.Fatalf("get: %v\noutput: %s", err, out2.String())
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(out2.String())), &got); err != nil {
		t.Fatalf("get output not valid JSON: %v\noutput: %s", err, out2.String())
	}
	if got["value"] != "true" {
		t.Errorf("get value = %v, want true", got["value"])
	}
}

func TestCLI_ConfigList_OnlySetByDefault(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		// Return two entries: one set, one not set
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `[
			{"key":"audit.deny-only","value":"true","kind":"bool","default":"false","set":true,"description":""},
			{"key":"iam.anon-enabled","value":"true","kind":"bool","default":"true","set":false,"description":""}
		]`)
	})
	sock := startFakeAdminUDS(t, mux)

	root := buildTestConfigRoot()
	root.SetArgs([]string{"config", "--endpoint", sock, "--json", "list"})
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetContext(context.Background())
	if err := root.Execute(); err != nil {
		t.Fatalf("list: %v\noutput: %s", err, out.String())
	}

	var items []map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(out.String())), &items); err != nil {
		t.Fatalf("list output not valid JSON: %v\noutput: %s", err, out.String())
	}
	// Without --all, only set keys should appear
	if len(items) != 1 {
		t.Errorf("len(items) = %d, want 1 (only set keys)", len(items))
	}
	if len(items) > 0 && items[0]["key"] != "audit.deny-only" {
		t.Errorf("items[0].key = %v, want audit.deny-only", items[0]["key"])
	}
}

func TestCLI_ConfigList_AllShowsCatalog(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `[
			{"key":"audit.deny-only","value":"false","kind":"bool","default":"false","set":false,"description":"When true, all requests are logged but none are denied"},
			{"key":"iam.anon-enabled","value":"true","kind":"bool","default":"true","set":false,"description":"Allow anonymous (unauthenticated) S3 access"},
			{"key":"trusted-proxy.cidr","value":"","kind":"string","default":"","set":false,"description":"Trusted proxy CIDR range"},
			{"key":"jwt.signing-key-rotate","value":"","kind":"trigger","default":"","set":false,"description":"Rotate the JWT signing key"}
		]`)
	})
	sock := startFakeAdminUDS(t, mux)

	root := buildTestConfigRoot()
	root.SetArgs([]string{"config", "--endpoint", sock, "--json", "list", "--all"})
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetContext(context.Background())
	if err := root.Execute(); err != nil {
		t.Fatalf("list --all: %v\noutput: %s", err, out.String())
	}

	var items []map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(out.String())), &items); err != nil {
		t.Fatalf("list --all output not valid JSON: %v\noutput: %s", err, out.String())
	}
	if len(items) != 4 {
		t.Errorf("len(items) = %d, want 4", len(items))
	}
	keys := make(map[string]bool)
	for _, item := range items {
		if k, ok := item["key"].(string); ok {
			keys[k] = true
		}
	}
	for _, want := range []string{"audit.deny-only", "iam.anon-enabled", "trusted-proxy.cidr", "jwt.signing-key-rotate"} {
		if !keys[want] {
			t.Errorf("missing key %q in --all output", want)
		}
	}
}

func TestCLI_ConfigSet_JSONStdoutWhenPipe(t *testing.T) {
	// Tests that when stdout is non-tty (e.g. a pipe under `go test`), the CLI
	// emits JSON even without --json. stdoutIsTerminal() reads os.Stdout which
	// is a pipe in test environments, so omitting --json exercises the real path.
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/config/audit.deny-only", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"key":"audit.deny-only","value":"true","kind":"bool","default":"false","set":true,"description":""}`)
	})
	sock := startFakeAdminUDS(t, mux)

	// No --json flag: relies on stdoutIsTerminal() returning false under go test.
	root := buildTestConfigRoot()
	root.SetArgs([]string{"config", "--endpoint", sock, "get", "audit.deny-only"})
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetContext(context.Background())
	if err := root.Execute(); err != nil {
		t.Fatalf("get (no --json): %v\noutput: %s", err, out.String())
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(out.String())), &parsed); err != nil {
		t.Errorf("expected JSON output from non-tty stdout, got: %v\noutput: %s", err, out.String())
	}
	if parsed["key"] != "audit.deny-only" {
		t.Errorf("key = %v, want audit.deny-only", parsed["key"])
	}
}
