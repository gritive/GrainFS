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

func TestAdminEndpointFromCmd_Empty(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.Flags().String("endpoint", "", "")
	if _, err := adminEndpointFromCmd(cmd); err == nil {
		t.Fatal("expected error for empty endpoint")
	}
}

func TestAdminEndpointFromCmd_StripUnixPrefix(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.Flags().String("endpoint", "unix:/tmp/admin.sock", "")
	got, err := adminEndpointFromCmd(cmd)
	if err != nil {
		t.Fatal(err)
	}
	if got != "/tmp/admin.sock" {
		t.Errorf("got %q, want /tmp/admin.sock", got)
	}
}

func TestAdminEndpointFromCmd_RejectHTTP(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.Flags().String("endpoint", "http://example.com", "")
	if _, err := adminEndpointFromCmd(cmd); err == nil {
		t.Fatal("expected rejection of http:// scheme")
	}
}

func TestAdminEndpointFromCmd_EnvVar(t *testing.T) {
	t.Setenv("GRAINFS_ADMIN_SOCKET", "/tmp/env.sock")
	cmd := &cobra.Command{}
	cmd.Flags().String("endpoint", "", "")
	got, err := adminEndpointFromCmd(cmd)
	if err != nil {
		t.Fatal(err)
	}
	if got != "/tmp/env.sock" {
		t.Errorf("got %q, want /tmp/env.sock", got)
	}
}

func TestAdminEndpointFromCmd_FlagOverridesEnv(t *testing.T) {
	t.Setenv("GRAINFS_ADMIN_SOCKET", "/tmp/env.sock")
	cmd := &cobra.Command{}
	cmd.Flags().String("endpoint", "/tmp/flag.sock", "")
	got, err := adminEndpointFromCmd(cmd)
	if err != nil {
		t.Fatal(err)
	}
	if got != "/tmp/flag.sock" {
		t.Errorf("got %q, want /tmp/flag.sock", got)
	}
}

// TestIAMSACreate_EndToEnd is a cobra smoke test: it drives the live rootCmd
// through cobra → iamadmin → adminapi against a fake admin UDS server, then
// verifies the runner emits valid JSON keyed by sa_id. Wire-level shape and
// text rendering are covered by internal/iamadmin/*_test.go; this test exists
// to catch breakage at the cmd boundary (flag wiring, command registration).
func TestIAMSACreate_EndToEnd(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iam/sa", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["name"] != "alice" {
			t.Errorf("name = %q, want alice", body["name"])
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"sa_id":"sa-x","name":"alice","access_key":"AK1","secret_key":"SK1"}`)
	})
	sock := startFakeAdminUDS(t, mux)

	var out bytes.Buffer
	rootCmd.SetArgs([]string{"iam", "--endpoint", sock, "--json", "sa", "create", "alice"})
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&out)
	rootCmd.SetContext(context.Background())
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, out.String())
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(out.String())), &parsed); err != nil {
		t.Errorf("output is not valid JSON: %v\noutput: %s", err, out.String())
	}
	if parsed["sa_id"] != "sa-x" {
		t.Errorf("sa_id = %v, want sa-x", parsed["sa_id"])
	}
}

// TestCLI_KeyCreate_BucketFlag verifies the cobra `--bucket` repeatable flag
// reaches the wire as a JSON `buckets` array. Wire shape itself is covered by
// internal/iamadmin/client_test.go; this asserts the cmd-level flag wiring.
//
// NOTE: `--bucket` is a StringSlice on the singleton iamKeyCreateCmd; we reset
// it in t.Cleanup so TestCLI_KeyCreate_NoBucketFlag (which asserts the absence
// of the buckets field) sees a clean slate.
func TestCLI_KeyCreate_BucketFlag(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iam/sa/sa-x/key", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"access_key":"AK1","secret_key":"SK1","sa_id":"sa-x","created_at":"2026-01-01T00:00:00Z","buckets":["logs","reports"]}`)
	})
	sock := startFakeAdminUDS(t, mux)

	t.Cleanup(func() {
		// StringSlice values persist on the singleton command; clear so the
		// sibling NoBucketFlag test sees no leaked buckets.
		if sv, ok := iamKeyCreateCmd.Flags().Lookup("bucket").Value.(interface {
			Replace([]string) error
		}); ok {
			_ = sv.Replace([]string{})
		}
		iamKeyCreateCmd.Flags().Lookup("bucket").Changed = false
	})

	var out bytes.Buffer
	rootCmd.SetArgs([]string{"iam", "--endpoint", sock, "key", "create", "sa-x", "--bucket", "logs", "--bucket", "reports"})
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&out)
	rootCmd.SetContext(context.Background())
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, out.String())
	}

	bucketsRaw, ok := gotBody["buckets"]
	if !ok {
		t.Fatalf("request body missing 'buckets' field; got %v", gotBody)
	}
	var buckets []string
	for _, v := range bucketsRaw.([]any) {
		buckets = append(buckets, v.(string))
	}
	if len(buckets) != 2 || buckets[0] != "logs" || buckets[1] != "reports" {
		t.Errorf("buckets = %v, want [logs reports]", buckets)
	}
}

// TestCLI_KeyCreate_NoBucketFlag asserts that omitting --bucket produces a
// request body without the `buckets` key (preserves the legacy {} wire format).
func TestCLI_KeyCreate_NoBucketFlag(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iam/sa/sa-x/key", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"access_key":"AK1","secret_key":"SK1","sa_id":"sa-x","created_at":"2026-01-01T00:00:00Z"}`)
	})
	sock := startFakeAdminUDS(t, mux)

	var out bytes.Buffer
	rootCmd.SetArgs([]string{"iam", "--endpoint", sock, "key", "create", "sa-x"})
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&out)
	rootCmd.SetContext(context.Background())
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, out.String())
	}

	if _, present := gotBody["buckets"]; present {
		t.Fatalf("body should not contain \"buckets\" key when --bucket not used; got %v", gotBody)
	}
}
