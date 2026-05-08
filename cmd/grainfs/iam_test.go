package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func TestIAMEndpointFromCmd_Empty(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.Flags().String("endpoint", "", "")
	if _, err := iamEndpointFromCmd(cmd); err == nil {
		t.Fatal("expected error for empty endpoint")
	}
}

func TestIAMEndpointFromCmd_StripUnixPrefix(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.Flags().String("endpoint", "unix:/tmp/admin.sock", "")
	got, err := iamEndpointFromCmd(cmd)
	if err != nil {
		t.Fatal(err)
	}
	if got != "/tmp/admin.sock" {
		t.Errorf("got %q, want /tmp/admin.sock", got)
	}
}

func TestIAMEndpointFromCmd_RejectHTTP(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.Flags().String("endpoint", "http://example.com", "")
	if _, err := iamEndpointFromCmd(cmd); err == nil {
		t.Fatal("expected rejection of http:// scheme")
	}
}

// startFakeAdminUDS spins a fake admin UDS server and returns the socket path.
// Uses os.MkdirTemp under the OS temp root (not t.TempDir, whose path can exceed
// the platform's sun_path limit on macOS / BSD when the test name is long).
func startFakeAdminUDS(t *testing.T, mux *http.ServeMux) string {
	t.Helper()
	d, err := os.MkdirTemp("", "iamtest-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(d) })
	sock := filepath.Join(d, "a.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		_ = srv.Close()
		_ = ln.Close()
		_ = os.Remove(sock)
	})
	return sock
}

// buildTestIAMRoot returns a fresh root+iam command tree (no globals touched).
func buildTestIAMRoot() *cobra.Command {
	root := &cobra.Command{Use: "grainfs"}
	iam := &cobra.Command{Use: "iam"}
	iam.PersistentFlags().String("endpoint", "", "")
	iam.AddCommand(iamSACmd(), iamKeyCmd(), iamGrantCmd())
	root.AddCommand(iam)
	return root
}

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

	root := buildTestIAMRoot()
	root.SetArgs([]string{"iam", "--endpoint", sock, "sa", "create", "alice"})
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetContext(context.Background())
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, out.String())
	}
	if !strings.Contains(out.String(), `"sa_id":"sa-x"`) {
		t.Errorf("unexpected output: %s", out.String())
	}
}

func TestIAMGrantList_QueryParams(t *testing.T) {
	mux := http.NewServeMux()
	var gotQuery string
	mux.HandleFunc("/v1/iam/grant", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		gotQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `[]`)
	})
	sock := startFakeAdminUDS(t, mux)

	root := buildTestIAMRoot()
	root.SetArgs([]string{"iam", "--endpoint", sock, "grant", "list", "--sa", "sa-x", "--bucket", "b1"})
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetContext(context.Background())
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, out.String())
	}
	if !strings.Contains(gotQuery, "sa=sa-x") || !strings.Contains(gotQuery, "bucket=b1") {
		t.Errorf("query = %q, want sa=sa-x and bucket=b1", gotQuery)
	}
}

func TestIAMRequest_Non2xxReturnsError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iam/sa/missing", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `not found`)
	})
	sock := startFakeAdminUDS(t, mux)

	root := buildTestIAMRoot()
	root.SetArgs([]string{"iam", "--endpoint", sock, "sa", "get", "missing"})
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetContext(context.Background())
	err := root.Execute()
	if err == nil {
		t.Fatal("expected error for 404")
	}
	if !strings.Contains(err.Error(), "404") {
		t.Errorf("error = %v, want 404 in message", err)
	}
}

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

	root := buildTestIAMRoot()
	root.SetArgs([]string{"iam", "--endpoint", sock, "key", "create", "sa-x", "--bucket", "logs", "--bucket", "reports"})
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetContext(context.Background())
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, out.String())
	}

	// Verify the request body contained buckets: ["logs","reports"]
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

	root := buildTestIAMRoot()
	root.SetArgs([]string{"iam", "--endpoint", sock, "key", "create", "sa-x"})
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetContext(context.Background())
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, out.String())
	}

	// Verify the request body does NOT contain buckets field (preserves legacy {} wire format)
	if _, present := gotBody["buckets"]; present {
		t.Fatalf("body should not contain \"buckets\" key when --bucket not used; got %v", gotBody)
	}
}
