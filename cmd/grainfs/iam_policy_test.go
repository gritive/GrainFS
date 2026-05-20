package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeFixturePolicy writes a policy JSON string to a temp file and returns the path.
func writeFixturePolicy(t *testing.T, policyJSON string) string {
	t.Helper()
	dir := t.TempDir()
	f := filepath.Join(dir, "policy.json")
	if err := os.WriteFile(f, []byte(policyJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	return f
}

// TestCLI_PolicyPut_RoundTrip: PUT policy doc then list — name appears in output.
func TestCLI_PolicyPut_RoundTrip(t *testing.T) {
	const polName = "my-pol"
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iam/policy/"+polName, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			_, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/v1/iam/policy", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `[%q,"readonly"]`, polName)
	})
	sock := startFakeAdminUDS(t, mux)
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject"],"Resource":"*"}]}`
	polFile := writeFixturePolicy(t, doc)

	var out bytes.Buffer
	rootCmd.SetArgs([]string{"iam", "--endpoint", sock, "policy", "put", polName, "--file", polFile})
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&out)
	rootCmd.SetContext(context.Background())
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("put: %v\noutput: %s", err, out.String())
	}

	out.Reset()
	rootCmd.SetArgs([]string{"iam", "--endpoint", sock, "policy", "list"})
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&out)
	rootCmd.SetContext(context.Background())
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("list: %v\noutput: %s", err, out.String())
	}
	if s := out.String(); !strings.Contains(s, polName) {
		t.Errorf("list output missing %q:\n%s", polName, s)
	}
}

// TestCLI_PolicyAttach_ResourceStarWarning: attaching a policy with Resource:*
// emits the spec'd warning to stderr.
func TestCLI_PolicyAttach_ResourceStarWarning(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iam/policy/readonly", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject"],"Resource":"*"}]}`)
	})
	mux.HandleFunc("/v1/iam/policy/readonly/attach/sa/alice", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	sock := startFakeAdminUDS(t, mux)

	var stdout, stderr bytes.Buffer
	rootCmd.SetArgs([]string{"iam", "--endpoint", sock, "policy", "attach", "readonly", "--sa", "alice"})
	rootCmd.SetOut(&stdout)
	rootCmd.SetErr(&stderr)
	rootCmd.SetContext(context.Background())
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("attach: %v\nstdout: %s\nstderr: %s", err, stdout.String(), stderr.String())
	}
	if s := stderr.String(); !strings.Contains(s, "Resource:* — grants access to ALL buckets") {
		t.Errorf("stderr missing Resource:* warning:\n%s", s)
	}
}

// TestCLI_PolicyAttach_IKnow_Suppresses: --i-know suppresses the Resource:* warning.
func TestCLI_PolicyAttach_IKnow_Suppresses(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iam/policy/readonly", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject"],"Resource":"*"}]}`)
	})
	mux.HandleFunc("/v1/iam/policy/readonly/attach/sa/alice", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	sock := startFakeAdminUDS(t, mux)

	var stdout, stderr bytes.Buffer
	rootCmd.SetArgs([]string{"iam", "--endpoint", sock, "policy", "attach", "readonly", "--sa", "alice", "--i-know"})
	rootCmd.SetOut(&stdout)
	rootCmd.SetErr(&stderr)
	rootCmd.SetContext(context.Background())
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("attach: %v\nstdout: %s\nstderr: %s", err, stdout.String(), stderr.String())
	}
	if s := stderr.String(); strings.Contains(s, "Resource:*") {
		t.Errorf("stderr should NOT contain Resource:* warning when --i-know passed:\n%s", s)
	}
}

// TestCLI_PolicyDelete_BuiltinRefused: server returns 403 for built-in; CLI exits non-zero.
func TestCLI_PolicyDelete_BuiltinRefused(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iam/policy/readonly", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprint(w, `{"code":"forbidden","message":"cannot delete built-in policy: \"readonly\""}`)
	})
	sock := startFakeAdminUDS(t, mux)

	var out bytes.Buffer
	rootCmd.SetArgs([]string{"iam", "--endpoint", sock, "policy", "delete", "readonly"})
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&out)
	rootCmd.SetContext(context.Background())
	err := rootCmd.Execute()
	if err == nil {
		t.Fatal("expected non-zero exit for built-in delete, got nil error")
	}
}

// TestCLI_PolicyValidate_Local: validate reads a file locally without any admin UDS.
func TestCLI_PolicyValidate_Local(t *testing.T) {
	good := writeFixturePolicy(t, `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject"],"Resource":"*"}]}`)
	bad := writeFixturePolicy(t, `{"NotAction":"this is not a valid field"}`)

	// Good policy — exit 0
	var out bytes.Buffer
	rootCmd.SetArgs([]string{"iam", "policy", "validate", "--file", good})
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&out)
	rootCmd.SetContext(context.Background())
	if err := rootCmd.Execute(); err != nil {
		t.Errorf("validate good policy: unexpected error: %v\noutput: %s", err, out.String())
	}

	// Bad policy — exit non-zero
	out.Reset()
	rootCmd.SetArgs([]string{"iam", "policy", "validate", "--file", bad})
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&out)
	rootCmd.SetContext(context.Background())
	if err := rootCmd.Execute(); err == nil {
		t.Errorf("validate bad policy: expected non-zero exit\noutput: %s", out.String())
	}
}

// TestCLI_PolicySimulate: simulate POST returns effect; CLI prints "Allow".
func TestCLI_PolicySimulate(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iam/policy/simulate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		_, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"effect":"Allow","matched_policy":"readonly","matched_sid":"sid1","reason":"explicit Allow"}`)
	})
	sock := startFakeAdminUDS(t, mux)

	var out bytes.Buffer
	rootCmd.SetArgs([]string{
		"iam", "--endpoint", sock,
		"policy", "simulate",
		"--sa", "sa-x",
		"--action", "s3:GetObject",
		"--resource", "arn:aws:s3:::my-bucket/*",
	})
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&out)
	rootCmd.SetContext(context.Background())
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("simulate: %v\noutput: %s", err, out.String())
	}
	if s := out.String(); !strings.Contains(s, "Allow") {
		t.Errorf("output missing 'Allow':\n%s", s)
	}
}
