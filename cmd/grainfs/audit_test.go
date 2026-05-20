package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// buildTestAuditRoot returns a fresh root + audit command tree.
func buildTestAuditRoot() *cobra.Command {
	root := &cobra.Command{Use: "grainfs"}
	root.AddCommand(auditCmd())
	return root
}

func runAuditCmd(t *testing.T, sock string, args ...string) (string, error) {
	t.Helper()
	root := buildTestAuditRoot()
	root.SetArgs(append([]string{"audit", "--endpoint", sock}, args...))
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetContext(context.Background())
	err := root.Execute()
	return out.String(), err
}

// TestCLI_AuditQuery_Counts verifies that `audit query <SQL>` renders the
// response from the fake admin endpoint. The "2 denies" expectation from the
// plan spec is satisfied by the mock handler returning count=2.
func TestCLI_AuditQuery_Counts(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/audit/query", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"columns":["count(*)"],"rows":[["2"]]}`)
	})
	sock := startFakeAdminUDS(t, mux)

	out, err := runAuditCmd(t, sock, "--json", "query",
		"SELECT count(*) FROM grainfs_iceberg.audit.s3 WHERE auth_status='deny'")
	if err != nil {
		t.Fatalf("audit query: %v\noutput: %s", err, out)
	}
	if !strings.Contains(out, "2") {
		t.Errorf("expected output to contain '2', got: %s", out)
	}
}

// TestCLI_AuditRecentDenies verifies that `audit recent-denies` renders rows
// containing "deny" from the fake endpoint.
func TestCLI_AuditRecentDenies(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/audit/recent-denies", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"columns":["ts","sa_id","operation","bucket","key","matched_sid"],`+
			`"rows":[["2024-01-01T00:00:00Z","sa:abc","GetObject","mybucket","mykey","deny-sid"]]}`)
	})
	sock := startFakeAdminUDS(t, mux)

	out, err := runAuditCmd(t, sock, "recent-denies")
	if err != nil {
		t.Fatalf("audit recent-denies: %v\noutput: %s", err, out)
	}
	if !strings.Contains(out, "deny") {
		t.Errorf("expected output to contain 'deny', got: %s", out)
	}
}

// TestCLI_AuditByRequestID verifies that `audit by-request-id <rid>` passes
// the request ID in the URL and renders the returned row.
func TestCLI_AuditByRequestID(t *testing.T) {
	const rid = "01938abc-1234-7000-0000-000000000001"
	var capturedPath string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/audit/by-request-id/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		capturedPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"columns":["ts","request_id","sa_id","source_ip","user_agent",`+
			`"operation","bucket","key","http_status","err_class","err_reason","latency_ms"],`+
			`"rows":[["2024-01-01T00:00:00Z",%q,"sa:abc","1.2.3.4","aws-cli",`+
			`"GetObject","mybucket","mykey","403","Forbidden","","5"]]}`, rid)
	})
	sock := startFakeAdminUDS(t, mux)

	out, err := runAuditCmd(t, sock, "--json", "by-request-id", rid)
	if err != nil {
		t.Fatalf("audit by-request-id: %v\noutput: %s", err, out)
	}
	if !strings.Contains(capturedPath, rid) {
		t.Errorf("request path %q does not contain request ID %q", capturedPath, rid)
	}
	if !strings.Contains(out, rid) {
		t.Errorf("expected output to contain request ID %q, got: %s", rid, out)
	}
}

// TestCLI_AuditBySA verifies that `audit by-sa <said>` passes the SA ID in
// the URL and renders the returned row.
func TestCLI_AuditBySA(t *testing.T) {
	const said = "sa:myserviceaccount"
	var capturedPath string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/audit/by-sa/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		capturedPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"columns":["ts","request_id","sa_id","source_ip","user_agent",`+
			`"operation","bucket","key","http_status","err_class","err_reason","latency_ms"],`+
			`"rows":[["2024-01-01T00:00:00Z","req-001",%q,"1.2.3.4","aws-cli",`+
			`"PutObject","mybucket","mykey","200","","","10"]]}`, said)
	})
	sock := startFakeAdminUDS(t, mux)

	out, err := runAuditCmd(t, sock, "--json", "by-sa", said)
	if err != nil {
		t.Fatalf("audit by-sa: %v\noutput: %s", err, out)
	}
	if !strings.Contains(capturedPath, "sa%3Amyserviceaccount") && !strings.Contains(capturedPath, said) {
		t.Errorf("request path %q does not contain sa_id", capturedPath)
	}
	if !strings.Contains(out, said) {
		t.Errorf("expected output to contain SA ID %q, got: %s", said, out)
	}
}
