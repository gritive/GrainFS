package statusadmin

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gritive/GrainFS/internal/adminapi"
)

func newTestServer(t *testing.T, mux *http.ServeMux) (url string, stop func()) {
	t.Helper()
	srv := httptest.NewServer(mux)
	return srv.URL, srv.Close
}

// sampleReport builds a StatusReport with recognisable values for assertions.
func sampleReport() adminapi.StatusReport {
	return adminapi.StatusReport{
		Cluster: adminapi.ClusterStatus{
			NodeID:      "node-1",
			ClusterSize: 1,
		},
		IAM: adminapi.IAMStatus{
			SACount: 3,
		},
		Encryption: adminapi.EncryptionStatus{
			Enabled: true,
			DEKGen:  7,
		},
		TLS: adminapi.TLSStatus{
			CertPresent: true,
		},
		TrustedProxy: []string{"10.0.0.0/8"},
		Audit: adminapi.AuditStatus{
			DenyOnly: false,
		},
		JWTKeys: adminapi.JWTStatus{
			CurrentKID:  "kid-abc",
			PreviousKID: "kid-xyz",
		},
		Banner: true,
	}
}

// TestRunGetStatus_TableOutput verifies the human-readable table contains
// all key fields from the status report.
func TestRunGetStatus_TableOutput(t *testing.T) {
	report := sampleReport()
	body, _ := json.Marshal(report)

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	})
	url, stop := newTestServer(t, mux)
	defer stop()

	var out bytes.Buffer
	err := RunGetStatus(context.Background(), GetOptions{
		BaseOptions: BaseOptions{Endpoint: url, Stdout: &out},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := out.String()
	checks := []string{
		"node-1",              // cluster.node_id
		"1",                   // cluster_size
		"sa_count:",           // iam.sa_count label
		"encryption_enabled:", // encryption label
		"tls_cert_present:",   // tls label
		"trusted_proxy:",      // trusted_proxy label
		"audit_deny_only:",    // audit label
		"kid-abc",             // jwt current kid
		"kid-xyz",             // jwt previous kid
	}
	for _, want := range checks {
		if !strings.Contains(got, want) {
			t.Errorf("output missing %q\nfull output:\n%s", want, got)
		}
	}
}

// TestRunGetStatus_JSONOut verifies --json mode emits valid JSON with top-level keys.
func TestRunGetStatus_JSONOut(t *testing.T) {
	report := sampleReport()
	body, _ := json.Marshal(report)

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	})
	url, stop := newTestServer(t, mux)
	defer stop()

	var out bytes.Buffer
	err := RunGetStatus(context.Background(), GetOptions{
		BaseOptions: BaseOptions{Endpoint: url, Stdout: &out, JSONOut: true},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(out.String())), &got); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, out.String())
	}

	topLevel := []string{"cluster", "iam", "encryption", "tls", "trusted_proxy", "audit", "jwt_keys", "banner"}
	for _, key := range topLevel {
		if _, ok := got[key]; !ok {
			t.Errorf("JSON output missing top-level key %q", key)
		}
	}
	for _, key := range []string{"phase", "mode"} {
		if _, ok := got[key]; ok {
			t.Errorf("JSON output should not contain top-level key %q", key)
		}
	}
}

// TestRunGetStatus_ServerError verifies a 503 response yields an error.
func TestRunGetStatus_ServerError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/status", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"code":"unavailable","error":"leader election in progress"}`))
	})
	url, stop := newTestServer(t, mux)
	defer stop()

	err := RunGetStatus(context.Background(), GetOptions{
		BaseOptions: BaseOptions{Endpoint: url, Stdout: &bytes.Buffer{}},
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TestRunGetStatus_NotFound verifies a 404 response yields an error.
func TestRunGetStatus_NotFound(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/status", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"code":"not_found","error":"endpoint not registered"}`))
	})
	url, stop := newTestServer(t, mux)
	defer stop()

	err := RunGetStatus(context.Background(), GetOptions{
		BaseOptions: BaseOptions{Endpoint: url, Stdout: &bytes.Buffer{}},
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TestRunGetStatus_NoJWTKeys verifies table output is still clean when JWT
// keys are absent (empty KIDs suppress the jwt lines).
func TestRunGetStatus_NoJWTKeys(t *testing.T) {
	report := sampleReport()
	report.JWTKeys = adminapi.JWTStatus{} // both KIDs empty
	body, _ := json.Marshal(report)

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	})
	url, stop := newTestServer(t, mux)
	defer stop()

	var out bytes.Buffer
	err := RunGetStatus(context.Background(), GetOptions{
		BaseOptions: BaseOptions{Endpoint: url, Stdout: &out},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// JWT lines must not appear when KIDs are empty.
	if strings.Contains(out.String(), "jwt_current_kid") {
		t.Errorf("unexpected jwt_current_kid line; output:\n%s", out.String())
	}
}
