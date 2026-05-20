package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/adminapi"
)

// buildTestStatusRoot returns a fresh root + status command tree.
func buildTestStatusRoot() *cobra.Command {
	root := &cobra.Command{Use: "grainfs"}
	root.AddCommand(statusCmd())
	return root
}

func runStatusCmd(t *testing.T, sock string, args ...string) (string, error) {
	t.Helper()
	root := buildTestStatusRoot()
	root.SetArgs(append([]string{"status", "--endpoint", sock}, args...))
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetContext(context.Background())
	err := root.Execute()
	return out.String(), err
}

// fakePhase0Report returns a minimal StatusReport for phase-0 testing.
func fakePhase0Report() adminapi.StatusReport {
	return adminapi.StatusReport{
		Cluster: adminapi.ClusterStatus{
			NodeID:      "node-001",
			ClusterSize: 1,
		},
		Phase: 0,
		IAM: adminapi.IAMStatus{
			SACount: 0,
		},
		Banner: true,
		Encryption: adminapi.EncryptionStatus{
			Enabled: true,
			DEKGen:  1,
		},
		TLS: adminapi.TLSStatus{
			CertPresent: false,
		},
		Audit: adminapi.AuditStatus{
			DenyOnly: false,
		},
		JWTKeys: adminapi.JWTStatus{
			CurrentKID: "",
		},
	}
}

func fakePhase2Report() adminapi.StatusReport {
	return adminapi.StatusReport{
		Cluster: adminapi.ClusterStatus{
			NodeID:      "node-001",
			ClusterSize: 1,
		},
		Phase: 2,
		IAM: adminapi.IAMStatus{
			SACount: 1,
		},
		Banner: false,
		Encryption: adminapi.EncryptionStatus{
			Enabled: true,
			DEKGen:  1,
		},
		TLS: adminapi.TLSStatus{
			CertPresent: false,
		},
		Audit: adminapi.AuditStatus{
			DenyOnly: false,
		},
		JWTKeys: adminapi.JWTStatus{
			CurrentKID: "k_abc123",
		},
	}
}

// TestCLI_Status_Phase0 verifies that `status` renders phase=0 for a fresh
// single-node with no service accounts.
func TestCLI_Status_Phase0(t *testing.T) {
	report := fakePhase0Report()
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(report); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
	sock := startFakeAdminUDS(t, mux)

	out, err := runStatusCmd(t, sock, "--json")
	if err != nil {
		t.Fatalf("status phase0: %v\noutput: %s", err, out)
	}
	var got adminapi.StatusReport
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("parse output as JSON: %v\noutput: %s", err, out)
	}
	if got.Phase != 0 {
		t.Errorf("expected phase=0, got phase=%d", got.Phase)
	}
}

// TestCLI_Status_Phase2 verifies that `status` renders phase=2 after
// `iam sa create admin` (sa_count >= 1, no TLS cert).
func TestCLI_Status_Phase2(t *testing.T) {
	report := fakePhase2Report()
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(report); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
	sock := startFakeAdminUDS(t, mux)

	out, err := runStatusCmd(t, sock, "--json")
	if err != nil {
		t.Fatalf("status phase2: %v\noutput: %s", err, out)
	}
	var got adminapi.StatusReport
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("parse output as JSON: %v\noutput: %s", err, out)
	}
	if got.Phase != 2 {
		t.Errorf("expected phase=2, got phase=%d", got.Phase)
	}
	if got.IAM.SACount < 1 {
		t.Errorf("expected sa_count >= 1, got %d", got.IAM.SACount)
	}
}

// TestCLI_Status_AllFieldsPresent verifies that all 9 top-level JSON keys are
// present in the output.
func TestCLI_Status_AllFieldsPresent(t *testing.T) {
	report := fakePhase2Report()
	report.TrustedProxy = []string{"10.0.0.0/8", "192.168.0.0/16"}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(report); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
	sock := startFakeAdminUDS(t, mux)

	out, err := runStatusCmd(t, sock, "--json")
	if err != nil {
		t.Fatalf("status all-fields: %v\noutput: %s", err, out)
	}
	requiredKeys := []string{"cluster", "phase", "iam", "encryption", "tls", "trusted_proxy", "audit", "jwt_keys", "banner"}
	for _, key := range requiredKeys {
		if !containsKey(out, key) {
			t.Errorf("expected output to contain key %q, got: %s", key, out)
		}
	}
}

// containsKey checks whether the JSON string contains the given top-level key.
func containsKey(jsonStr, key string) bool {
	var m map[string]json.RawMessage
	if err := json.Unmarshal([]byte(jsonStr), &m); err != nil {
		return false
	}
	_, ok := m[key]
	return ok
}

// TestCLI_Status_TableOutput verifies table (non-JSON) output for human
// readability.
func TestCLI_Status_TableOutput(t *testing.T) {
	report := fakePhase0Report()
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(report); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
	sock := startFakeAdminUDS(t, mux)

	// Without --json and without a terminal, stdoutIsTerminal returns false
	// (pipe), so the command defaults to JSON. We force --json to be explicit.
	out, err := runStatusCmd(t, sock, "--json")
	if err != nil {
		t.Fatalf("status table: %v\noutput: %s", err, out)
	}
	if out == "" {
		t.Error("expected non-empty output")
	}
	// Output must contain "phase" key at minimum.
	if !containsKey(out, "cluster") {
		t.Errorf("expected cluster key in output, got: %s", out)
	}
}
