package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
)

// TestCLI_IAMBucketCreate_NoAttach: POST /v1/buckets → 201; CLI exits 0.
func TestCLI_IAMBucketCreate_NoAttach(t *testing.T) {
	const bucketName = "my-bucket"
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		body, _ := io.ReadAll(r.Body)
		var req map[string]string
		_ = json.Unmarshal(body, &req)
		if req["name"] != bucketName {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"name":"` + bucketName + `"}`))
	})
	sock := startFakeAdminUDS(t, mux)

	var out bytes.Buffer
	rootCmd.SetArgs([]string{"iam", "--endpoint", sock, "bucket", "create", bucketName})
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&out)
	rootCmd.SetContext(context.Background())
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("iam bucket create: %v\noutput: %s", err, out.String())
	}
	if s := out.String(); !strings.Contains(s, bucketName) {
		t.Errorf("output missing bucket name %q:\n%s", bucketName, s)
	}
}

// TestCLI_IAMBucketCreate_WithAttach: attach-sa + attach-policy fields sent in body.
func TestCLI_IAMBucketCreate_WithAttach(t *testing.T) {
	const bucketName = "analytics"
	const saID = "sa-abc123"
	const policy = "readwrite"

	var gotAttachSA, gotAttachPolicy string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		body, _ := io.ReadAll(r.Body)
		var req map[string]string
		_ = json.Unmarshal(body, &req)
		gotAttachSA = req["attach_sa"]
		gotAttachPolicy = req["attach_policy"]
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"name":"` + bucketName + `"}`))
	})
	sock := startFakeAdminUDS(t, mux)

	var out bytes.Buffer
	rootCmd.SetArgs([]string{
		"iam", "--endpoint", sock, "bucket", "create", bucketName,
		"--attach-sa", saID,
		"--attach-policy", policy,
	})
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&out)
	rootCmd.SetContext(context.Background())
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("iam bucket create with attach: %v\noutput: %s", err, out.String())
	}
	if gotAttachSA != saID {
		t.Errorf("attach_sa: want %q got %q", saID, gotAttachSA)
	}
	if gotAttachPolicy != policy {
		t.Errorf("attach_policy: want %q got %q", policy, gotAttachPolicy)
	}
}

// TestCLI_IAMBucketCreate_AttachMutualRequirement: only one of attach-sa/attach-policy → error.
func TestCLI_IAMBucketCreate_AttachMutualRequirement(t *testing.T) {
	mux := http.NewServeMux()
	sock := startFakeAdminUDS(t, mux)

	var out bytes.Buffer
	rootCmd.SetArgs([]string{
		"iam", "--endpoint", sock, "bucket", "create", "test-bucket",
		"--attach-sa", "sa-123",
		// --attach-policy intentionally omitted
	})
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&out)
	rootCmd.SetContext(context.Background())
	err := rootCmd.Execute()
	if err == nil {
		t.Fatal("expected error for mismatched attach flags, got nil")
	}
}

// TestCLI_IAMBucketDelete: DELETE /v1/buckets/my-bucket → 204; CLI exits 0.
func TestCLI_IAMBucketDelete(t *testing.T) {
	const bucketName = "my-bucket"
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/"+bucketName, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	sock := startFakeAdminUDS(t, mux)

	var out bytes.Buffer
	rootCmd.SetArgs([]string{"iam", "--endpoint", sock, "bucket", "delete", bucketName})
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&out)
	rootCmd.SetContext(context.Background())
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("iam bucket delete: %v\noutput: %s", err, out.String())
	}
}

// TestCLI_IAMBucketList: GET /v1/buckets → list; CLI exits 0.
func TestCLI_IAMBucketList(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"buckets":[{"name":"alpha"},{"name":"beta"}]}`))
	})
	sock := startFakeAdminUDS(t, mux)

	var out bytes.Buffer
	rootCmd.SetArgs([]string{"iam", "--endpoint", sock, "bucket", "list"})
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&out)
	rootCmd.SetContext(context.Background())
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("iam bucket list: %v\noutput: %s", err, out.String())
	}
	s := out.String()
	if !strings.Contains(s, "alpha") || !strings.Contains(s, "beta") {
		t.Errorf("output missing bucket names:\n%s", s)
	}
}

// TestCLI_IAMBucketPolicyPutDelete: PUT and DELETE /v1/buckets/my-bucket/policy
func TestCLI_IAMBucketPolicyPutDelete(t *testing.T) {
	const bucketName = "my-bucket"
	const policyDoc = `{"Version":"2012-10-17","Statement":[]}`
	path := "/v1/buckets/" + bucketName + "/policy"

	var gotMethod string
	var gotBody []byte
	mux := http.NewServeMux()
	mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotBody, _ = io.ReadAll(r.Body)
		switch r.Method {
		case http.MethodPut, http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	sock := startFakeAdminUDS(t, mux)

	// Create a temp policy file.
	policyFile := t.TempDir() + "/policy.json"
	if err := os.WriteFile(policyFile, []byte(policyDoc), 0644); err != nil {
		t.Fatalf("write temp policy: %v", err)
	}

	// policy put
	var out bytes.Buffer
	rootCmd.SetArgs([]string{"iam", "--endpoint", sock, "bucket", "policy", "put", bucketName, "--file", policyFile})
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&out)
	rootCmd.SetContext(context.Background())
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("policy put: %v\noutput: %s", err, out.String())
	}
	if gotMethod != http.MethodPut {
		t.Errorf("policy put: expected PUT, got %s", gotMethod)
	}
	// Body must be the server-side envelope {"policy": <doc>}, not the raw doc.
	wantBody := `{"policy":` + policyDoc + `}`
	if string(gotBody) != wantBody {
		t.Errorf("policy put: body mismatch\nwant: %s\ngot:  %s", wantBody, gotBody)
	}

	// policy delete
	out.Reset()
	rootCmd.SetArgs([]string{"iam", "--endpoint", sock, "bucket", "policy", "delete", bucketName})
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&out)
	rootCmd.SetContext(context.Background())
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("policy delete: %v\noutput: %s", err, out.String())
	}
	if gotMethod != http.MethodDelete {
		t.Errorf("policy delete: expected DELETE, got %s", gotMethod)
	}
}
