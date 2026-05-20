package iamadmin

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeTempPolicy(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	f := filepath.Join(dir, "policy.json")
	if err := os.WriteFile(f, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return f
}

func TestRunPolicyPut_SendsDoc(t *testing.T) {
	var gotBody []byte
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iam/policy/my-pol", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("method = %s", r.Method)
		}
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusNoContent)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject"],"Resource":"*"}]}`
	f := writeTempPolicy(t, doc)
	err := RunPolicyPut(context.Background(), PolicyPutOptions{
		BaseOptions: BaseOptions{Endpoint: srv.URL, Stdout: &bytes.Buffer{}},
		Name:        "my-pol",
		FilePath:    f,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(gotBody), "s3:GetObject") {
		t.Errorf("body = %s", gotBody)
	}
}

func TestRunPolicyPut_RejectsInvalid(t *testing.T) {
	f := writeTempPolicy(t, `{"NotAction":"oops"}`)
	err := RunPolicyPut(context.Background(), PolicyPutOptions{
		BaseOptions: BaseOptions{Endpoint: "http://localhost:1", Stdout: &bytes.Buffer{}},
		Name:        "bad",
		FilePath:    f,
	})
	if err == nil {
		t.Fatal("expected error for invalid policy")
	}
}

func TestRunPolicyList_Text(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iam/policy", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`["readonly","readwrite","my-pol"]`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	var out bytes.Buffer
	err := RunPolicyList(context.Background(), PolicyListOptions{
		BaseOptions: BaseOptions{Endpoint: srv.URL, Stdout: &out},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "readonly") || !strings.Contains(out.String(), "my-pol") {
		t.Errorf("output = %q", out.String())
	}
}

func TestRunPolicyDelete_OK(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iam/policy/my-pol", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("method = %s", r.Method)
		}
		w.WriteHeader(http.StatusNoContent)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	var out bytes.Buffer
	err := RunPolicyDelete(context.Background(), PolicyDeleteOptions{
		BaseOptions: BaseOptions{Endpoint: srv.URL, Stdout: &out},
		Name:        "my-pol",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "my-pol") {
		t.Errorf("output = %q", out.String())
	}
}

func TestRunPolicyAttach_SAWarns(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iam/policy/readonly", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject"],"Resource":"*"}]}`))
	})
	mux.HandleFunc("/v1/iam/policy/readonly/attach/sa/sa-x", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("method = %s", r.Method)
		}
		w.WriteHeader(http.StatusNoContent)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	var stderr bytes.Buffer
	err := RunPolicyAttach(context.Background(), PolicyAttachOptions{
		BaseOptions: BaseOptions{Endpoint: srv.URL, Stdout: &bytes.Buffer{}, Stderr: &stderr},
		PolicyName:  "readonly",
		SAID:        "sa-x",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stderr.String(), "Resource:*") {
		t.Errorf("expected Resource:* warning; stderr = %q", stderr.String())
	}
}

func TestRunPolicyAttach_IKnow_NoWarn(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iam/policy/readonly", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject"],"Resource":"*"}]}`))
	})
	mux.HandleFunc("/v1/iam/policy/readonly/attach/sa/sa-x", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	var stderr bytes.Buffer
	err := RunPolicyAttach(context.Background(), PolicyAttachOptions{
		BaseOptions: BaseOptions{Endpoint: srv.URL, Stdout: &bytes.Buffer{}, Stderr: &stderr},
		PolicyName:  "readonly",
		SAID:        "sa-x",
		IKnow:       true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(stderr.String(), "Resource:*") {
		t.Errorf("unexpected Resource:* warning with IKnow=true; stderr = %q", stderr.String())
	}
}

func TestRunPolicyValidate_Good(t *testing.T) {
	f := writeTempPolicy(t, `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject"],"Resource":"*"}]}`)
	var out bytes.Buffer
	err := RunPolicyValidate(context.Background(), PolicyValidateOptions{
		BaseOptions: BaseOptions{Stdout: &out},
		FilePath:    f,
	})
	if err != nil {
		t.Fatalf("expected no error for valid policy: %v", err)
	}
}

func TestRunPolicyValidate_Bad(t *testing.T) {
	f := writeTempPolicy(t, `{"NotAction":"bad"}`)
	err := RunPolicyValidate(context.Background(), PolicyValidateOptions{
		BaseOptions: BaseOptions{Stdout: &bytes.Buffer{}},
		FilePath:    f,
	})
	if err == nil {
		t.Fatal("expected error for invalid policy")
	}
}

func TestRunPolicySimulate_Text(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iam/policy/simulate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s", r.Method)
		}
		var req PolicySimulateRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.SAID != "sa-x" || req.Action != "s3:GetObject" {
			t.Errorf("req = %+v", req)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"effect":"Allow","matched_policy":"readonly","matched_sid":"sid1","reason":"explicit Allow"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	var out bytes.Buffer
	err := RunPolicySimulate(context.Background(), PolicySimulateOptions{
		BaseOptions: BaseOptions{Endpoint: srv.URL, Stdout: &out},
		SAID:        "sa-x",
		Action:      "s3:GetObject",
		Resource:    "arn:aws:s3:::my-bucket/*",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "Allow") {
		t.Errorf("output = %q", out.String())
	}
}
