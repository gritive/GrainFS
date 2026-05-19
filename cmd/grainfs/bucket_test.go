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
	"github.com/stretchr/testify/require"
)

// buildTestBucketRoot returns a fresh root+bucket command tree (no globals touched).
func buildTestBucketRoot() *cobra.Command {
	root := &cobra.Command{Use: "grainfs"}
	bkt := &cobra.Command{Use: "bucket"}
	bkt.PersistentFlags().String("endpoint", "", "")
	bkt.PersistentFlags().Bool("json", false, "")
	bkt.AddCommand(bucketCreateCmd(), bucketListCmd(), bucketDeleteCmd(), bucketInfoCmd())
	root.AddCommand(bkt)
	return root
}

func TestBucketCreateCmd(t *testing.T) {
	var gotMethod, gotPath string
	var gotBody map[string]string

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets", func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		fmt.Fprint(w, `{"name":"test-bucket"}`)
	})
	sock := startFakeAdminUDS(t, mux)

	root := buildTestBucketRoot()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetErr(&buf)
	root.SetContext(context.Background())
	root.SetArgs([]string{"bucket", "--endpoint", sock, "create", "test-bucket"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, buf.String())
	}
	if gotMethod != "POST" {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/v1/buckets" {
		t.Errorf("path = %q, want /v1/buckets", gotPath)
	}
	if gotBody["name"] != "test-bucket" {
		t.Errorf("body name = %q, want test-bucket", gotBody["name"])
	}
	if !strings.Contains(buf.String(), "test-bucket") {
		t.Errorf("output %q should contain test-bucket", buf.String())
	}
}

func TestBucketCreateCmd_AttachPolicy(t *testing.T) {
	var gotBody map[string]string

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		fmt.Fprint(w, `{"name":"test-bucket"}`)
	})
	sock := startFakeAdminUDS(t, mux)

	root := buildTestBucketRoot()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetErr(&buf)
	root.SetContext(context.Background())
	root.SetArgs([]string{
		"bucket", "--endpoint", sock,
		"create", "test-bucket",
		"--attach-sa", "sa-bench",
		"--attach-policy", "bucket-admin",
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, buf.String())
	}
	if gotBody["name"] != "test-bucket" {
		t.Errorf("body name = %q, want test-bucket", gotBody["name"])
	}
	if gotBody["attach_sa"] != "sa-bench" {
		t.Errorf("body attach_sa = %q, want sa-bench", gotBody["attach_sa"])
	}
	if gotBody["attach_policy"] != "bucket-admin" {
		t.Errorf("body attach_policy = %q, want bucket-admin", gotBody["attach_policy"])
	}
}

func TestBucketListCmd(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"buckets":[{"name":"alpha"},{"name":"beta"}]}`)
	})
	sock := startFakeAdminUDS(t, mux)

	root := buildTestBucketRoot()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetErr(&buf)
	root.SetContext(context.Background())
	root.SetArgs([]string{"bucket", "--endpoint", sock, "list"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, buf.String())
	}
	if !strings.Contains(buf.String(), "alpha") || !strings.Contains(buf.String(), "beta") {
		t.Errorf("output %q should contain alpha and beta", buf.String())
	}
}

func TestBucketDeleteCmd(t *testing.T) {
	var gotMethod, gotPath string

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/del-me", func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusNoContent)
	})
	sock := startFakeAdminUDS(t, mux)

	root := buildTestBucketRoot()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetErr(&buf)
	root.SetContext(context.Background())
	root.SetArgs([]string{"bucket", "--endpoint", sock, "delete", "del-me"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, buf.String())
	}
	if gotMethod != "DELETE" {
		t.Errorf("method = %q, want DELETE", gotMethod)
	}
	if gotPath != "/v1/buckets/del-me" {
		t.Errorf("path = %q, want /v1/buckets/del-me", gotPath)
	}
}

func TestBucketDeleteCmd_Force(t *testing.T) {
	var gotQuery string

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/del-me", func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		w.WriteHeader(http.StatusNoContent)
	})
	sock := startFakeAdminUDS(t, mux)

	root := buildTestBucketRoot()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetErr(&buf)
	root.SetContext(context.Background())
	root.SetArgs([]string{"bucket", "--endpoint", sock, "delete", "--force", "del-me"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, buf.String())
	}
	if gotQuery != "force=true" {
		t.Errorf("query = %q, want force=true", gotQuery)
	}
}

func TestBucketListCmd_Table(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"buckets":[{"name":"alpha"},{"name":"beta"}]}`)
	})
	sock := startFakeAdminUDS(t, mux)

	root := buildTestBucketRoot()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetErr(&buf)
	root.SetContext(context.Background())
	root.SetArgs([]string{"bucket", "--endpoint", sock, "list"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, buf.String())
	}
	out := buf.String()
	if !strings.Contains(out, "NAME") {
		t.Errorf("output %q missing NAME header", out)
	}
	if !strings.Contains(out, "alpha") || !strings.Contains(out, "beta") {
		t.Errorf("output %q missing bucket names", out)
	}
}

func TestBucketListCmd_JSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"buckets":[{"name":"alpha"}]}`)
	})
	sock := startFakeAdminUDS(t, mux)

	root := buildTestBucketRoot()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetErr(&buf)
	root.SetContext(context.Background())
	root.SetArgs([]string{"bucket", "--endpoint", sock, "--json", "list"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, buf.String())
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &parsed); err != nil {
		t.Errorf("output is not valid JSON: %v\noutput: %s", err, buf.String())
	}
}

func TestBucketDeleteCmd_PrintsFeedback(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/my-bucket", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	sock := startFakeAdminUDS(t, mux)

	root := buildTestBucketRoot()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetErr(&buf)
	root.SetContext(context.Background())
	root.SetArgs([]string{"bucket", "--endpoint", sock, "delete", "my-bucket"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, buf.String())
	}
	if !strings.Contains(buf.String(), "Deleted bucket my-bucket") {
		t.Errorf("output %q missing delete feedback", buf.String())
	}
}

func TestBucketCreateCmd_TextFeedback(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		fmt.Fprint(w, `{"name":"new-bucket"}`)
	})
	sock := startFakeAdminUDS(t, mux)

	root := buildTestBucketRoot()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetErr(&buf)
	root.SetContext(context.Background())
	root.SetArgs([]string{"bucket", "--endpoint", sock, "create", "new-bucket"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, buf.String())
	}
	if !strings.Contains(buf.String(), "Created bucket new-bucket") {
		t.Errorf("output %q missing create feedback", buf.String())
	}
}

func TestBucketInfoCmd_Table(t *testing.T) {
	count := int64(42)
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/my-bucket", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"name":"my-bucket","object_count":%d}`, count)
	})
	sock := startFakeAdminUDS(t, mux)

	root := buildTestBucketRoot()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetErr(&buf)
	root.SetContext(context.Background())
	root.SetArgs([]string{"bucket", "--endpoint", sock, "info", "my-bucket"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, buf.String())
	}
	out := buf.String()
	if !strings.Contains(out, "NAME") || !strings.Contains(out, "OBJECTS") {
		t.Errorf("output %q missing table headers", out)
	}
	if !strings.Contains(out, "my-bucket") || !strings.Contains(out, "42") {
		t.Errorf("output %q missing bucket info", out)
	}
}

func TestBucketInfoCmd_JSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/my-bucket", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"name":"my-bucket","object_count":7}`)
	})
	sock := startFakeAdminUDS(t, mux)

	root := buildTestBucketRoot()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetErr(&buf)
	root.SetContext(context.Background())
	root.SetArgs([]string{"bucket", "--endpoint", sock, "--json", "info", "my-bucket"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, buf.String())
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &parsed); err != nil {
		t.Errorf("output is not valid JSON: %v\noutput: %s", err, buf.String())
	}
}

func TestBucketCreateCmd_JSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		fmt.Fprint(w, `{"name":"new-bucket"}`)
	})
	sock := startFakeAdminUDS(t, mux)

	root := buildTestBucketRoot()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetErr(&buf)
	root.SetContext(context.Background())
	root.SetArgs([]string{"bucket", "--endpoint", sock, "--json", "create", "new-bucket"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, buf.String())
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &parsed); err != nil {
		t.Errorf("output is not valid JSON: %v\noutput: %s", err, buf.String())
	}
}

func TestBucketListCmd_HasUpstreamColumn(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"buckets":[{"name":"alpha","has_upstream":true},{"name":"beta","has_upstream":false}]}`)
	})
	sock := startFakeAdminUDS(t, mux)

	root := buildTestBucketRoot()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetContext(context.Background())
	root.SetArgs([]string{"bucket", "--endpoint", sock, "list"})
	require.NoError(t, root.Execute())

	out := buf.String()
	if !strings.Contains(out, "HAS_UPSTREAM") {
		t.Errorf("output %q missing HAS_UPSTREAM header", out)
	}
	if !strings.Contains(out, "yes") {
		t.Errorf("output %q missing 'yes' for alpha", out)
	}
}

func TestBucketInfoCmd_Enriched(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/my-bucket", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"name":"my-bucket","object_count":42,"has_upstream":true,"versioning":"Enabled"}`)
	})
	sock := startFakeAdminUDS(t, mux)

	root := buildTestBucketRoot()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetContext(context.Background())
	root.SetArgs([]string{"bucket", "--endpoint", sock, "info", "my-bucket"})
	require.NoError(t, root.Execute())

	out := buf.String()
	if !strings.Contains(out, "VERSIONING") {
		t.Errorf("output %q missing VERSIONING header", out)
	}
	if !strings.Contains(out, "HAS_UPSTREAM") {
		t.Errorf("output %q missing HAS_UPSTREAM header", out)
	}
	if !strings.Contains(out, "Enabled") {
		t.Errorf("output %q missing versioning value", out)
	}
	if !strings.Contains(out, "yes") {
		t.Errorf("output %q missing 'yes' for has_upstream", out)
	}
}

func TestBucketInfoCmd_NotFound(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/missing", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"code":"not_found","message":"bucket not found"}`)
	})
	sock := startFakeAdminUDS(t, mux)

	root := buildTestBucketRoot()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetErr(&buf)
	root.SetContext(context.Background())
	root.SetArgs([]string{"bucket", "--endpoint", sock, "info", "missing"})
	err := root.Execute()
	if err == nil {
		t.Fatalf("expected error for not_found, got none; output: %s", buf.String())
	}
}
