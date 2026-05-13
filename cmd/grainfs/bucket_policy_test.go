package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func buildTestBucketPolicyRoot() *cobra.Command {
	root := &cobra.Command{Use: "grainfs"}
	bkt := &cobra.Command{Use: "bucket"}
	bkt.PersistentFlags().String("endpoint", "", "")
	bkt.PersistentFlags().Bool("json", false, "")
	bkt.AddCommand(bucketPolicyCmd())
	root.AddCommand(bkt)
	return root
}

func TestBucketPolicyGetCmd(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/my-bucket/policy", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"policy":{"Version":"2012-10-17","Statement":[]}}`)
	})
	sock := startFakeAdminUDS(t, mux)

	root := buildTestBucketPolicyRoot()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetContext(context.Background())
	root.SetArgs([]string{"bucket", "--endpoint", sock, "policy", "get", "my-bucket"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, buf.String())
	}
	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Errorf("output should be valid JSON: %v\n%s", err, buf.String())
	}
}

func TestBucketPolicySetCmd_File(t *testing.T) {
	var gotMethod string
	var gotBody map[string]any

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/my-bucket/policy", func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusNoContent)
	})
	sock := startFakeAdminUDS(t, mux)

	policyFile := t.TempDir() + "/policy.json"
	if err := writeTempFile(policyFile, `{"Version":"2012-10-17","Statement":[]}`); err != nil {
		t.Fatal(err)
	}

	root := buildTestBucketPolicyRoot()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetContext(context.Background())
	root.SetArgs([]string{"bucket", "--endpoint", sock, "policy", "set", "my-bucket", policyFile})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, buf.String())
	}
	if gotMethod != "PUT" {
		t.Errorf("method = %q, want PUT", gotMethod)
	}
	if gotBody["policy"] == nil {
		t.Errorf("body[\"policy\"] should not be nil, got %v", gotBody)
	}
}

func TestBucketPolicyDeleteCmd(t *testing.T) {
	var gotMethod string

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/my-bucket/policy", func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		w.WriteHeader(http.StatusNoContent)
	})
	sock := startFakeAdminUDS(t, mux)

	root := buildTestBucketPolicyRoot()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetContext(context.Background())
	root.SetArgs([]string{"bucket", "--endpoint", sock, "policy", "delete", "my-bucket"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, buf.String())
	}
	if gotMethod != "DELETE" {
		t.Errorf("method = %q, want DELETE", gotMethod)
	}
}

func TestBucketPolicySetCmd_Stdin(t *testing.T) {
	var gotBody map[string]any

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/my-bucket/policy", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusNoContent)
	})
	sock := startFakeAdminUDS(t, mux)

	root := buildTestBucketPolicyRoot()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetIn(strings.NewReader(`{"Version":"2012-10-17","Statement":[]}`))
	root.SetContext(context.Background())
	root.SetArgs([]string{"bucket", "--endpoint", sock, "policy", "set", "my-bucket", "-"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, buf.String())
	}
	if gotBody["policy"] == nil {
		t.Errorf("body[\"policy\"] should not be nil")
	}
}

func writeTempFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o600)
}
