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

func buildTestBucketVersioningRoot() *cobra.Command {
	root := &cobra.Command{Use: "grainfs"}
	bkt := &cobra.Command{Use: "bucket"}
	bkt.PersistentFlags().String("endpoint", "", "")
	bkt.PersistentFlags().Bool("json", false, "")
	bkt.AddCommand(bucketVersioningCmd())
	root.AddCommand(bkt)
	return root
}

func TestBucketVersioningGetCmd(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/my-bucket/versioning", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"status":"Enabled"}`)
	})
	sock := startFakeAdminUDS(t, mux)

	root := buildTestBucketVersioningRoot()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetContext(context.Background())
	root.SetArgs([]string{"bucket", "--endpoint", sock, "versioning", "get", "my-bucket"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, buf.String())
	}
	if !strings.Contains(buf.String(), "Enabled") {
		t.Errorf("output %q should contain 'Enabled'", buf.String())
	}
}

func TestBucketVersioningEnableCmd(t *testing.T) {
	var gotBody map[string]string

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/my-bucket/versioning", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "PUT" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusNoContent)
	})
	sock := startFakeAdminUDS(t, mux)

	root := buildTestBucketVersioningRoot()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetContext(context.Background())
	root.SetArgs([]string{"bucket", "--endpoint", sock, "versioning", "enable", "my-bucket"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, buf.String())
	}
	if gotBody["status"] != "Enabled" {
		t.Errorf("body status = %q, want Enabled", gotBody["status"])
	}
}

func TestBucketVersioningSuspendCmd(t *testing.T) {
	var gotBody map[string]string

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/my-bucket/versioning", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusNoContent)
	})
	sock := startFakeAdminUDS(t, mux)

	root := buildTestBucketVersioningRoot()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetContext(context.Background())
	root.SetArgs([]string{"bucket", "--endpoint", sock, "versioning", "suspend", "my-bucket"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, buf.String())
	}
	if gotBody["status"] != "Suspended" {
		t.Errorf("body status = %q, want Suspended", gotBody["status"])
	}
}
