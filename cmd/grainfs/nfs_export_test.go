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

func buildTestNfsRoot() *cobra.Command {
	root := &cobra.Command{Use: "grainfs"}
	nfs := &cobra.Command{Use: "nfs"}
	registerAdminEndpointFlag(nfs)
	registerAdminTimeoutFlag(nfs)
	nfs.PersistentFlags().String("format", "text", "")
	nfs.AddCommand(nfsExportCmd())
	root.AddCommand(nfs)
	return root
}

func TestNfsExportAddCmdHappyPath(t *testing.T) {
	var gotMethod string
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/nfs/exports", func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"bucket":"my-data","read_only":false,"fsid_major":1,"fsid_minor":2,"generation":1}`)
	})
	sock := startFakeAdminUDS(t, mux)
	root := buildTestNfsRoot()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetContext(context.Background())
	root.SetArgs([]string{"nfs", "--endpoint", sock, "export", "add", "my-data"})

	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v\n%s", err, out.String())
	}
	if gotMethod != "POST" || gotBody["bucket"] != "my-data" || gotBody["read_only"] != false {
		t.Fatalf("unexpected request: method=%s body=%v", gotMethod, gotBody)
	}
	if !strings.Contains(out.String(), `added NFS export "my-data"`) {
		t.Fatalf("unexpected output: %s", out.String())
	}
}

func TestNfsExportAddCmdDryRun(t *testing.T) {
	root := buildTestNfsRoot()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetContext(context.Background())
	root.SetArgs([]string{"nfs", "export", "add", "my-data", "--dry-run"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !strings.Contains(out.String(), `would add NFS export "my-data"`) {
		t.Fatalf("unexpected output: %s", out.String())
	}
}

func TestNfsExportRemoveCmdHappyPath(t *testing.T) {
	var gotMethod string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/nfs/exports/my-data", func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		w.WriteHeader(http.StatusNoContent)
	})
	sock := startFakeAdminUDS(t, mux)
	root := buildTestNfsRoot()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetContext(context.Background())
	root.SetArgs([]string{"nfs", "--endpoint", sock, "export", "remove", "my-data"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if gotMethod != "DELETE" {
		t.Fatalf("method=%s", gotMethod)
	}
	if !strings.Contains(out.String(), `removed NFS export "my-data"`) {
		t.Fatalf("unexpected output: %s", out.String())
	}
}

func TestNfsExportUpdateCmdRO(t *testing.T) {
	var gotMethod string
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/nfs/exports/my-data", func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"bucket":"my-data","read_only":true,"fsid_major":1,"fsid_minor":2,"generation":2}`)
	})
	sock := startFakeAdminUDS(t, mux)
	root := buildTestNfsRoot()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetContext(context.Background())
	root.SetArgs([]string{"nfs", "--endpoint", sock, "export", "update", "my-data", "--ro"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if gotMethod != "PATCH" || gotBody["read_only"] != true {
		t.Fatalf("unexpected request: %s %v", gotMethod, gotBody)
	}
	if !strings.Contains(out.String(), `updated NFS export "my-data"`) {
		t.Fatalf("unexpected output: %s", out.String())
	}
}

func TestNfsExportUpdateCmdRequiresMode(t *testing.T) {
	root := buildTestNfsRoot()
	root.SetArgs([]string{"nfs", "export", "update", "my-data"})
	err := root.Execute()
	if err == nil || !strings.Contains(err.Error(), "--ro or --rw") {
		t.Fatalf("expected mode error, got %v", err)
	}
}

func TestNfsExportUpdateCmdMutexRoRw(t *testing.T) {
	root := buildTestNfsRoot()
	root.SetArgs([]string{"nfs", "export", "update", "my-data", "--ro", "--rw"})
	err := root.Execute()
	if err == nil {
		t.Fatal("expected mutually exclusive flag error")
	}
}

func TestNfsExportQuietRejectsJSON(t *testing.T) {
	root := buildTestNfsRoot()
	root.SetArgs([]string{"nfs", "export", "add", "my-data", "--dry-run", "--quiet", "--format", "json"})
	err := root.Execute()
	if err == nil || !strings.Contains(err.Error(), "--quiet") {
		t.Fatalf("expected quiet/json error, got %v", err)
	}
}

func TestNfsExportListCmd(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/nfs/exports", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"exports":[{"bucket":"b1","read_only":false,"fsid_major":1,"fsid_minor":2,"generation":3}]}`)
	})
	sock := startFakeAdminUDS(t, mux)
	root := buildTestNfsRoot()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetContext(context.Background())
	root.SetArgs([]string{"nfs", "--endpoint", sock, "export", "list"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !strings.Contains(out.String(), "BUCKET") || !strings.Contains(out.String(), "b1") {
		t.Fatalf("unexpected output: %s", out.String())
	}
}
