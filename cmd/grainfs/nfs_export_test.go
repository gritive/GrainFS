package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
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

	require.NoError(t, root.Execute(), out.String())
	require.Equal(t, "POST", gotMethod)
	require.Equal(t, "my-data", gotBody["bucket"])
	require.Equal(t, false, gotBody["read_only"])
	require.Contains(t, out.String(), `added NFS export "my-data"`)
}

func TestNfsExportAddCmdDryRun(t *testing.T) {
	root := buildTestNfsRoot()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetContext(context.Background())
	root.SetArgs([]string{"nfs", "export", "add", "my-data", "--dry-run"})
	require.NoError(t, root.Execute())
	require.Contains(t, out.String(), `Would add export 'my-data' (rw)`)
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
	require.NoError(t, root.Execute())
	require.Equal(t, "DELETE", gotMethod)
	require.Contains(t, out.String(), `removed NFS export "my-data"`)
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
	require.NoError(t, root.Execute())
	require.Equal(t, "PATCH", gotMethod)
	require.Equal(t, true, gotBody["read_only"])
	require.Contains(t, out.String(), `updated NFS export "my-data"`)
}

func TestNfsExportUpdateCmdRequiresMode(t *testing.T) {
	root := buildTestNfsRoot()
	root.SetArgs([]string{"nfs", "export", "update", "my-data"})
	err := root.Execute()
	require.Error(t, err)
	require.Contains(t, err.Error(), "--ro or --rw")
}

func TestNfsExportUpdateCmdRejectsQuiesceWaitWithRW(t *testing.T) {
	root := buildTestNfsRoot()
	root.SetArgs([]string{"nfs", "export", "update", "my-data", "--rw", "--quiesce-wait", "5s"})
	err := root.Execute()
	require.Error(t, err)
	require.Contains(t, err.Error(), "--quiesce-wait only applies")
}

func TestNfsExportUpdateCmdMutexRoRw(t *testing.T) {
	root := buildTestNfsRoot()
	root.SetArgs([]string{"nfs", "export", "update", "my-data", "--ro", "--rw"})
	err := root.Execute()
	require.Error(t, err)
}

func TestNfsExportQuietRejectsJSON(t *testing.T) {
	root := buildTestNfsRoot()
	root.SetArgs([]string{"nfs", "export", "add", "my-data", "--dry-run", "--quiet", "--format", "json"})
	err := root.Execute()
	require.Error(t, err)
	require.Contains(t, err.Error(), "--quiet")
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
	require.NoError(t, root.Execute())
	require.Contains(t, out.String(), "BUCKET")
	require.Contains(t, out.String(), "b1")
}
