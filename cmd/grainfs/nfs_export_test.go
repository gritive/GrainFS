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

func TestNfsExportSubcommandFlags(t *testing.T) {
	root := buildTestNfsRoot()
	for _, sub := range []string{"add", "remove", "update"} {
		t.Run(sub, func(t *testing.T) {
			cmd, _, err := root.Find([]string{"nfs", "export", sub})
			require.NoError(t, err)
			for _, name := range []string{"dry-run", "quiet", "json"} {
				require.NotNilf(t, cmd.Flags().Lookup(name), "subcommand %q missing --%s", sub, name)
			}
		})
	}
	t.Run("list", func(t *testing.T) {
		cmd, _, err := root.Find([]string{"nfs", "export", "list"})
		require.NoError(t, err)
		require.NotNil(t, cmd.Flags().Lookup("json"))
		require.Nil(t, cmd.Flags().Lookup("dry-run"))
		require.Nil(t, cmd.Flags().Lookup("quiet"))
	})
}

func TestNfsExportQuietRejectsJSON(t *testing.T) {
	for _, sub := range []string{"add", "remove", "update"} {
		t.Run(sub, func(t *testing.T) {
			root := buildTestNfsRoot()
			args := []string{"nfs", "export", sub, "my-data", "--dry-run", "--quiet", "--json"}
			if sub == "update" {
				args = append(args, "--ro")
			}
			root.SetArgs(args)
			err := root.Execute()
			require.Error(t, err)
			require.Contains(t, err.Error(), "--quiet")
			require.Contains(t, err.Error(), "--json")
		})
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
	require.NoError(t, root.Execute())
	require.Contains(t, out.String(), "BUCKET")
	require.Contains(t, out.String(), "b1")
}

func TestNfsExportListCmdJSON(t *testing.T) {
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
	root.SetArgs([]string{"nfs", "--endpoint", sock, "export", "list", "--json"})
	require.NoError(t, root.Execute())
	require.JSONEq(t, `{"exports":[{"bucket":"b1","read_only":false,"fsid_major":1,"fsid_minor":2,"generation":3}]}`, out.String())
}
