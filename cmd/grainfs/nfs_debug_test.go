package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func buildTestNFSDebugRoot() *cobra.Command {
	root := &cobra.Command{Use: "grainfs"}
	nfs := &cobra.Command{Use: "nfs"}
	registerAdminEndpointFlag(nfs)
	registerAdminTimeoutFlag(nfs)
	nfs.AddCommand(nfsDebugCmd())
	root.AddCommand(nfs)
	return root
}

func TestNFSDebugCmdText(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/nfs/exports/my-data/debug", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"bucket":"my-data","registered":true,"read_only":false,"fsid_major":1,"fsid_minor":2,"generation":3,"backend_bucket":{"exists":true,"object_count":9},"propagation":{"applied_nodes":["node-a"],"total_nodes":1}}`)
	})
	sock := startFakeAdminUDS(t, mux)
	root := buildTestNFSDebugRoot()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetContext(context.Background())
	root.SetArgs([]string{"nfs", "--endpoint", sock, "debug", "my-data"})

	require.NoError(t, root.Execute())
	require.Contains(t, out.String(), "Bucket: my-data")
	require.Contains(t, out.String(), "registered (rw, fsid=1.2, gen=3)")
	require.Contains(t, out.String(), "Backend: exists (9 objects)")
}

func TestNFSDebugCmdJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/nfs/exports/my-data/debug", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"bucket":"my-data","registered":true,"backend_bucket":{"exists":true,"object_count":9},"propagation":{"total_nodes":0}}`)
	})
	sock := startFakeAdminUDS(t, mux)
	root := buildTestNFSDebugRoot()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetContext(context.Background())
	root.SetArgs([]string{"nfs", "--endpoint", sock, "debug", "my-data", "--json"})

	require.NoError(t, root.Execute())
	require.JSONEq(t, `{"bucket":"my-data","registered":true,"propagation":{"applied_nodes":null,"total_nodes":0},"backend_bucket":{"exists":true,"object_count":9}}`, out.String())
}
