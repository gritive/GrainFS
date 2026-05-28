package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCmdRevokeNode_PostsNodeID(t *testing.T) {
	var calls atomic.Int32
	var lastBody map[string]any
	sock := startUDSStubServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/v1/cluster/revoke-node", r.URL.Path)
		calls.Add(1)
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(body, &lastBody))
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok", "message": "node revoked"})
	}))

	cmd := clusterRevokeNodeCmd()
	cmd.Flags().String("endpoint", "", "")
	out := &bytes.Buffer{}
	cmd.SetOut(out)
	cmd.SetArgs([]string{"--endpoint", sock, "node-b"})

	require.NoError(t, cmd.Execute())
	require.Equal(t, int32(1), calls.Load())
	require.Equal(t, "node-b", lastBody["node_id"])
	require.Contains(t, strings.TrimSpace(out.String()), "Zero-CA node revoked: node-b")
}
