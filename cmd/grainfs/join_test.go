package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newJoinTestCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "join <peer>", Args: cobra.ExactArgs(1), RunE: runJoin}
	registerAdminEndpointFlag(cmd)
	registerAdminTimeoutFlag(cmd)
	cmd.Flags().Bool("force", false, "")
	return cmd
}

func runJoinCmd(t *testing.T, sock string, peerAddr string, extraArgs ...string) (string, error) {
	t.Helper()
	cmd := newJoinTestCmd()
	out := &bytes.Buffer{}
	cmd.SetOut(out)
	cmd.SetErr(out)
	args := append([]string{"--endpoint", sock}, extraArgs...)
	args = append(args, peerAddr)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

type stubJoinServer struct {
	calls     atomic.Int32
	lastPeer  atomic.Value // string
	lastForce atomic.Bool
	response  map[string]any
}

func (s *stubJoinServer) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/cluster/join", func(w http.ResponseWriter, r *http.Request) {
		s.calls.Add(1)
		var req struct {
			PeerAddr string `json:"peer_addr"`
			Force    bool   `json:"force"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		s.lastPeer.Store(req.PeerAddr)
		s.lastForce.Store(req.Force)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(s.response)
	})
	return mux
}

func TestJoinCmd_RestartInitiated(t *testing.T) {
	stub := &stubJoinServer{
		response: map[string]any{
			"status":  "restart_initiated",
			"message": "node will restart and join 127.0.0.1:8300",
		},
	}
	sock := startUDSStubServer(t, stub.handler())

	out, err := runJoinCmd(t, sock, "127.0.0.1:8300")
	require.NoError(t, err, out)
	assert.Equal(t, int32(1), stub.calls.Load())
	assert.Equal(t, "127.0.0.1:8300", stub.lastPeer.Load())
	assert.Contains(t, out, "restart_initiated")
}

func TestJoinCmd_AlreadyMember(t *testing.T) {
	stub := &stubJoinServer{
		response: map[string]any{
			"status":  "already_member",
			"message": "node is already part of a multi-node cluster",
		},
	}
	sock := startUDSStubServer(t, stub.handler())

	out, err := runJoinCmd(t, sock, "127.0.0.1:8300")
	require.NoError(t, err, out)
	assert.Contains(t, strings.ToLower(out), "already_member")
}

func TestJoinCmd_ForceFlag_PropagatedToServer(t *testing.T) {
	stub := &stubJoinServer{
		response: map[string]any{"status": "restart_initiated"},
	}
	sock := startUDSStubServer(t, stub.handler())

	_, err := runJoinCmd(t, sock, "127.0.0.1:8300", "--force")
	require.NoError(t, err)
	assert.True(t, stub.lastForce.Load(), "force=true must be sent to server")
}

func TestJoinCmd_NoForceFlag_SendsFalse(t *testing.T) {
	stub := &stubJoinServer{
		response: map[string]any{"status": "restart_initiated"},
	}
	sock := startUDSStubServer(t, stub.handler())

	_, err := runJoinCmd(t, sock, "127.0.0.1:8300")
	require.NoError(t, err)
	assert.False(t, stub.lastForce.Load(), "force must default to false")
}
