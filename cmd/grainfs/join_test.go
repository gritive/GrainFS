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
	cmd.Flags().Bool("confirm-staged-keys", false, "")
	return cmd
}

// runJoinCmd invokes the join command against an admin UDS stub. The
// --confirm-staged-keys gate is auto-injected because the existing tests
// exercise the request-issuing path, not the gate itself. Tests that need
// to exercise the refusal path build the cmd directly.
func runJoinCmd(t *testing.T, sock string, peerAddr string, extraArgs ...string) (string, error) {
	t.Helper()
	cmd := newJoinTestCmd()
	out := &bytes.Buffer{}
	cmd.SetOut(out)
	cmd.SetErr(out)
	args := append([]string{"--endpoint", sock, "--confirm-staged-keys"}, extraArgs...)
	args = append(args, peerAddr)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

type stubJoinServer struct {
	calls      atomic.Int32
	lastPeer   atomic.Value // string
	lastForce  atomic.Bool
	response   map[string]any
	statusCode int // 0 → 200
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
		code := s.statusCode
		if code == 0 {
			code = http.StatusOK
		}
		w.WriteHeader(code)
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

func TestJoinCmd_DataPresent_409_DisplaysFriendlyMessage(t *testing.T) {
	stub := &stubJoinServer{
		statusCode: http.StatusConflict,
		response: map[string]any{
			"status":  "data_present",
			"message": "solo node has user data; re-send with force=true to discard it and join",
		},
	}
	sock := startUDSStubServer(t, stub.handler())

	out, err := runJoinCmd(t, sock, "127.0.0.1:8300")
	require.Error(t, err)
	assert.Contains(t, out, "data_present")
	assert.Contains(t, out, "force=true")
}

// TestJoinCmd_RefusesWithoutConfirmStagedKeys verifies P4-H1 fix: in Phase A
// the runtime-join CLI refuses to issue an admin request until the operator
// explicitly acknowledges that <dataDir>/keys/0.key and <dataDir>/cluster.id
// have been pre-staged from a healthy peer. Without the gate the restarted
// node would boot with its own auto-generated keystore and fail the join
// handshake with a confusing KEK/cluster_id mismatch.
func TestJoinCmd_RefusesWithoutConfirmStagedKeys(t *testing.T) {
	// Stub server that should NEVER be called — the gate must short-circuit
	// before any HTTP request is issued.
	stub := &stubJoinServer{
		response: map[string]any{"status": "restart_initiated"},
	}
	sock := startUDSStubServer(t, stub.handler())

	cmd := newJoinTestCmd()
	out := &bytes.Buffer{}
	cmd.SetOut(out)
	cmd.SetErr(out)
	cmd.SetArgs([]string{"--endpoint", sock, "127.0.0.1:8300"})
	err := cmd.Execute()

	require.Error(t, err, "join must refuse without --confirm-staged-keys")
	assert.Contains(t, err.Error(), "confirm-staged-keys",
		"error must mention the required flag so the operator knows the remediation")
	assert.Contains(t, err.Error(), "keys/0.key",
		"error must mention the staging path so the operator knows what to scp")
	assert.Contains(t, err.Error(), "cluster.id",
		"error must mention cluster.id so the operator stages both files")
	assert.Equal(t, int32(0), stub.calls.Load(),
		"gate must short-circuit before issuing any admin request")
}
