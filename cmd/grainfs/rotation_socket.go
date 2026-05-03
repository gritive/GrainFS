package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/transport"
)

// 회전 명령 socket — D19 enforces localhost-only via Unix domain socket
// permissions (0600, owner-only). 외부 네트워크에 절대 노출되지 않으므로
// PSK 누설 위험 없이 새 키를 운반할 수 있다.
const rotationSocketName = "rotate.sock"

// rotationSocketRequest is the JSON envelope wire format. Action determines
// which fields are required:
//   - "begin": new_key (64-char hex, 32-byte PSK after decoding)
//   - "status": no fields
//   - "abort": reason (string)
type rotationSocketRequest struct {
	Action string `json:"action"`
	NewKey string `json:"new_key,omitempty"`
	Reason string `json:"reason,omitempty"`
}

type rotationSocketResponse struct {
	Phase      int    `json:"phase"`
	RotationID string `json:"rotation_id,omitempty"`
	OldSPKI    string `json:"old_spki,omitempty"`
	NewSPKI    string `json:"new_spki,omitempty"`
	Error      string `json:"error,omitempty"`
}

// startRotationSocket binds a Unix domain socket at $dataDir/rotate.sock and
// services rotation requests until ctx is cancelled. Returns an error only
// for fatal listener-creation failures; per-request errors are encoded into
// the response payload.
func startRotationSocket(ctx context.Context, dataDir string, metaRaft *cluster.MetaRaft) error {
	if metaRaft == nil {
		return nil // solo mode — no cluster, nothing to rotate
	}
	sockPath := filepath.Join(dataDir, rotationSocketName)
	// Remove a stale socket file from a previous crashed process. This is
	// safe — only the current grainfs process should hold this name.
	_ = os.Remove(sockPath)
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		return fmt.Errorf("rotate.sock: listen: %w", err)
	}
	if err := os.Chmod(sockPath, 0600); err != nil {
		ln.Close()
		return fmt.Errorf("rotate.sock: chmod: %w", err)
	}
	log.Info().Str("socket", sockPath).Msg("rotation socket listening")

	go func() {
		<-ctx.Done()
		ln.Close()
		_ = os.Remove(sockPath)
	}()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					return
				}
				log.Warn().Err(err).Msg("rotate.sock: accept error")
				continue
			}
			go handleRotationConn(ctx, conn, dataDir, metaRaft)
		}
	}()
	return nil
}

func handleRotationConn(ctx context.Context, conn net.Conn, dataDir string, metaRaft *cluster.MetaRaft) {
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(40 * time.Second))

	var req rotationSocketRequest
	if err := json.NewDecoder(conn).Decode(&req); err != nil {
		writeRotResp(conn, rotationSocketResponse{Error: fmt.Sprintf("decode: %v", err)})
		return
	}
	resp := dispatchRotationRequest(ctx, req, dataDir, metaRaft)
	writeRotResp(conn, resp)
}

func dispatchRotationRequest(ctx context.Context, req rotationSocketRequest, dataDir string, metaRaft *cluster.MetaRaft) rotationSocketResponse {
	switch req.Action {
	case "status":
		return rotationStatusResp(metaRaft)
	case "begin":
		return rotationBegin(ctx, req.NewKey, dataDir, metaRaft)
	case "abort":
		return rotationAbort(ctx, req.Reason, metaRaft)
	default:
		return rotationSocketResponse{Error: fmt.Sprintf("unknown action %q", req.Action)}
	}
}

func rotationStatusResp(metaRaft *cluster.MetaRaft) rotationSocketResponse {
	st := metaRaft.RotationState()
	out := rotationSocketResponse{Phase: st.Phase}
	if st.RotationID != ([16]byte{}) {
		out.RotationID = hex.EncodeToString(st.RotationID[:])
	}
	if st.OldSPKI != ([32]byte{}) {
		out.OldSPKI = hex.EncodeToString(st.OldSPKI[:])
	}
	if st.NewSPKI != ([32]byte{}) {
		out.NewSPKI = hex.EncodeToString(st.NewSPKI[:])
	}
	return out
}

func rotationBegin(ctx context.Context, newKey, dataDir string, metaRaft *cluster.MetaRaft) rotationSocketResponse {
	if !metaRaft.IsLeader() {
		return rotationSocketResponse{Error: "rotate-key begin must be issued on the meta-raft leader (current leader: " + metaRaft.LeaderID() + ")"}
	}
	if len(newKey) != 64 {
		return rotationSocketResponse{Error: fmt.Sprintf("new_key must be 64 hex chars (32 bytes), got %d", len(newKey))}
	}
	if _, err := hex.DecodeString(newKey); err != nil {
		return rotationSocketResponse{Error: fmt.Sprintf("new_key not valid hex: %v", err)}
	}
	// Derive SPKI from the operator-supplied PSK.
	_, newSPKI, err := transport.DeriveClusterIdentity(newKey)
	if err != nil {
		return rotationSocketResponse{Error: fmt.Sprintf("derive identity: %v", err)}
	}
	// Write keys.d/next.key BEFORE submitting raft cmd. Workers verify SPKI
	// matches before swapping; if write fails here, no FSM state changes.
	ks := transport.NewKeystore(dataDir)
	if err := ks.WriteNext(newKey); err != nil {
		return rotationSocketResponse{Error: fmt.Sprintf("write next.key: %v", err)}
	}
	rotID := uuid.New() // UUID v4 (uuid.New returns v4); 16 bytes of crypto-random — meets D15 idempotency
	auditHash := sha256.Sum256(newSPKI[:])
	cmd := cluster.RotateKeyBegin{
		ExpectedNewSPKI:  newSPKI,
		AuditNewSPKIHash: auditHash,
		StartedBy:        metaRaft.LeaderID(),
		Capabilities:     cluster.CapRotationV1,
	}
	copy(cmd.RotationID[:], rotID[:])
	propCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := metaRaft.ProposeRotateKeyBegin(propCtx, cmd); err != nil {
		// Roll back the next.key write so a retry isn't blocked by stale state.
		_ = ks.DeleteNext()
		return rotationSocketResponse{Error: fmt.Sprintf("ProposeRotateKeyBegin: %v", err)}
	}
	st := metaRaft.RotationState()
	return rotationSocketResponse{
		Phase:      st.Phase,
		RotationID: hex.EncodeToString(cmd.RotationID[:]),
		OldSPKI:    hex.EncodeToString(st.OldSPKI[:]),
		NewSPKI:    hex.EncodeToString(newSPKI[:]),
	}
}

func rotationAbort(ctx context.Context, reason string, metaRaft *cluster.MetaRaft) rotationSocketResponse {
	if !metaRaft.IsLeader() {
		return rotationSocketResponse{Error: "rotate-key abort must be issued on the meta-raft leader (current leader: " + metaRaft.LeaderID() + ")"}
	}
	st := metaRaft.RotationState()
	if st.Phase == cluster.PhaseSteady {
		return rotationSocketResponse{Error: "no rotation in progress"}
	}
	if reason == "" {
		reason = "operator"
	}
	cmd := cluster.RotateKeyAbort{RotationID: st.RotationID, Reason: reason}
	propCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := metaRaft.ProposeRotateKeyAbort(propCtx, cmd); err != nil {
		return rotationSocketResponse{Error: fmt.Sprintf("ProposeRotateKeyAbort: %v", err)}
	}
	final := metaRaft.RotationState()
	return rotationSocketResponse{Phase: final.Phase}
}

func writeRotResp(conn net.Conn, resp rotationSocketResponse) {
	_ = json.NewEncoder(conn).Encode(resp)
}
