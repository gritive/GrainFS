package serveruntime

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
// RotationSocketName is the path component (relative to dataDir) where the
// localhost-only rotation Unix domain socket is bound.
const RotationSocketName = "rotate.sock"

type RotationSocketRequest struct {
	Action string `json:"action"`
	NewKey string `json:"new_key,omitempty"`
	Reason string `json:"reason,omitempty"`
}

type RotationSocketResponse struct {
	Phase      int    `json:"phase"`
	RotationID string `json:"rotation_id,omitempty"`
	OldSPKI    string `json:"old_spki,omitempty"`
	NewSPKI    string `json:"new_spki,omitempty"`
	Error      string `json:"error,omitempty"`
}

// StartRotationSocket binds a Unix domain socket at $dataDir/rotate.sock and
// services rotation requests until ctx is cancelled. Returns an error only
// for fatal listener-creation failures; per-request errors are encoded into
// the response payload. nil metaRaft (solo mode) is a no-op.
func StartRotationSocket(ctx context.Context, dataDir string, metaRaft *cluster.MetaRaft) error {
	if metaRaft == nil {
		return nil
	}
	sockPath := filepath.Join(dataDir, RotationSocketName)
	_ = os.Remove(sockPath)
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		return fmt.Errorf("rotate.sock: listen: %w", err)
	}
	if err := os.Chmod(sockPath, 0600); err != nil {
		ln.Close()
		return fmt.Errorf("rotate.sock: chmod: %w", err)
	}
	log.Info().Str("path", sockPath).
		Str("hint", fmt.Sprintf("--endpoint %q", sockPath)).
		Msg("rotate endpoint")

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

	var req RotationSocketRequest
	if err := json.NewDecoder(conn).Decode(&req); err != nil {
		writeRotResp(conn, RotationSocketResponse{Error: fmt.Sprintf("decode: %v", err)})
		return
	}
	resp := dispatchRotationRequest(ctx, req, dataDir, metaRaft)
	writeRotResp(conn, resp)
}

func dispatchRotationRequest(ctx context.Context, req RotationSocketRequest, dataDir string, metaRaft *cluster.MetaRaft) RotationSocketResponse {
	switch req.Action {
	case "status":
		return rotationStatusResp(metaRaft)
	case "begin":
		return rotationBegin(ctx, req.NewKey, dataDir, metaRaft)
	case "abort":
		return rotationAbort(ctx, req.Reason, metaRaft)
	default:
		return RotationSocketResponse{Error: fmt.Sprintf("unknown action %q", req.Action)}
	}
}

func rotationStatusResp(metaRaft *cluster.MetaRaft) RotationSocketResponse {
	st := metaRaft.RotationState()
	out := RotationSocketResponse{Phase: st.Phase}
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

func rotationBegin(ctx context.Context, newKey, dataDir string, metaRaft *cluster.MetaRaft) RotationSocketResponse {
	if !metaRaft.IsLeader() {
		return RotationSocketResponse{Error: "rotate-key begin must be issued on the meta-raft leader (current leader: " + metaRaft.LeaderID() + ")"}
	}
	// Refuse if a rotation is already in progress. Without this guard a
	// repeated begin would (1) overwrite keys.d/next.key with the new PSK,
	// (2) hit FSM rejection ("rotation already in progress"), (3) hit the
	// rollback path below and DeleteNext — which would destroy the in-flight
	// rotation's key on the leader and break Switch (cluster network split).
	if st := metaRaft.RotationState(); st.Phase != cluster.PhaseSteady {
		return RotationSocketResponse{Error: fmt.Sprintf("rotation already in progress (rotation_id=%x, phase=%d); abort first", st.RotationID, st.Phase)}
	}
	if len(newKey) != 64 {
		return RotationSocketResponse{Error: fmt.Sprintf("new_key must be 64 hex chars (32 bytes), got %d", len(newKey))}
	}
	if _, err := hex.DecodeString(newKey); err != nil {
		return RotationSocketResponse{Error: fmt.Sprintf("new_key not valid hex: %v", err)}
	}
	_, newSPKI, err := transport.DeriveClusterIdentity(newKey)
	if err != nil {
		return RotationSocketResponse{Error: fmt.Sprintf("derive identity: %v", err)}
	}
	ks := transport.NewKeystore(dataDir)
	if err := ks.WriteNext(newKey); err != nil {
		return RotationSocketResponse{Error: fmt.Sprintf("write next.key: %v", err)}
	}
	rotID := uuid.New()
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
		if delErr := ks.DeleteNext(); delErr != nil {
			log.Warn().Err(delErr).Msg("rotate-key begin: failed to roll back next.key after propose error; manual cleanup may be required")
		}
		return RotationSocketResponse{Error: fmt.Sprintf("ProposeRotateKeyBegin: %v", err)}
	}
	st := metaRaft.RotationState()
	return RotationSocketResponse{
		Phase:      st.Phase,
		RotationID: hex.EncodeToString(cmd.RotationID[:]),
		OldSPKI:    hex.EncodeToString(st.OldSPKI[:]),
		NewSPKI:    hex.EncodeToString(newSPKI[:]),
	}
}

func rotationAbort(ctx context.Context, reason string, metaRaft *cluster.MetaRaft) RotationSocketResponse {
	if !metaRaft.IsLeader() {
		return RotationSocketResponse{Error: "rotate-key abort must be issued on the meta-raft leader (current leader: " + metaRaft.LeaderID() + ")"}
	}
	st := metaRaft.RotationState()
	if st.Phase == cluster.PhaseSteady {
		return RotationSocketResponse{Error: "no rotation in progress"}
	}
	if reason == "" {
		reason = "operator"
	}
	cmd := cluster.RotateKeyAbort{RotationID: st.RotationID, Reason: reason}
	propCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := metaRaft.ProposeRotateKeyAbort(propCtx, cmd); err != nil {
		return RotationSocketResponse{Error: fmt.Sprintf("ProposeRotateKeyAbort: %v", err)}
	}
	final := metaRaft.RotationState()
	return RotationSocketResponse{Phase: final.Phase}
}

func writeRotResp(conn net.Conn, resp RotationSocketResponse) {
	_ = json.NewEncoder(conn).Encode(resp)
}
