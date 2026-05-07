package serveruntime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	hzserver "github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/transport"
)

// 회전 명령 socket — D19 enforces localhost-only via Unix domain socket
// permissions (0600, owner-only). 외부 네트워크에 절대 노출되지 않으므로
// PSK 누설 위험 없이 새 키를 운반할 수 있다. The transport is HTTP/Hertz on
// a dedicated UDS so the CLI shares one HTTP client with cluster admin
// while keeping the stricter file-mode boundary (0600 vs admin.sock 0660).
const RotationSocketName = "rotate.sock"

// RotationBeginRequest is the JSON body for POST /v1/rotate-key/begin.
type RotationBeginRequest struct {
	NewKey string `json:"new_key"`
}

// RotationAbortRequest is the JSON body for POST /v1/rotate-key/abort.
type RotationAbortRequest struct {
	Reason string `json:"reason"`
}

// RotationStatusResponse is returned by all three rotate-key endpoints.
// Error is non-empty when the server rejects the request; the HTTP status
// code is 200 in all cases (matches the line-delimited socket's prior
// contract — clients branch on Error string).
type RotationStatusResponse struct {
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

	h := hzserver.New(
		hzserver.WithListener(ln),
		hzserver.WithTransport(standard.NewTransporter),
		hzserver.WithHostPorts(""),
	)
	registerRotationRoutes(h, dataDir, metaRaft)

	go func() {
		_ = h.Run()
	}()

	go func() {
		<-ctx.Done()
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = h.Shutdown(stopCtx)
		_ = os.Remove(sockPath)
	}()
	return nil
}

func registerRotationRoutes(h *hzserver.Hertz, dataDir string, metaRaft *cluster.MetaRaft) {
	g := h.Group("/v1/rotate-key")
	g.GET("/status", func(c context.Context, ctx *app.RequestContext) {
		resp := rotationStatusResp(metaRaft)
		ctx.JSON(200, resp)
	})
	g.POST("/begin", func(c context.Context, ctx *app.RequestContext) {
		var req RotationBeginRequest
		if err := json.Unmarshal(ctx.Request.Body(), &req); err != nil {
			ctx.JSON(200, RotationStatusResponse{Error: fmt.Sprintf("decode: %v", err)})
			return
		}
		ctx.JSON(200, rotationBegin(c, req.NewKey, dataDir, metaRaft))
	})
	g.POST("/abort", func(c context.Context, ctx *app.RequestContext) {
		var req RotationAbortRequest
		if err := json.Unmarshal(ctx.Request.Body(), &req); err != nil {
			ctx.JSON(200, RotationStatusResponse{Error: fmt.Sprintf("decode: %v", err)})
			return
		}
		ctx.JSON(200, rotationAbort(c, req.Reason, metaRaft))
	})
}

func rotationStatusResp(metaRaft *cluster.MetaRaft) RotationStatusResponse {
	st := metaRaft.RotationState()
	out := RotationStatusResponse{Phase: st.Phase}
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

func rotationBegin(ctx context.Context, newKey, dataDir string, metaRaft *cluster.MetaRaft) RotationStatusResponse {
	if !metaRaft.IsLeader() {
		return RotationStatusResponse{Error: "rotate-key begin must be issued on the meta-raft leader (current leader: " + metaRaft.LeaderID() + ")"}
	}
	if st := metaRaft.RotationState(); st.Phase != cluster.PhaseSteady {
		return RotationStatusResponse{Error: fmt.Sprintf("rotation already in progress (rotation_id=%x, phase=%d); abort first", st.RotationID, st.Phase)}
	}
	if len(newKey) != 64 {
		return RotationStatusResponse{Error: fmt.Sprintf("new_key must be 64 hex chars (32 bytes), got %d", len(newKey))}
	}
	if _, err := hex.DecodeString(newKey); err != nil {
		return RotationStatusResponse{Error: fmt.Sprintf("new_key not valid hex: %v", err)}
	}
	_, newSPKI, err := transport.DeriveClusterIdentity(newKey)
	if err != nil {
		return RotationStatusResponse{Error: fmt.Sprintf("derive identity: %v", err)}
	}
	ks := transport.NewKeystore(dataDir)
	if err := ks.WriteNext(newKey); err != nil {
		return RotationStatusResponse{Error: fmt.Sprintf("write next.key: %v", err)}
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
		return RotationStatusResponse{Error: fmt.Sprintf("ProposeRotateKeyBegin: %v", err)}
	}
	st := metaRaft.RotationState()
	return RotationStatusResponse{
		Phase:      st.Phase,
		RotationID: hex.EncodeToString(cmd.RotationID[:]),
		OldSPKI:    hex.EncodeToString(st.OldSPKI[:]),
		NewSPKI:    hex.EncodeToString(newSPKI[:]),
	}
}

func rotationAbort(ctx context.Context, reason string, metaRaft *cluster.MetaRaft) RotationStatusResponse {
	if !metaRaft.IsLeader() {
		return RotationStatusResponse{Error: "rotate-key abort must be issued on the meta-raft leader (current leader: " + metaRaft.LeaderID() + ")"}
	}
	st := metaRaft.RotationState()
	if st.Phase == cluster.PhaseSteady {
		return RotationStatusResponse{Error: "no rotation in progress"}
	}
	if reason == "" {
		reason = "operator"
	}
	cmd := cluster.RotateKeyAbort{RotationID: st.RotationID, Reason: reason}
	propCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := metaRaft.ProposeRotateKeyAbort(propCtx, cmd); err != nil {
		return RotationStatusResponse{Error: fmt.Sprintf("ProposeRotateKeyAbort: %v", err)}
	}
	final := metaRaft.RotationState()
	return RotationStatusResponse{Phase: final.Phase}
}
