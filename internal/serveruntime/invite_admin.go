package serveruntime

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster"
)

// inviteProposer is the minimal meta-raft surface the invite-create handler
// needs: leader detection + the InviteMint proposal. Satisfied by
// *cluster.MetaRaft in production; a stub in tests.
type inviteProposer interface {
	IsLeader() bool
	LeaderID() string
	ProposeInviteMint(ctx context.Context, id string, pub ed25519.PublicKey, expiryNanos int64) error
}

// clusterIDSource yields the 16-byte cluster identity bound into the invite
// bundle. Satisfied by *cluster.MetaFSM (ClusterID()).
type clusterIDSource interface {
	ClusterID() [16]byte
}

// joinListenerInfo exposes the running Zero-CA join listener's dial address
// and persisted-stable SPKI (W9a). Satisfied by *bootState.
type joinListenerInfo interface {
	JoinListenerAddr() string
	JoinListenerSPKI() [32]byte
}

// InviteCreateRequest is the body for POST /v1/cluster/invite/create. TTLNanos
// is the invite lifetime; 0 lets the handler apply its default.
type InviteCreateRequest struct {
	TTLNanos int64 `json:"ttl_nanos"`
}

// InviteCreateResponse carries the operator-facing bundle token (base64) plus
// the invite id for logging/correlation. The operator copies Bundle into
// GRAINFS_INVITE_BUNDLE on the joiner.
type InviteCreateResponse struct {
	Bundle   string `json:"bundle"`
	InviteID string `json:"invite_id"`
}

// inviteCreateErrorResponse mirrors the not-leader / unavailable error body.
// LeaderID is populated on 409 so the operator knows where to re-run (the
// W7b FSM-resolve redirect that auto-forwards is a separate task).
type inviteCreateErrorResponse struct {
	Error    string `json:"error"`
	LeaderID string `json:"leader_id,omitempty"`
}

// defaultInviteTTL is the invite lifetime applied when the request omits one.
const defaultInviteTTL = time.Hour

// InviteHandler serves POST /v1/cluster/invite/create on the admin UDS. On the
// leader it mints a one-time Ed25519 invite keypair, records the PUBLIC key +
// TTL in meta-raft (ProposeInviteMint), and returns the InviteBundle token
// carrying the PRIVATE key + cluster.id + the running join-listener addr/SPKI
// (W9a). The joiner pins SeedSPKI and dials SeedAddr over the Zero-CA join ALPN.
type InviteHandler struct {
	proposer     inviteProposer
	clusterID    clusterIDSource
	joinListener joinListenerInfo
}

func (h *InviteHandler) Handle(ctx context.Context, c *app.RequestContext) {
	var req InviteCreateRequest
	// Empty body is valid (default TTL); only reject malformed JSON when a body
	// is present.
	if len(c.Request.Body()) > 0 {
		if err := c.BindJSON(&req); err != nil {
			c.JSON(consts.StatusBadRequest, inviteCreateErrorResponse{Error: "invalid JSON body"})
			return
		}
	}

	// Leader-only. TODO(W7b): FSM-resolve redirect to auto-forward to the
	// leader; for now return a clear 409 with the known leader id.
	if !h.proposer.IsLeader() {
		c.JSON(consts.StatusConflict, inviteCreateErrorResponse{
			Error:    "not leader: run `grainfs cluster invite create` on the meta-raft leader",
			LeaderID: h.proposer.LeaderID(),
		})
		return
	}

	// SeedAddr/SeedSPKI MUST come from the actual running join listener (W9a).
	// On a single-node leader the listener is not started — refuse rather than
	// emit a bundle pinning a zero SPKI.
	seedAddr := h.joinListener.JoinListenerAddr()
	if seedAddr == "" {
		c.JSON(consts.StatusServiceUnavailable, inviteCreateErrorResponse{
			Error: "join listener not running (single-node): invite-join requires a clustered leader",
		})
		return
	}
	seedSPKI := h.joinListener.JoinListenerSPKI()

	priv, pub, id, err := cluster.MintInviteKeypair()
	if err != nil {
		c.JSON(consts.StatusInternalServerError, inviteCreateErrorResponse{Error: err.Error()})
		return
	}

	ttl := time.Duration(req.TTLNanos)
	if ttl <= 0 {
		ttl = defaultInviteTTL
	}
	expiry := time.Now().Add(ttl).UnixNano()
	if err := h.proposer.ProposeInviteMint(ctx, id, pub, expiry); err != nil {
		c.JSON(consts.StatusInternalServerError, inviteCreateErrorResponse{Error: "propose invite mint: " + err.Error()})
		return
	}

	cid := h.clusterID.ClusterID()
	bundle := cluster.InviteBundle{
		InvitePriv:   priv,
		InviteID:     id,
		ClusterIDHex: hex.EncodeToString(cid[:]),
		SeedSPKI:     seedSPKI,
		SeedAddr:     seedAddr,
	}
	log.Info().Str("invite_id", id).Str("seed_addr", seedAddr).Dur("ttl", ttl).
		Msg("zero-CA invite minted")
	c.JSON(consts.StatusOK, InviteCreateResponse{
		Bundle:   cluster.EncodeInviteBundle(bundle),
		InviteID: id,
	})
}
