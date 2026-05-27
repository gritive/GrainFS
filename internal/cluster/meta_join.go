package cluster

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/x509"
	"errors"
	"fmt"
	"sync"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/pool"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/transport"
)

type JoinStatus string

// metaJoinTimeout is the maximum time a join operation waits for a Raft
// commit (both the KEK path and the invite path use the same budget).
const metaJoinTimeout = 60 * time.Second

const (
	JoinStatusOK            JoinStatus = "ok"
	JoinStatusAlreadyMember JoinStatus = "already_member"
	JoinStatusNotLeader     JoinStatus = "not_leader"
	JoinStatusAddrMismatch  JoinStatus = "addr_mismatch"
	JoinStatusClusterFull   JoinStatus = "cluster_full"
	JoinStatusMixedVersion  JoinStatus = "mixed_version"
	JoinStatusTimeout       JoinStatus = "timeout"
	JoinStatusError         JoinStatus = "error"
	// JoinStatusKEKMismatch is returned when the joiner's HMAC-SHA256 response
	// does not match the leader's KEK (or the nonce is unknown / replayed /
	// expired). The joiner is refused admission before AddVoter is called.
	// §7 T55 (D#15, F#23).
	JoinStatusKEKMismatch JoinStatus = "kek_mismatch"
)

type JoinRequest struct {
	NodeID  string `json:"node_id"`
	Address string `json:"address"`
	// HandshakeNonce/Response carry the KEK challenge-response proof. The
	// joiner first calls Challenge to obtain a fresh Nonce, computes
	// HMAC-SHA256(KEK, Nonce) as Response, and sends both alongside Join.
	// §7 T55 (D#15, F#23).
	HandshakeNonce    []byte `json:"handshake_nonce,omitempty"`
	HandshakeResponse []byte `json:"handshake_response,omitempty"`
	// Invite path (brand-new node, asymmetric — Path A: cert in request).
	SPKI      []byte `json:"spki,omitempty"`
	CertDER   []byte `json:"cert_der,omitempty"`   // joiner per-node leaf cert (Path A)
	NodeSig   []byte `json:"node_sig,omitempty"`   // ECDSA over transcript
	InviteSig []byte `json:"invite_sig,omitempty"` // Ed25519 over the SAME transcript
	InviteID  string `json:"invite_id,omitempty"`
	// JoinPhase selects the two-phase invite-join flow: 0 = legacy/KEK (no
	// invite), 1 = Phase-1 (invite gate + seal bootstrap, no membership), 2 =
	// Phase-2 ACK (membership stage + invite consume). W2 wire field.
	JoinPhase uint8 `json:"join_phase,omitempty"`
	// JoinerJoinListenerAddr/SPKI advertise the joiner's own join-listener so a
	// non-leader can later redirect the joiner to the leader's listener. Carried
	// on the wire (W2); the redirect itself is a later task.
	JoinerJoinListenerAddr string `json:"joiner_join_listener_addr,omitempty"`
	JoinerJoinListenerSPKI []byte `json:"joiner_join_listener_spki,omitempty"`
}

type JoinReply struct {
	Accepted   bool       `json:"accepted"`
	Status     JoinStatus `json:"status"`
	Message    string     `json:"message,omitempty"`
	LeaderID   string     `json:"leader_id,omitempty"`
	LeaderAddr string     `json:"leader_addr,omitempty"`
	// PeerSPKIs is the cluster accept-set delivered to a freshly admitted
	// pending-learner at commit1. NO KEK is delivered here — KEK is a
	// promotion-time follow-up (Phase 3).
	PeerSPKIs [][]byte `json:"peer_spkis,omitempty"`
	// SealedBootstrap is the Phase-1 envelope (wire form of encrypt.SealedToPeer)
	// the leader seals to the joiner's identity key: the bootstrap secrets
	// (encryption.key, KEK generations, transport PSK). Empty outside Phase-1.
	SealedBootstrap []byte `json:"sealed_bootstrap,omitempty"`
}

// BootstrapSecretProvider supplies the SECRET plaintext the invite handler
// seals to a joiner. Defined at the consuming use-site per repo convention;
// implemented by serveruntime over bootState. cluster.id is intentionally NOT
// included here — it is public and carried in the InviteBundle. KEKGen is the
// type already defined in bootstrap_codec.go (W2).
type BootstrapSecretProvider interface {
	// BootstrapSecrets returns the SECRET plaintext to seal: the static
	// encryption.key bytes, EVERY KEK generation in the KEKStore, and the
	// transport PSK. cluster.id is NOT included (public, in the InviteBundle).
	BootstrapSecrets() (encryptionKey []byte, kekGens []KEKGen, transportPSK []byte, err error)
}

type metaJoinDialer func(peer string, payload []byte) ([]byte, error)

type MetaJoinSender struct {
	dialer metaJoinDialer
}

func NewMetaJoinSender(d metaJoinDialer) *MetaJoinSender {
	return &MetaJoinSender{dialer: d}
}

func (s *MetaJoinSender) SendJoin(ctx context.Context, peers []string, req JoinRequest) (*JoinReply, error) {
	payload, err := encodeJoinRequest(req)
	if err != nil {
		return nil, err
	}
	var lastErr error
	for _, peer := range peers {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		replyBytes, err := s.dialer(peer, payload)
		if err != nil {
			lastErr = err
			continue
		}
		reply, err := decodeJoinReply(replyBytes)
		if err != nil {
			return nil, err
		}
		if reply.Status == JoinStatusNotLeader && reply.LeaderAddr != "" {
			replyBytes, err = s.dialer(reply.LeaderAddr, payload)
			if err != nil {
				lastErr = err
				continue
			}
			return decodeJoinReply(replyBytes)
		}
		return reply, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("meta_join: no peers")
}

type metaJoinCoordinator interface {
	IsLeader() bool
	LeaderID() string
	Join(ctx context.Context, id, addr string) error
	Nodes() []MetaNodeEntry
	// Invite admission gate (zero-CA Phase 2, Path A). Behavior methods rather
	// than concrete-type accessors so external packages (serveruntime fakes)
	// can implement the interface without naming unexported registry types.
	IsSPKIDenylisted(spki [32]byte) bool
	SPKIOwner(spki [32]byte) (string, bool)
	LookupInvite(id string, now time.Time) (ed25519.PublicKey, bool)
	AcceptSPKIBytes() [][]byte
	JoinViaInvite(ctx context.Context, nodeID, addr string, spki [32]byte, inviteID string) error
	// Two-phase invite-join (W7). Phase-1 binds the invite to the first
	// (nodeID, spki) redeemer; Phase-2 validates the ACK against that binding,
	// stages membership, and consumes the invite.
	ProposeInvitePending(ctx context.Context, inviteID, nodeID string, spki [32]byte, addr string) error
	LookupPending(inviteID string) (nodeID string, spki [32]byte, addr string, ok bool)
	ProposeInviteConsume(ctx context.Context, inviteID string) error
	RemoveLearner(nodeID, addr string) error
}

type MetaJoinReceiver struct {
	meta         metaJoinCoordinator
	joinMu       sync.Mutex
	postJoinHook func(context.Context, JoinRequest) error
	// verifier, when non-nil, gates admission on the KEK challenge-response
	// HMAC. T55 ships the plumbing; T57 will require verifier != nil at boot.
	// When nil (legacy callers, unit tests not exercising the handshake), the
	// HMAC gate is skipped so existing behavior is preserved.
	verifier *encrypt.HandshakeVerifier
	// clusterID is the 16-byte cluster identifier bound into the invite
	// transcript on the receiver side (sourced from the KEK verifier at boot).
	// The invite gate requires it to be non-empty.
	clusterID []byte
	// secretProvider supplies the bootstrap secrets (encryption.key, all KEK
	// generations, transport PSK) the invite handler seals to a joiner. Wired
	// here in W3; consumed by the seal/handler logic in a later task (W7).
	secretProvider BootstrapSecretProvider
}

func NewMetaJoinReceiver(meta metaJoinCoordinator) *MetaJoinReceiver {
	return &MetaJoinReceiver{meta: meta}
}

func (r *MetaJoinReceiver) WithPostJoinHook(fn func(context.Context, JoinRequest) error) *MetaJoinReceiver {
	r.postJoinHook = fn
	return r
}

// WithHandshakeVerifier installs the KEK handshake verifier used to gate
// admission. The SAME verifier instance must be wired into the paired
// MetaChallengeReceiver so the issued-nonce map is shared. §7 T55 (D#15, F#23).
func (r *MetaJoinReceiver) WithHandshakeVerifier(v *encrypt.HandshakeVerifier) *MetaJoinReceiver {
	r.verifier = v
	return r
}

// WithBootstrapSecretProvider installs the provider that assembles the secret
// plaintext (encryption.key, KEK generations, transport PSK) the invite handler
// seals to a joiner. Wired at boot; the seal logic that calls
// BootstrapSecrets() lands in a later task.
func (r *MetaJoinReceiver) WithBootstrapSecretProvider(p BootstrapSecretProvider) *MetaJoinReceiver {
	r.secretProvider = p
	return r
}

// WithClusterID sets the 16-byte cluster identifier bound into the invite
// transcript. It must match the value the joiner used when signing. Sourced
// from the KEK verifier's ClusterID() at boot.
func (r *MetaJoinReceiver) WithClusterID(cid []byte) *MetaJoinReceiver {
	r.clusterID = append([]byte(nil), cid...)
	return r
}

func (r *MetaJoinReceiver) Handle(req *transport.Message) *transport.Message {
	joinReq, err := decodeJoinRequest(req.Payload)
	if err != nil {
		return joinMessage(JoinReply{Accepted: false, Status: JoinStatusError, Message: err.Error()})
	}
	if joinReq.NodeID == "" || joinReq.Address == "" {
		return joinMessage(JoinReply{Accepted: false, Status: JoinStatusError, Message: "node_id and address are required"})
	}
	if !r.meta.IsLeader() {
		leaderID := r.meta.LeaderID()
		leaderAddr := ""
		for _, n := range r.meta.Nodes() {
			if n.ID == leaderID || n.Address == leaderID {
				leaderAddr = n.Address
				break
			}
		}
		if leaderAddr == "" {
			leaderAddr = leaderID
		}
		// TODO(W7b/W9): return leader_join_addr+leader_join_spki from member state
		// so an invite joiner can redirect its join-listener dial to the leader.
		// The full join-listener redirect (MetaNodeEntry extension) is a separate
		// task and is intentionally NOT implemented here.
		return joinMessage(JoinReply{
			Accepted:   false,
			Status:     JoinStatusNotLeader,
			LeaderID:   leaderID,
			LeaderAddr: leaderAddr,
		})
	}
	// Invite admission path (zero-CA Phase 2, §4.2). A brand-new node with no
	// pre-shared KEK presents an invite signature + its per-node ECDSA cert.
	// Path A: the leaf cert DER travels IN the request (the join handler does
	// not expose the TLS session). The two-phase flow (W7) is delegated to
	// HandleJoin. This in-process Handle entry has no TLS session, so it passes
	// the CLAIMED SPKI as the captured SPKI; W9's JoinListener calls HandleJoin
	// directly with the real TLS-captured SPKI.
	if len(joinReq.InviteSig) > 0 {
		var captured [32]byte
		copy(captured[:], joinReq.SPKI)
		ctx, cancel := context.WithTimeout(context.Background(), metaJoinTimeout)
		defer cancel()
		return joinMessage(r.HandleJoin(ctx, captured, joinReq))
	}
	// KEK handshake gate. Runs after the leader check so non-leaders return
	// JoinStatusNotLeader without consuming a nonce on the leader's verifier
	// (saves nonce churn on follower retries). §7 T55 (D#15, F#23).
	//
	// Phase A: transcript joiner_version and leader_active_version are
	// pinned to 0 on both sides — the wire JoinRequest carries only
	// (NodeID, Address, Nonce, Response); the version fields will be
	// extended in Phase C. The verifier already holds cluster_id from
	// boot; we reach it via ClusterID() so receivers stay decoupled from
	// NodeConfig.
	if r.verifier != nil {
		transcript := encrypt.JoinTranscript{
			ClusterID:           r.verifier.ClusterID(),
			Nonce:               joinReq.HandshakeNonce,
			NodeID:              joinReq.NodeID,
			Address:             joinReq.Address,
			JoinerVersion:       0,
			LeaderActiveVersion: 0,
		}
		activeVer := r.verifier.Store().ActiveVersion()
		if err := r.verifier.VerifyResponse(activeVer, transcript, joinReq.HandshakeResponse); err != nil {
			return joinMessage(JoinReply{
				Accepted: false,
				Status:   JoinStatusKEKMismatch,
				Message:  "KEK handshake failed: " + err.Error(),
			})
		}
	}
	r.joinMu.Lock()
	defer r.joinMu.Unlock()
	for _, n := range r.meta.Nodes() {
		if n.ID == joinReq.NodeID {
			if n.Address == joinReq.Address {
				return joinMessage(JoinReply{Accepted: true, Status: JoinStatusAlreadyMember})
			}
			return joinMessage(JoinReply{Accepted: false, Status: JoinStatusAddrMismatch, Message: "node ID already exists with different address"})
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), metaJoinTimeout)
	defer cancel()
	if err := r.meta.Join(ctx, joinReq.NodeID, joinReq.Address); err != nil {
		return joinMessage(joinReplyFromError(err))
	}
	if r.postJoinHook != nil {
		if err := r.postJoinHook(ctx, joinReq); err != nil {
			return joinMessage(joinReplyFromError(err))
		}
	}
	return joinMessage(JoinReply{Accepted: true, Status: JoinStatusOK})
}

// HandleJoin runs the two-phase invite-join flow (W7). It receives the
// JoinListener-captured TLS peer SPKI (W9 supplies the real value; the
// in-process Handle entry passes the claimed SPKI). The caller must already
// hold leadership; HandleJoin assumes IsLeader() is true.
//
//   - Phase-1 (req.JoinPhase==1): run the invite gate, assert capturedSPKI ==
//     req.SPKI, bind the invite to this (nodeID, spki) via ProposeInvitePending,
//     and seal the bootstrap secrets to the joiner's identity key. NO membership
//     change.
//   - Phase-2 (req.JoinPhase==2): match the ACK against the pending binding AND
//     capturedSPKI, stage membership (idempotently), run the post-join hook, and
//     consume the invite. On membership failure: RemoveLearner rollback.
func (r *MetaJoinReceiver) HandleJoin(ctx context.Context, capturedSPKI [32]byte, req JoinRequest) JoinReply {
	if len(r.clusterID) == 0 {
		return JoinReply{Accepted: false, Status: JoinStatusError, Message: "invite path unavailable: cluster id not configured"}
	}
	var spki [32]byte
	copy(spki[:], req.SPKI)

	switch req.JoinPhase {
	case 2:
		return r.handleJoinPhase2(ctx, capturedSPKI, spki, req)
	default:
		// Phase-1 (and any legacy invite request that omits join_phase): gate +
		// seal, no membership.
		return r.handleJoinPhase1(ctx, capturedSPKI, spki, req)
	}
}

// gateInvite runs the invite admission gate shared by Phase-1 (and re-checked
// before membership at Phase-2). It returns the joiner's ECDSA identity key on
// success, or a populated rejection reply (ok=false).
func (r *MetaJoinReceiver) gateInvite(spki [32]byte, req JoinRequest) (*ecdsa.PublicKey, JoinReply, bool) {
	// 1. denylist + SPKI uniqueness.
	if r.meta.IsSPKIDenylisted(spki) {
		return nil, JoinReply{Accepted: false, Status: JoinStatusError, Message: "SPKI denylisted"}, false
	}
	if owner, ok := r.meta.SPKIOwner(spki); ok && owner != req.NodeID {
		return nil, JoinReply{Accepted: false, Status: JoinStatusError, Message: "SPKI already registered"}, false
	}
	// 2. invite public key lookup (present, unused, unexpired).
	invitePub, ok := r.meta.LookupInvite(req.InviteID, time.Now())
	if !ok {
		return nil, JoinReply{Accepted: false, Status: JoinStatusError, Message: "invite invalid/used/expired"}, false
	}
	// 3. parse joiner cert (Path A) + bind SPKI to it.
	leaf, err := x509.ParseCertificate(req.CertDER)
	if err != nil {
		return nil, JoinReply{Accepted: false, Status: JoinStatusError, Message: "bad cert"}, false
	}
	if sha256.Sum256(leaf.RawSubjectPublicKeyInfo) != spki {
		return nil, JoinReply{Accepted: false, Status: JoinStatusError, Message: "SPKI does not match presented cert"}, false
	}
	ecPub, ok := leaf.PublicKey.(*ecdsa.PublicKey)
	if !ok {
		return nil, JoinReply{Accepted: false, Status: JoinStatusError, Message: "non-ECDSA node key"}, false
	}
	// 4. rebuild canonical transcript. Bind is empty — Path A defers the
	// TLS-exporter channel binding; the nonce is joiner-generated, carried in
	// HandshakeNonce, and bound by both signatures.
	// TODO(phase-2-followup): Path A deferred TLS-exporter channel binding
	// (Bind) + server-issued nonce freshness; see design doc.
	tr := encrypt.InviteTranscript{
		ClusterID: r.clusterID,
		Nonce:     req.HandshakeNonce,
		NodeID:    req.NodeID,
		Address:   req.Address,
		SPKI:      req.SPKI,
		Bind:      nil,
	}
	// 5. verify BOTH signatures over the same transcript.
	if !encrypt.VerifyInviteTranscript(invitePub, tr, req.InviteSig) {
		return nil, JoinReply{Accepted: false, Status: JoinStatusError, Message: "invite signature invalid"}, false
	}
	if !encrypt.VerifyNodeTranscript(ecPub, tr, req.NodeSig) {
		return nil, JoinReply{Accepted: false, Status: JoinStatusError, Message: "node signature invalid"}, false
	}
	return ecPub, JoinReply{}, true
}

func (r *MetaJoinReceiver) handleJoinPhase1(ctx context.Context, capturedSPKI, spki [32]byte, req JoinRequest) JoinReply {
	ecPub, reject, ok := r.gateInvite(spki, req)
	if !ok {
		return reject
	}
	// The TLS-captured SPKI must match the claimed identity: a joiner cannot
	// present a cert it does not hold the key for.
	if capturedSPKI != spki {
		return JoinReply{Accepted: false, Status: JoinStatusError, Message: "captured SPKI does not match claimed SPKI"}
	}
	r.joinMu.Lock()
	defer r.joinMu.Unlock()
	if r.secretProvider == nil {
		return JoinReply{Accepted: false, Status: JoinStatusError, Message: "bootstrap secret provider not configured"}
	}
	// Bind the invite to the first (nodeID, spki) redeemer. A different identity
	// re-redeeming the same invite is rejected here (errInvitePendingMismatch).
	if err := r.meta.ProposeInvitePending(ctx, req.InviteID, req.NodeID, spki, req.Address); err != nil {
		return joinReplyFromError(err)
	}
	// Seal the bootstrap secrets to the joiner's identity key.
	encKey, kekGens, psk, err := r.secretProvider.BootstrapSecrets()
	if err != nil {
		return JoinReply{Accepted: false, Status: JoinStatusError, Message: "bootstrap secrets: " + err.Error()}
	}
	payload := encodeBootstrapSecretsPayload(encKey, kekGens, psk)
	bindCtx := r.sealBindContext(req)
	blob, err := encrypt.SealToPeer(ecPub, payload, bindCtx, bindCtx)
	if err != nil {
		return JoinReply{Accepted: false, Status: JoinStatusError, Message: "seal bootstrap: " + err.Error()}
	}
	return JoinReply{
		Accepted: true,
		Status:   JoinStatusOK,
		// LeaderID lets the joiner reproduce sealBindContext's leaderNodeID
		// component when opening the sealed bootstrap (W9 cross-process).
		LeaderID:        r.meta.LeaderID(),
		SealedBootstrap: encodeSealedBootstrap(blob.EphemeralPub, blob.Ciphertext),
	}
}

func (r *MetaJoinReceiver) handleJoinPhase2(ctx context.Context, capturedSPKI, spki [32]byte, req JoinRequest) JoinReply {
	r.joinMu.Lock()
	defer r.joinMu.Unlock()
	// Phase-2 security rests on the Phase-1 pending binding (persisted in the
	// FSM) plus the TLS-captured SPKI — NOT on re-verifying the signatures. The
	// pending record proves the invite was gate-verified at Phase-1; capturedSPKI
	// proves this ACK comes from the holder of that Phase-1 key (payload fields
	// alone are forgeable).
	pendNode, pendSPKI, _, ok := r.meta.LookupPending(req.InviteID)
	if !ok {
		return JoinReply{Accepted: false, Status: JoinStatusError, Message: "no pending invite redemption"}
	}
	if pendNode != req.NodeID || pendSPKI != spki {
		return JoinReply{Accepted: false, Status: JoinStatusError, Message: "ACK does not match pending redemption"}
	}
	if capturedSPKI != pendSPKI {
		return JoinReply{Accepted: false, Status: JoinStatusError, Message: "captured SPKI does not match pending redemption"}
	}
	// Stage membership idempotently (JoinViaInvite resumes from the first missing
	// step). On failure roll back the un-promoted learner.
	if err := r.meta.JoinViaInvite(ctx, req.NodeID, req.Address, spki, req.InviteID); err != nil {
		if rbErr := r.meta.RemoveLearner(req.NodeID, req.Address); rbErr != nil {
			log.Warn().Err(rbErr).Str("node_id", req.NodeID).
				Msg("meta_join: JoinViaInvite rollback RemoveLearner failed")
		}
		return joinReplyFromError(err)
	}
	if r.postJoinHook != nil {
		if err := r.postJoinHook(ctx, req); err != nil {
			return joinReplyFromError(err)
		}
	}
	// Consume the invite (pending → used). Single consume owned by Phase-2. On a
	// retry the invite is already used — tolerate that so the ACK still succeeds
	// idempotently (the pending+captured checks above already authorized it).
	if err := r.meta.ProposeInviteConsume(ctx, req.InviteID); err != nil && !errors.Is(err, errInviteInvalid) {
		return joinReplyFromError(err)
	}
	return JoinReply{Accepted: true, Status: JoinStatusOK, PeerSPKIs: r.meta.AcceptSPKIBytes()}
}

// sealBindContext derives the contextInfo/aad bytes binding a sealed bootstrap
// blob to its transcript identity: clusterID ‖ inviteID ‖ joinerNodeID ‖
// leaderNodeID. The joiner reproduces the same bytes when opening. Both
// contextInfo and aad use this value (consistent with SealToPeer's design).
func (r *MetaJoinReceiver) sealBindContext(req JoinRequest) []byte {
	leaderID := r.meta.LeaderID()
	out := make([]byte, 0, len(r.clusterID)+len(req.InviteID)+len(req.NodeID)+len(leaderID))
	out = append(out, r.clusterID...)
	out = append(out, req.InviteID...)
	out = append(out, req.NodeID...)
	out = append(out, leaderID...)
	return out
}

func joinReplyFromError(err error) JoinReply {
	switch {
	case errors.Is(err, raft.ErrNotLeader):
		return JoinReply{Accepted: false, Status: JoinStatusNotLeader, Message: err.Error()}
	case errors.Is(err, raft.ErrMixedVersionNoMembershipChange):
		return JoinReply{Accepted: false, Status: JoinStatusMixedVersion, Message: err.Error()}
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return JoinReply{Accepted: false, Status: JoinStatusTimeout, Message: err.Error()}
	default:
		return JoinReply{Accepted: false, Status: JoinStatusError, Message: err.Error()}
	}
}

func joinMessage(reply JoinReply) *transport.Message {
	data, _ := encodeJoinReply(reply)
	return &transport.Message{Type: transport.StreamMetaJoin, Payload: data}
}

var metaJoinRequestMagic = []byte("GFSMJN2")

// 128-byte initial: request is two short strings; reply is one bool + one enum
// + three short strings. Typical payload fits in one slab.
var metaJoinBuilderPool = pool.New(func() *flatbuffers.Builder {
	return flatbuffers.NewBuilder(128)
})

func newMetaJoinBuilder() *flatbuffers.Builder {
	b := metaJoinBuilderPool.Get()
	b.Reset()
	return b
}

func releaseMetaJoinBuilder(b *flatbuffers.Builder) {
	metaJoinBuilderPool.Put(b)
}

func joinStatusToFB(s JoinStatus) clusterpb.JoinStatus {
	switch s {
	case JoinStatusOK:
		return clusterpb.JoinStatusOK
	case JoinStatusAlreadyMember:
		return clusterpb.JoinStatusAlreadyMember
	case JoinStatusNotLeader:
		return clusterpb.JoinStatusNotLeader
	case JoinStatusAddrMismatch:
		return clusterpb.JoinStatusAddrMismatch
	case JoinStatusClusterFull:
		return clusterpb.JoinStatusClusterFull
	case JoinStatusMixedVersion:
		return clusterpb.JoinStatusMixedVersion
	case JoinStatusTimeout:
		return clusterpb.JoinStatusTimeout
	case JoinStatusError:
		return clusterpb.JoinStatusError
	case JoinStatusKEKMismatch:
		return clusterpb.JoinStatusKEKMismatch
	default:
		return clusterpb.JoinStatusUnknown
	}
}

func joinStatusFromFB(s clusterpb.JoinStatus) JoinStatus {
	switch s {
	case clusterpb.JoinStatusOK:
		return JoinStatusOK
	case clusterpb.JoinStatusAlreadyMember:
		return JoinStatusAlreadyMember
	case clusterpb.JoinStatusNotLeader:
		return JoinStatusNotLeader
	case clusterpb.JoinStatusAddrMismatch:
		return JoinStatusAddrMismatch
	case clusterpb.JoinStatusClusterFull:
		return JoinStatusClusterFull
	case clusterpb.JoinStatusMixedVersion:
		return JoinStatusMixedVersion
	case clusterpb.JoinStatusTimeout:
		return JoinStatusTimeout
	case clusterpb.JoinStatusError:
		return JoinStatusError
	case clusterpb.JoinStatusKEKMismatch:
		return JoinStatusKEKMismatch
	default:
		return JoinStatus("")
	}
}

func encodeJoinRequest(req JoinRequest) ([]byte, error) {
	b := newMetaJoinBuilder()
	defer releaseMetaJoinBuilder(b)
	nodeOff := b.CreateString(req.NodeID)
	addrOff := b.CreateString(req.Address)
	nonceOff := b.CreateByteVector(req.HandshakeNonce)
	respOff := b.CreateByteVector(req.HandshakeResponse)
	spkiOff := b.CreateByteVector(req.SPKI)
	certOff := b.CreateByteVector(req.CertDER)
	nodeSigOff := b.CreateByteVector(req.NodeSig)
	inviteSigOff := b.CreateByteVector(req.InviteSig)
	inviteIDOff := b.CreateString(req.InviteID)
	joinListenerAddrOff := b.CreateString(req.JoinerJoinListenerAddr)
	joinListenerSPKIOff := b.CreateByteVector(req.JoinerJoinListenerSPKI)
	clusterpb.JoinRequestStart(b)
	clusterpb.JoinRequestAddNodeId(b, nodeOff)
	clusterpb.JoinRequestAddAddress(b, addrOff)
	clusterpb.JoinRequestAddHandshakeNonce(b, nonceOff)
	clusterpb.JoinRequestAddHandshakeResponse(b, respOff)
	clusterpb.JoinRequestAddSpki(b, spkiOff)
	clusterpb.JoinRequestAddCertDer(b, certOff)
	clusterpb.JoinRequestAddNodeSig(b, nodeSigOff)
	clusterpb.JoinRequestAddInviteSig(b, inviteSigOff)
	clusterpb.JoinRequestAddInviteId(b, inviteIDOff)
	clusterpb.JoinRequestAddJoinPhase(b, req.JoinPhase)
	clusterpb.JoinRequestAddJoinerJoinListenerAddr(b, joinListenerAddrOff)
	clusterpb.JoinRequestAddJoinerJoinListenerSpki(b, joinListenerSPKIOff)
	b.Finish(clusterpb.JoinRequestEnd(b))
	fb := b.FinishedBytes()
	out := make([]byte, 0, len(metaJoinRequestMagic)+len(fb))
	out = append(out, metaJoinRequestMagic...)
	out = append(out, fb...)
	return out, nil
}

func decodeJoinRequest(data []byte) (req JoinRequest, err error) {
	if !bytes.HasPrefix(data, metaJoinRequestMagic) {
		return JoinRequest{}, fmt.Errorf("meta_join: bad magic")
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("meta_join: malformed JoinRequest: %v", r)
		}
	}()
	fb := clusterpb.GetRootAsJoinRequest(data[len(metaJoinRequestMagic):], 0)
	req.NodeID = string(fb.NodeId())
	req.Address = string(fb.Address())
	if n := fb.HandshakeNonceBytes(); len(n) > 0 {
		req.HandshakeNonce = append([]byte(nil), n...)
	}
	if r := fb.HandshakeResponseBytes(); len(r) > 0 {
		req.HandshakeResponse = append([]byte(nil), r...)
	}
	if s := fb.SpkiBytes(); len(s) > 0 {
		req.SPKI = append([]byte(nil), s...)
	}
	if c := fb.CertDerBytes(); len(c) > 0 {
		req.CertDER = append([]byte(nil), c...)
	}
	if s := fb.NodeSigBytes(); len(s) > 0 {
		req.NodeSig = append([]byte(nil), s...)
	}
	if s := fb.InviteSigBytes(); len(s) > 0 {
		req.InviteSig = append([]byte(nil), s...)
	}
	req.InviteID = string(fb.InviteId())
	req.JoinPhase = fb.JoinPhase()
	req.JoinerJoinListenerAddr = string(fb.JoinerJoinListenerAddr())
	if s := fb.JoinerJoinListenerSpkiBytes(); len(s) > 0 {
		req.JoinerJoinListenerSPKI = append([]byte(nil), s...)
	}
	return req, nil
}

func encodeJoinReply(reply JoinReply) ([]byte, error) {
	b := newMetaJoinBuilder()
	defer releaseMetaJoinBuilder(b)
	msgOff := b.CreateString(reply.Message)
	leaderIDOff := b.CreateString(reply.LeaderID)
	leaderAddrOff := b.CreateString(reply.LeaderAddr)
	var sealedOff flatbuffers.UOffsetT
	if len(reply.SealedBootstrap) > 0 {
		sealedOff = b.CreateByteVector(reply.SealedBootstrap)
	}
	var peerSPKIsOff flatbuffers.UOffsetT
	if len(reply.PeerSPKIs) > 0 {
		offs := make([]flatbuffers.UOffsetT, len(reply.PeerSPKIs))
		for i, s := range reply.PeerSPKIs {
			vOff := b.CreateByteVector(s)
			clusterpb.SPKIBytesStart(b)
			clusterpb.SPKIBytesAddValue(b, vOff)
			offs[i] = clusterpb.SPKIBytesEnd(b)
		}
		clusterpb.JoinReplyStartPeerSpkisVector(b, len(offs))
		for i := len(offs) - 1; i >= 0; i-- {
			b.PrependUOffsetT(offs[i])
		}
		peerSPKIsOff = b.EndVector(len(offs))
	}
	clusterpb.JoinReplyStart(b)
	clusterpb.JoinReplyAddAccepted(b, reply.Accepted)
	clusterpb.JoinReplyAddStatus(b, joinStatusToFB(reply.Status))
	clusterpb.JoinReplyAddMessage(b, msgOff)
	clusterpb.JoinReplyAddLeaderId(b, leaderIDOff)
	clusterpb.JoinReplyAddLeaderAddr(b, leaderAddrOff)
	if len(reply.PeerSPKIs) > 0 {
		clusterpb.JoinReplyAddPeerSpkis(b, peerSPKIsOff)
	}
	if sealedOff != 0 {
		clusterpb.JoinReplyAddSealedBootstrap(b, sealedOff)
	}
	b.Finish(clusterpb.JoinReplyEnd(b))
	return append([]byte(nil), b.FinishedBytes()...), nil
}

func decodeJoinReply(data []byte) (reply *JoinReply, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("meta_join: malformed JoinReply: %v", r)
		}
	}()
	fb := clusterpb.GetRootAsJoinReply(data, 0)
	out := &JoinReply{
		Accepted:   fb.Accepted(),
		Status:     joinStatusFromFB(fb.Status()),
		Message:    string(fb.Message()),
		LeaderID:   string(fb.LeaderId()),
		LeaderAddr: string(fb.LeaderAddr()),
	}
	if n := fb.PeerSpkisLength(); n > 0 {
		out.PeerSPKIs = make([][]byte, 0, n)
		var sp clusterpb.SPKIBytes
		for i := 0; i < n; i++ {
			if !fb.PeerSpkis(&sp, i) {
				continue
			}
			out.PeerSPKIs = append(out.PeerSPKIs, append([]byte(nil), sp.ValueBytes()...))
		}
	}
	if s := fb.SealedBootstrapBytes(); len(s) > 0 {
		out.SealedBootstrap = append([]byte(nil), s...)
	}
	return out, nil
}
