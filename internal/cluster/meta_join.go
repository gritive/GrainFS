package cluster

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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
	// the leader seals to the joiner's identity key: KEK generations and
	// transport bootstrap data. Empty outside Phase-1.
	SealedBootstrap []byte `json:"sealed_bootstrap,omitempty"`
}

// BootstrapSecretProvider supplies the SECRET plaintext the invite handler
// seals to a joiner. Defined at the consuming use-site per repo convention;
// implemented by serveruntime over bootState. cluster.id is intentionally NOT
// included here — it is public and carried in the InviteBundle. KEKGen is the
// type already defined in bootstrap_codec.go (W2).
type BootstrapSecretProvider interface {
	// BootstrapSecrets returns the SECRET plaintext to seal: EVERY KEK
	// generation in the KEKStore and the transport PSK. cluster.id is NOT
	// included (public, in the InviteBundle). The static encryption.key is not
	// sent in new payloads; legacy decode compatibility stays in the codec.
	BootstrapSecrets() (kekGens []KEKGen, transportPSK []byte, err error)
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
	LookupPending(inviteID string, now time.Time) (nodeID string, spki [32]byte, addr string, ok bool)
	ProposeInviteConsumeAt(ctx context.Context, inviteID string, consumedAt time.Time) error
	// PeerSPKIs and ClusterKeyDropped supply the FSM state needed to populate
	// peer_spkis in the sealed bootstrap for invite-join joiners (PR-2a §8f M1).
	PeerSPKIs() [][32]byte
	ClusterKeyDropped() bool
}

type MetaJoinReceiver struct {
	meta         metaJoinCoordinator
	joinMu       sync.Mutex
	postJoinHook func(context.Context, JoinRequest) error
	// clusterID is the 16-byte cluster identifier bound into the invite
	// transcript on the receiver side (sourced from the KEK verifier's
	// ClusterID() at boot). The invite gate requires it to be non-empty.
	clusterID []byte
	// secretProvider supplies the bootstrap secrets (KEK generations and
	// transport PSK) the invite handler seals to a joiner.
	secretProvider BootstrapSecretProvider
}

func NewMetaJoinReceiver(meta metaJoinCoordinator) *MetaJoinReceiver {
	return &MetaJoinReceiver{meta: meta}
}

func (r *MetaJoinReceiver) WithPostJoinHook(fn func(context.Context, JoinRequest) error) *MetaJoinReceiver {
	r.postJoinHook = fn
	return r
}

// WithBootstrapSecretProvider installs the provider that assembles the secret
// plaintext (KEK generations and transport PSK) the invite handler seals to a
// joiner.
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

// HandleJoin runs the two-phase invite-join flow (W7). It receives the
// JoinListener-captured TLS peer SPKI. The caller must already hold leadership;
// HandleJoin assumes IsLeader() is true.
//
//   - Phase-1 (req.JoinPhase==1): run the invite gate, assert capturedSPKI ==
//     req.SPKI, bind the invite to this (nodeID, spki) via ProposeInvitePending,
//     and seal the bootstrap secrets to the joiner's identity key. NO membership
//     change.
//   - Phase-2 (req.JoinPhase==2): match the ACK against the pending binding AND
//     capturedSPKI, stage membership (idempotently), run the post-join hook, and
//     consume the invite. On membership failure, leave pending state retryable.
func (r *MetaJoinReceiver) HandleJoin(ctx context.Context, capturedSPKI [32]byte, bind []byte, req JoinRequest) JoinReply {
	if len(r.clusterID) == 0 {
		return JoinReply{Accepted: false, Status: JoinStatusError, Message: "invite path unavailable: cluster id not configured"}
	}
	// Required-field guard, mirroring the in-process Handle path. The dedicated
	// join listener dispatches decoded requests straight here, so without
	// this a malformed client could sign Phase-1 with an empty node_id/address;
	// Phase-2 would then register/promote a learner keyed by the address and only
	// ProposeAddNode rejects the empty id AFTER promotion.
	if req.NodeID == "" || req.Address == "" {
		return JoinReply{Accepted: false, Status: JoinStatusError, Message: "node_id and address are required"}
	}
	var spki [32]byte
	copy(spki[:], req.SPKI)

	switch req.JoinPhase {
	case 2:
		return r.handleJoinPhase2(ctx, capturedSPKI, spki, req)
	default:
		// Phase-1 (and any legacy invite request that omits join_phase): gate +
		// seal, no membership.
		return r.handleJoinPhase1(ctx, capturedSPKI, spki, bind, req)
	}
}

// HandleJoinStream is the JoinListener (W9) glue: it reads the framed JoinRequest
// off the transport stream, runs HandleJoin with the TLS-captured peer SPKI, and
// writes the framed JoinReply back. The wire is length-prefixed binary (NO
// JSON) using transport.JoinReadFields/JoinPutField: exactly ONE field in each
// direction, carrying the magic-prefixed FlatBuffers JoinRequest/JoinReply blob.
// stream is typically a *quic.Stream (io.ReadWriteCloser); the caller owns
// closing the underlying connection.
func (r *MetaJoinReceiver) HandleJoinStream(ctx context.Context, peerSPKI [32]byte, bind []byte, stream io.ReadWriteCloser) {
	defer func() { _ = stream.Close() }()
	fields, err := transport.JoinReadFields(stream, 1)
	if err != nil {
		log.Warn().Err(err).Msg("meta_join: read join request frame failed")
		return
	}
	req, err := decodeJoinRequest(fields[0])
	if err != nil {
		r.writeJoinReply(stream, JoinReply{Accepted: false, Status: JoinStatusError, Message: err.Error()})
		return
	}
	// Leadership gate: HandleJoin assumes IsLeader() is true. Every node runs a
	// join listener, so a follower that receives a dial must return the standard
	// not-leader reply (with leader hint) rather than running the invite path
	// and failing noisily at the raft layer. (The leader_join_addr/spki redirect
	// extension is W7b — intentionally out of scope here.)
	if !r.meta.IsLeader() {
		r.writeJoinReply(stream, r.notLeaderReply())
		return
	}
	r.writeJoinReply(stream, r.HandleJoin(ctx, peerSPKI, bind, req))
}

// notLeaderReply builds the JoinStatusNotLeader reply with the best-effort
// leader id + address hint, mirroring the in-process Handle path.
func (r *MetaJoinReceiver) notLeaderReply() JoinReply {
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
	return JoinReply{
		Accepted:   false,
		Status:     JoinStatusNotLeader,
		LeaderID:   leaderID,
		LeaderAddr: leaderAddr,
	}
}

func (r *MetaJoinReceiver) writeJoinReply(w io.Writer, reply JoinReply) {
	replyBytes, err := encodeJoinReply(reply)
	if err != nil {
		log.Warn().Err(err).Msg("meta_join: encode join reply failed")
		return
	}
	if _, err := w.Write(transport.JoinPutField(nil, replyBytes)); err != nil {
		log.Warn().Err(err).Msg("meta_join: write join reply frame failed")
	}
}

// gateInvite runs the invite admission gate shared by Phase-1 (and re-checked
// before membership at Phase-2). It returns the joiner's ECDSA identity key on
// success, or a populated rejection reply (ok=false).
func (r *MetaJoinReceiver) gateInvite(spki [32]byte, bind []byte, req JoinRequest) (*ecdsa.PublicKey, JoinReply, bool) {
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
	// 4. Mandatory TLS-exporter channel binding: the transcript both peers signed
	// is bound to this join-listener TLS session. A mismatched bind fails
	// signature verification below; an absent/short bind is rejected here so a
	// direct/fake caller of this internal API cannot supply a degenerate value.
	// Enforce the EXACT exporter length, not merely non-empty.
	if len(bind) != transport.JoinBindingLen {
		return nil, JoinReply{Accepted: false, Status: JoinStatusError, Message: "channel binding required"}, false
	}
	tr := encrypt.InviteTranscript{
		ClusterID: r.clusterID,
		Nonce:     req.HandshakeNonce,
		NodeID:    req.NodeID,
		Address:   req.Address,
		SPKI:      req.SPKI,
		Bind:      bind,
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

func (r *MetaJoinReceiver) handleJoinPhase1(ctx context.Context, capturedSPKI, spki [32]byte, bind []byte, req JoinRequest) JoinReply {
	ecPub, reject, ok := r.gateInvite(spki, bind, req)
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
	kekGens, psk, err := r.secretProvider.BootstrapSecrets()
	if err != nil {
		return JoinReply{Accepted: false, Status: JoinStatusError, Message: "bootstrap secrets: " + err.Error()}
	}
	clusterKeyDropped := r.meta.ClusterKeyDropped()
	if clusterKeyDropped {
		psk = nil
	}
	// Populate peer_spkis from the FSM registry so the joiner can pre-seed its
	// accept-set before Listen (PR-2a §8f M1). After PR-2b's drop, do not send
	// the revoked cluster transport PSK to a new joiner; it must present its
	// per-node cert from the first normal transport handshake.
	payload := EncodeBootstrapSecretsPayloadWithCutover(kekGens, psk, r.meta.PeerSPKIs(), clusterKeyDropped)
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
	phaseNow := time.Now()
	pendNode, pendSPKI, pendAddr, ok := r.meta.LookupPending(req.InviteID, phaseNow)
	if !ok {
		return JoinReply{Accepted: false, Status: JoinStatusError, Message: "invite invalid/used/expired"}
	}
	if pendNode != req.NodeID || pendSPKI != spki {
		return JoinReply{Accepted: false, Status: JoinStatusError, Message: "ACK does not match pending redemption"}
	}
	if capturedSPKI != pendSPKI {
		return JoinReply{Accepted: false, Status: JoinStatusError, Message: "captured SPKI does not match pending redemption"}
	}
	// Use the address PERSISTED at Phase-1 (which was bound into the signed invite
	// transcript), NOT req.Address — a replayed/altered ACK must not be able to
	// finalize membership for a different (unreachable or attacker-chosen) endpoint
	// than the one the invite actually authorized.
	// Stage membership idempotently. On failure, leave the pending redemption and
	// any learner state retryable instead of attempting best-effort rollback that
	// can itself fail and obscure the original membership error.
	if err := r.meta.JoinViaInvite(ctx, req.NodeID, pendAddr, spki, req.InviteID); err != nil {
		log.Warn().Err(err).Str("node_id", req.NodeID).
			Msg("meta_join: invite Phase-2 membership staging failed; leaving pending state for retry")
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
	if err := r.meta.ProposeInviteConsumeAt(ctx, req.InviteID, phaseNow); err != nil && !errors.Is(err, errInviteInvalid) {
		return joinReplyFromError(err)
	}
	return JoinReply{Accepted: true, Status: JoinStatusOK, PeerSPKIs: r.meta.AcceptSPKIBytes()}
}

// sealBindContext derives the contextInfo/aad bytes binding a sealed bootstrap
// blob to its transcript identity: clusterID ‖ inviteID ‖ joinerNodeID ‖
// leaderNodeID. The joiner reproduces the same bytes when opening. Both
// contextInfo and aad use this value (consistent with SealToPeer's design).
//
// Each variable-length field is length-prefixed (4-byte big-endian) behind a
// domain tag so two distinct transcripts can never collide into the same
// context bytes via boundary ambiguity (e.g. inviteID="ab"+nodeID="c" vs
// inviteID="a"+nodeID="bc"). The joiner's inviteSealBindContext mirrors this
// byte-for-byte; the e2e seal/open round-trip guards against drift.
func (r *MetaJoinReceiver) sealBindContext(req JoinRequest) []byte {
	leaderID := r.meta.LeaderID()
	out := make([]byte, 0, len(inviteSealDomain)+16+len(r.clusterID)+len(req.InviteID)+len(req.NodeID)+len(leaderID))
	out = append(out, inviteSealDomain...)
	out = appendInviteSealField(out, r.clusterID)
	out = appendInviteSealField(out, []byte(req.InviteID))
	out = appendInviteSealField(out, []byte(req.NodeID))
	out = appendInviteSealField(out, []byte(leaderID))
	return out
}

// inviteSealDomain is the domain-separation tag prefixing the invite-join seal
// bind context. invite_join_boot.go's inviteSealBindContext uses the IDENTICAL
// literal; they MUST stay in sync (the e2e round-trip enforces it).
const inviteSealDomain = "grainfs-invite-seal-v1"

// appendInviteSealField appends b length-prefixed (4-byte big-endian length).
func appendInviteSealField(out, b []byte) []byte {
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], uint32(len(b)))
	out = append(out, l[:]...)
	return append(out, b...)
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

// EncodeJoinRequest serializes a JoinRequest to the magic-prefixed FlatBuffers
// blob carried in one length-prefixed join-wire field. Used by the W9b joiner
// to drive Phase-1/Phase-2 over transport.DialJoinTCP.
func EncodeJoinRequest(req JoinRequest) ([]byte, error) {
	return encodeJoinRequest(req)
}

// DecodeJoinReply parses the FlatBuffers JoinReply blob the leader writes back.
// Used by the W9b joiner.
func DecodeJoinReply(data []byte) (*JoinReply, error) {
	return decodeJoinReply(data)
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
