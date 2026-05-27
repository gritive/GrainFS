package cluster

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/transport"
)

// transcriptClusterID is the 16-byte cluster identifier bound into the invite
// transcript. It only needs to match on both the joiner and receiver sides; it
// is independent of the string clusterID GenerateNodeIdentity puts in the SAN.
var transcriptClusterID = []byte("cluster-x-16byt!")

// fakeBootstrapProvider supplies deterministic bootstrap secrets so the Phase-1
// seal has a known plaintext to round-trip-decrypt in tests.
type fakeBootstrapProvider struct {
	encKey  []byte
	kekGens []KEKGen
	psk     []byte
}

func (p *fakeBootstrapProvider) BootstrapSecrets() ([]byte, []KEKGen, []byte, error) {
	return p.encKey, p.kekGens, p.psk, nil
}

func newFakeBootstrapProvider() *fakeBootstrapProvider {
	return &fakeBootstrapProvider{
		encKey:  []byte("enc-key-32-bytes-aaaaaaaaaaaaaaaa"),
		kekGens: []KEKGen{{Gen: 1, Key: []byte("kek-gen-1-key-bytes-padding-aaaaa")}},
		psk:     []byte("transport-psk-bytes"),
	}
}

// inviteJoinFixture wires a real single-node (or, for membership tests, a paired
// second node) MetaRaft leader plus a valid invite-path JoinRequest for node-b.
type inviteJoinFixture struct {
	leader     *MetaRaft
	receiver   *MetaJoinReceiver
	provider   *fakeBootstrapProvider
	req        JoinRequest
	inviteID   string
	spki       [32]byte
	joinerKey  *ecdsa.PrivateKey
	invitePriv ed25519.PrivateKey
}

// buildInviteJoinFixture starts a real single-node MetaRaft leader, mints an
// invite into its FSM via Raft, and builds a valid invite-path JoinRequest for
// node-b. joinerRaftAddr is the address the joiner advertises (it must equal the
// raft transport registration key of node-b for membership to complete; for
// Phase-1-only tests any value works because no membership is staged).
func buildInviteJoinFixture(t *testing.T, joinerRaftAddr string) inviteJoinFixture {
	t.Helper()
	m := newSingleMetaRaft(t)
	t.Cleanup(func() { _ = m.Close() })
	require.NoError(t, m.Bootstrap())
	require.NoError(t, m.Start(context.Background(), nil))
	require.Eventually(t, func() bool {
		return m.node.State() == raft.Leader
	}, 2*time.Second, 20*time.Millisecond, "leader must elect")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Mint an invite via Raft so the FSM records its public key (present, unused,
	// unexpired). The private key stays with the joiner to sign the transcript.
	invitePriv, invitePub, inviteID, err := MintInviteKeypair()
	require.NoError(t, err)
	require.NoError(t, m.ProposeInviteMint(ctx, inviteID, invitePub, time.Now().Add(time.Hour).UnixNano()))

	// Build the joiner per-node identity (cert + SPKI).
	cert, spki, err := transport.GenerateNodeIdentity("cluster-x", "node-b")
	require.NoError(t, err)
	joinerKey := cert.PrivateKey.(*ecdsa.PrivateKey)

	tr := encrypt.InviteTranscript{
		ClusterID: transcriptClusterID,
		Nonce:     []byte("joiner-nonce"),
		NodeID:    "node-b",
		Address:   joinerRaftAddr,
		SPKI:      spki[:],
		Bind:      nil,
	}
	inviteSig := encrypt.SignInviteTranscript(invitePriv, tr)
	nodeSig, err := encrypt.SignNodeTranscript(joinerKey, tr)
	require.NoError(t, err)

	provider := newFakeBootstrapProvider()
	receiver := NewMetaJoinReceiver(m).
		WithClusterID(transcriptClusterID).
		WithBootstrapSecretProvider(provider)

	req := JoinRequest{
		NodeID:         "node-b",
		Address:        joinerRaftAddr,
		HandshakeNonce: []byte("joiner-nonce"),
		SPKI:           spki[:],
		CertDER:        cert.Leaf.Raw,
		NodeSig:        nodeSig,
		InviteSig:      inviteSig,
		InviteID:       inviteID,
	}
	return inviteJoinFixture{
		leader:     m,
		receiver:   receiver,
		provider:   provider,
		req:        req,
		inviteID:   inviteID,
		spki:       spki,
		joinerKey:  joinerKey,
		invitePriv: invitePriv,
	}
}

func newSingleNodeInviteFixture(t *testing.T) inviteJoinFixture {
	return buildInviteJoinFixture(t, "10.0.0.2:7001")
}

// buildTwoNodeInviteFixture starts the leader plus a real second MetaRaft
// (node-b) registered in the SAME transport fake at the joiner's advertised
// address, so JoinViaInvite's AddLearner/PromoteToVoter can catch up and
// commit. It returns the fixture and the started joiner node (kept alive for
// the test lifetime).
func buildTwoNodeInviteFixture(t *testing.T) (inviteJoinFixture, *MetaRaft) {
	t.Helper()
	const joinerAddr = "node-b-addr"
	fx := buildInviteJoinFixture(t, joinerAddr)

	tr := fx.leader.cfg.Transport
	joiner, err := NewMetaRaft(MetaRaftConfig{
		NodeID:    "node-b",
		RaftID:    joinerAddr,
		Peers:     []string{"node-0"},
		JoinMode:  true,
		DataDir:   t.TempDir(),
		Transport: tr,
	})
	require.NoError(t, err)
	tr.(*MetaTransportFake).register(joinerAddr, joiner)
	require.NoError(t, joiner.Start(context.Background(), nil))
	t.Cleanup(func() { _ = joiner.Close() })
	return fx, joiner
}

// --- Phase-1 -----------------------------------------------------------------

func TestHandleJoin_Phase1_Valid_SealsBootstrapNoMembership(t *testing.T) {
	fx := newSingleNodeInviteFixture(t)
	ctx := context.Background()

	req := fx.req
	req.JoinPhase = 1
	reply := fx.receiver.HandleJoin(ctx, fx.spki, req)

	require.True(t, reply.Accepted)
	require.Equal(t, JoinStatusOK, reply.Status)
	require.NotEmpty(t, reply.SealedBootstrap, "Phase-1 must return a sealed bootstrap envelope")

	// The joiner can open the sealed bootstrap with its identity key.
	eph, ct, err := decodeSealedBootstrap(reply.SealedBootstrap)
	require.NoError(t, err)
	bind := fx.receiver.sealBindContext(req)
	plain, err := encrypt.OpenFromPeer(fx.joinerKey, encrypt.SealedToPeer{EphemeralPub: eph, Ciphertext: ct}, bind, bind)
	require.NoError(t, err)
	encKey, gens, psk, err := decodeBootstrapSecretsPayload(plain)
	require.NoError(t, err)
	require.Equal(t, fx.provider.encKey, encKey)
	require.Equal(t, fx.provider.psk, psk)
	require.Len(t, gens, 1)
	require.Equal(t, uint32(1), gens[0].Gen)

	// pending binding persisted.
	node, pendSPKI, _, ok := fx.leader.LookupPending(fx.inviteID)
	require.True(t, ok)
	require.Equal(t, "node-b", node)
	require.Equal(t, fx.spki, pendSPKI)

	// invite NOT yet consumed (Phase-2 consumes it).
	_, ok = fx.leader.LookupInvite(fx.inviteID, time.Now())
	require.True(t, ok, "invite must remain redeemable after Phase-1")

	// NO membership change: node-b must not be in the FSM nodes.
	require.False(t, fsmHasNode(fx.leader, "node-b"), "Phase-1 must not stage membership")
}

func TestHandleJoin_Phase1_CapturedSPKIMismatch_Rejected(t *testing.T) {
	fx := newSingleNodeInviteFixture(t)
	req := fx.req
	req.JoinPhase = 1

	var wrong [32]byte
	wrong[0] = 0xFF
	reply := fx.receiver.HandleJoin(context.Background(), wrong, req)

	require.False(t, reply.Accepted)
	require.Equal(t, JoinStatusError, reply.Status)
	require.Contains(t, reply.Message, "captured SPKI does not match")
}

func TestHandleJoin_Phase1_ReplayByDifferentIdentity_Rejected(t *testing.T) {
	fx := newSingleNodeInviteFixture(t)
	ctx := context.Background()

	// First redeemer binds the invite.
	first := fx.req
	first.JoinPhase = 1
	require.True(t, fx.receiver.HandleJoin(ctx, fx.spki, first).Accepted)

	// A DIFFERENT identity node-c re-redeems the SAME invite, signing its own
	// transcript with the SAME (leaked) invite key the FSM holds. The gate
	// passes, but ProposeInvitePending must reject the rebind to a different
	// (nodeID, spki) because the invite is already pending for node-b.
	cert, spkiC, err := transport.GenerateNodeIdentity("cluster-x", "node-c")
	require.NoError(t, err)
	keyC := cert.PrivateKey.(*ecdsa.PrivateKey)
	tr := encrypt.InviteTranscript{
		ClusterID: transcriptClusterID,
		Nonce:     []byte("joiner-nonce-c"),
		NodeID:    "node-c",
		Address:   "10.0.0.3:7001",
		SPKI:      spkiC[:],
		Bind:      nil,
	}
	reqC := JoinRequest{
		NodeID:         "node-c",
		Address:        "10.0.0.3:7001",
		HandshakeNonce: []byte("joiner-nonce-c"),
		SPKI:           spkiC[:],
		CertDER:        cert.Leaf.Raw,
		NodeSig:        mustSignNode(t, keyC, tr),
		InviteSig:      encrypt.SignInviteTranscript(fx.invitePriv, tr),
		InviteID:       fx.inviteID,
		JoinPhase:      1,
	}
	reply := fx.receiver.HandleJoin(ctx, spkiC, reqC)
	require.False(t, reply.Accepted)
	require.Equal(t, JoinStatusError, reply.Status)
}

// --- Phase-1 gate rejections (signature / SPKI / invite validity) ------------

func TestHandleJoin_Phase1_RejectsForgedNodeSig(t *testing.T) {
	fx := newSingleNodeInviteFixture(t)
	otherKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tr := encrypt.InviteTranscript{
		ClusterID: transcriptClusterID,
		Nonce:     fx.req.HandshakeNonce,
		NodeID:    fx.req.NodeID,
		Address:   fx.req.Address,
		SPKI:      fx.req.SPKI,
	}
	req := fx.req
	req.JoinPhase = 1
	req.NodeSig = mustSignNode(t, otherKey, tr)

	reply := fx.receiver.HandleJoin(context.Background(), fx.spki, req)
	require.False(t, reply.Accepted)
	require.Equal(t, "node signature invalid", reply.Message)
}

func TestHandleJoin_Phase1_RejectsSPKICertMismatch(t *testing.T) {
	fx := newSingleNodeInviteFixture(t)
	otherCert, _, err := transport.GenerateNodeIdentity("cluster-x", "node-z")
	require.NoError(t, err)
	req := fx.req
	req.JoinPhase = 1
	req.CertDER = otherCert.Leaf.Raw

	reply := fx.receiver.HandleJoin(context.Background(), fx.spki, req)
	require.False(t, reply.Accepted)
	require.Equal(t, "SPKI does not match presented cert", reply.Message)
}

func TestHandleJoin_Phase1_RejectsDenylistedSPKI(t *testing.T) {
	fx := newSingleNodeInviteFixture(t)
	fx.leader.fsm.peers.denylist(fx.spki)
	req := fx.req
	req.JoinPhase = 1

	reply := fx.receiver.HandleJoin(context.Background(), fx.spki, req)
	require.False(t, reply.Accepted)
	require.Contains(t, reply.Message, "SPKI denylisted")
}

func TestHandleJoin_Phase1_RejectsExpiredInvite(t *testing.T) {
	fx := newSingleNodeInviteFixture(t)
	// Re-mint the same invite-id already expired.
	_, expiredPub, _, err := MintInviteKeypair()
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, fx.leader.ProposeInviteMint(ctx, fx.inviteID, expiredPub, time.Now().Add(-time.Hour).UnixNano()))
	req := fx.req
	req.JoinPhase = 1

	reply := fx.receiver.HandleJoin(ctx, fx.spki, req)
	require.False(t, reply.Accepted)
	require.Equal(t, "invite invalid/used/expired", reply.Message)
}

func TestHandleJoin_Phase1_RejectsForgedInviteSig(t *testing.T) {
	fx := newSingleNodeInviteFixture(t)
	otherPriv, _, _, err := MintInviteKeypair()
	require.NoError(t, err)
	tr := encrypt.InviteTranscript{
		ClusterID: transcriptClusterID,
		Nonce:     fx.req.HandshakeNonce,
		NodeID:    fx.req.NodeID,
		Address:   fx.req.Address,
		SPKI:      fx.req.SPKI,
	}
	req := fx.req
	req.JoinPhase = 1
	req.InviteSig = encrypt.SignInviteTranscript(otherPriv, tr)

	reply := fx.receiver.HandleJoin(context.Background(), fx.spki, req)
	require.False(t, reply.Accepted)
	require.Equal(t, "invite signature invalid", reply.Message)
}

// --- Phase-2 -----------------------------------------------------------------

func TestHandleJoin_Phase2_ACK_StagesMembershipConsumesInvite(t *testing.T) {
	fx, joiner := buildTwoNodeInviteFixture(t)
	ctx := context.Background()

	hookRan := false
	fx.receiver = fx.receiver.WithPostJoinHook(func(context.Context, JoinRequest) error {
		hookRan = true
		return nil
	})

	// Phase-1 binds the invite.
	p1 := fx.req
	p1.JoinPhase = 1
	require.True(t, fx.receiver.HandleJoin(ctx, fx.spki, p1).Accepted)

	// Phase-2 ACK from the pending redeemer.
	p2 := fx.req
	p2.JoinPhase = 2
	reply := fx.receiver.HandleJoin(ctx, fx.spki, p2)
	require.True(t, reply.Accepted, "phase-2 reply: %+v", reply)
	require.Equal(t, JoinStatusOK, reply.Status)
	require.True(t, hookRan, "postJoinHook must run on Phase-2")

	// node-b promoted to a voting member + present in FSM nodes.
	require.True(t, fsmHasNode(fx.leader, "node-b"))
	require.Equal(t, peerStateMember, fx.leader.fsm.peers.byNodeID["node-b"].State)

	// invite consumed (used).
	_, ok := fx.leader.LookupInvite(fx.inviteID, time.Now())
	require.False(t, ok, "invite must be consumed after Phase-2")

	_ = joiner
}

func TestHandleJoin_Phase2_CapturedSPKIMismatch_Rejected(t *testing.T) {
	fx, _ := buildTwoNodeInviteFixture(t)
	ctx := context.Background()

	p1 := fx.req
	p1.JoinPhase = 1
	require.True(t, fx.receiver.HandleJoin(ctx, fx.spki, p1).Accepted)

	p2 := fx.req
	p2.JoinPhase = 2
	var wrong [32]byte
	wrong[0] = 0xAB
	reply := fx.receiver.HandleJoin(ctx, wrong, p2)
	require.False(t, reply.Accepted)
	require.Equal(t, JoinStatusError, reply.Status)
	require.Contains(t, reply.Message, "captured SPKI does not match pending")

	// No membership staged.
	require.False(t, fsmHasNode(fx.leader, "node-b"))
}

func TestHandleJoin_Phase2_Idempotent_ResumesToConsume(t *testing.T) {
	fx, _ := buildTwoNodeInviteFixture(t)
	ctx := context.Background()

	p1 := fx.req
	p1.JoinPhase = 1
	require.True(t, fx.receiver.HandleJoin(ctx, fx.spki, p1).Accepted)

	p2 := fx.req
	p2.JoinPhase = 2
	first := fx.receiver.HandleJoin(ctx, fx.spki, p2)
	require.True(t, first.Accepted)

	// Second Phase-2 invocation simulates a crash-after-membership retry: every
	// membership step is already applied and the invite is already consumed.
	// Idempotent JoinViaInvite + consume must still report success.
	second := fx.receiver.HandleJoin(ctx, fx.spki, p2)
	require.True(t, second.Accepted, "phase-2 retry must resume idempotently: %+v", second)

	// Still exactly one voter member + consumed invite.
	require.True(t, fsmHasNode(fx.leader, "node-b"))
	require.Equal(t, peerStateMember, fx.leader.fsm.peers.byNodeID["node-b"].State)
	_, ok := fx.leader.LookupInvite(fx.inviteID, time.Now())
	require.False(t, ok)
}

// TestHandleJoin_Phase2_ResumesFromMembershipAppliedNotConsumed reproduces the
// exact crash-after-AddNode-before-consume middle state: membership is staged
// directly (JoinViaInvite) WITHOUT consuming the invite, then Phase-2 is called
// once. It must resume — keep the membership, run the hook, and consume.
func TestHandleJoin_Phase2_ResumesFromMembershipAppliedNotConsumed(t *testing.T) {
	fx, _ := buildTwoNodeInviteFixture(t)
	ctx := context.Background()

	// Phase-1 binds the invite.
	p1 := fx.req
	p1.JoinPhase = 1
	require.True(t, fx.receiver.HandleJoin(ctx, fx.spki, p1).Accepted)

	// Simulate the crash middle state: membership fully staged, invite NOT
	// consumed. (JoinViaInvite is the membership-only half; it does not consume.)
	require.NoError(t, fx.leader.JoinViaInvite(ctx, "node-b", fx.req.Address, fx.spki, fx.inviteID))
	require.True(t, fsmHasNode(fx.leader, "node-b"))
	_, stillRedeemable := fx.leader.LookupInvite(fx.inviteID, time.Now())
	require.True(t, stillRedeemable, "precondition: invite not yet consumed")

	// Phase-2 ACK resumes: membership idempotently re-applied, invite consumed.
	p2 := fx.req
	p2.JoinPhase = 2
	reply := fx.receiver.HandleJoin(ctx, fx.spki, p2)
	require.True(t, reply.Accepted, "phase-2 must resume from middle state: %+v", reply)
	require.Equal(t, peerStateMember, fx.leader.fsm.peers.byNodeID["node-b"].State)
	_, ok := fx.leader.LookupInvite(fx.inviteID, time.Now())
	require.False(t, ok, "invite must be consumed after the resuming Phase-2")
}

// TestHandleJoin_Phase2_RejectsAddressOwnedByAnotherNode is the P1 ownership
// guard: a Phase-1 redeem may advertise ANY address (the joiner controls its own
// transcript). If that address is already owned by a DIFFERENT existing voter,
// the Phase-2 ACK must be REJECTED — otherwise AddLearner's already-learner /
// PromoteToVoter's already-voter would be tolerated as a resume and ProposeAddNode
// would register a NEW member/SPKI over the existing node's address (membership
// corruption + a burned single-use invite).
func TestHandleJoin_Phase2_RejectsAddressOwnedByAnotherNode(t *testing.T) {
	fx, _ := buildTwoNodeInviteFixture(t)
	ctx := context.Background()
	const ownedAddr = "node-b-addr" // node-b's real membership address.

	// node-b joins for real: Phase-1 + Phase-2 → voting member at ownedAddr.
	p1 := fx.req
	p1.JoinPhase = 1
	require.True(t, fx.receiver.HandleJoin(ctx, fx.spki, p1).Accepted)
	p2 := fx.req
	p2.JoinPhase = 2
	require.True(t, fx.receiver.HandleJoin(ctx, fx.spki, p2).Accepted)
	require.True(t, fsmHasNode(fx.leader, "node-b"))
	require.Equal(t, peerStateMember, fx.leader.fsm.peers.byNodeID["node-b"].State)

	// Mint a SECOND, independent invite for a different identity (node-c).
	invitePrivC, invitePubC, inviteIDC, err := MintInviteKeypair()
	require.NoError(t, err)
	require.NoError(t, fx.leader.ProposeInviteMint(ctx, inviteIDC, invitePubC, time.Now().Add(time.Hour).UnixNano()))

	certC, spkiC, err := transport.GenerateNodeIdentity("cluster-x", "node-c")
	require.NoError(t, err)
	keyC := certC.PrivateKey.(*ecdsa.PrivateKey)
	// node-c advertises node-b's address in its (validly signed) transcript.
	trC := encrypt.InviteTranscript{
		ClusterID: transcriptClusterID,
		Nonce:     []byte("joiner-nonce-c"),
		NodeID:    "node-c",
		Address:   ownedAddr,
		SPKI:      spkiC[:],
		Bind:      nil,
	}
	reqC := JoinRequest{
		NodeID:         "node-c",
		Address:        ownedAddr,
		HandshakeNonce: []byte("joiner-nonce-c"),
		SPKI:           spkiC[:],
		CertDER:        certC.Leaf.Raw,
		NodeSig:        mustSignNode(t, keyC, trC),
		InviteSig:      encrypt.SignInviteTranscript(invitePrivC, trC),
		InviteID:       inviteIDC,
	}

	// Phase-1 succeeds (fresh invite, own SPKI; the address is not validated at
	// Phase-1 — it is bound into the pending redemption).
	reqC.JoinPhase = 1
	require.True(t, fx.receiver.HandleJoin(ctx, spkiC, reqC).Accepted)

	// Phase-2 ACK must be REJECTED: ownedAddr belongs to node-b, not node-c.
	reqC.JoinPhase = 2
	reply := fx.receiver.HandleJoin(ctx, spkiC, reqC)
	require.False(t, reply.Accepted, "phase-2 must reject an address owned by another node")
	require.Equal(t, JoinStatusError, reply.Status)

	// node-b's membership is intact (no NEW member registered for the address, no
	// SPKI overwrite).
	require.True(t, fsmHasNode(fx.leader, "node-b"))
	require.Equal(t, fx.spki, fx.leader.fsm.peers.byNodeID["node-b"].SPKI)
	require.False(t, fsmHasNode(fx.leader, "node-c"), "node-c must NOT become a member")

	// node-c's invite is NOT consumed (still redeemable for a legitimate retry
	// with a correct address).
	_, ok := fx.leader.LookupInvite(inviteIDC, time.Now())
	require.True(t, ok, "the rejected join must NOT consume node-c's single-use invite")
}

// --- helpers -----------------------------------------------------------------

func fsmHasNode(m *MetaRaft, id string) bool {
	for _, n := range m.fsm.Nodes() {
		if n.ID == id {
			return true
		}
	}
	return false
}

func mustSignNode(t *testing.T, key *ecdsa.PrivateKey, tr encrypt.InviteTranscript) []byte {
	t.Helper()
	sig, err := encrypt.SignNodeTranscript(key, tr)
	require.NoError(t, err)
	return sig
}
