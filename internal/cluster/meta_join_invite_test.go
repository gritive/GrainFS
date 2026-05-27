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
	"github.com/gritive/GrainFS/internal/transport"
)

// transcriptClusterID is the 16-byte cluster identifier bound into the invite
// transcript. It only needs to match on both the joiner and receiver sides; it
// is independent of the string clusterID GenerateNodeIdentity puts in the SAN.
var transcriptClusterID = []byte("cluster-x-16byt!")

// fakeInviteCoordinator implements metaJoinCoordinator backed by REAL peer
// registry + invite FSM instances. Its JoinViaInvite records the commit1 state
// (registerPendingLearner + applyConsume) so the test can assert without Raft.
type fakeInviteCoordinator struct {
	registry *peerRegistry
	invites  *inviteFSM
	nodes    []MetaNodeEntry
	joinErr  error
}

func newFakeInviteCoordinator() *fakeInviteCoordinator {
	return &fakeInviteCoordinator{registry: newPeerRegistry(), invites: newInviteFSM()}
}

func (f *fakeInviteCoordinator) IsLeader() bool         { return true }
func (f *fakeInviteCoordinator) LeaderID() string       { return "leader-1" }
func (f *fakeInviteCoordinator) Nodes() []MetaNodeEntry { return f.nodes }

func (f *fakeInviteCoordinator) IsSPKIDenylisted(spki [32]byte) bool {
	return f.registry.isDenylisted(spki)
}
func (f *fakeInviteCoordinator) SPKIOwner(spki [32]byte) (string, bool) {
	return f.registry.spkiOwner(spki)
}
func (f *fakeInviteCoordinator) LookupInvite(id string, now time.Time) (ed25519.PublicKey, bool) {
	return f.invites.lookup(id, now)
}
func (f *fakeInviteCoordinator) AcceptSPKIBytes() [][]byte {
	return f.registry.acceptSPKIBytes()
}

func (f *fakeInviteCoordinator) Join(ctx context.Context, id, addr string) error {
	return f.joinErr
}

func (f *fakeInviteCoordinator) JoinViaInvite(ctx context.Context, nodeID, addr string, spki [32]byte, inviteID string) error {
	if f.joinErr != nil {
		return f.joinErr
	}
	if err := f.registry.registerPendingLearner(nodeID, spki, addr); err != nil {
		return err
	}
	if err := f.invites.applyConsume(inviteID, time.Now()); err != nil {
		return err
	}
	f.nodes = append(f.nodes, MetaNodeEntry{ID: nodeID, Address: addr})
	return nil
}

// inviteJoinFixture builds a valid invite-path JoinRequest for node-b.
type inviteJoinFixture struct {
	coord    *fakeInviteCoordinator
	req      JoinRequest
	inviteID string
}

func buildInviteJoinFixture(t *testing.T) inviteJoinFixture {
	t.Helper()
	coord := newFakeInviteCoordinator()

	// Mint an invite and record its public key in the FSM (present, unused,
	// unexpired). The private key stays with the joiner to sign the transcript.
	invitePriv, invitePub, inviteID, err := MintInviteKeypair()
	require.NoError(t, err)
	coord.invites.applyMint(inviteID, invitePub, time.Now().Add(time.Hour).UnixNano())

	// Build the joiner per-node identity (cert + SPKI).
	cert, spki, err := transport.GenerateNodeIdentity("cluster-x", "node-b")
	require.NoError(t, err)
	joinerKey := cert.PrivateKey.(*ecdsa.PrivateKey)

	tr := encrypt.InviteTranscript{
		ClusterID: transcriptClusterID,
		Nonce:     []byte("joiner-nonce"),
		NodeID:    "node-b",
		Address:   "10.0.0.2:7001",
		SPKI:      spki[:],
		Bind:      nil,
	}
	inviteSig := encrypt.SignInviteTranscript(invitePriv, tr)
	nodeSig, err := encrypt.SignNodeTranscript(joinerKey, tr)
	require.NoError(t, err)

	req := JoinRequest{
		NodeID:         "node-b",
		Address:        "10.0.0.2:7001",
		HandshakeNonce: []byte("joiner-nonce"),
		SPKI:           spki[:],
		CertDER:        cert.Leaf.Raw,
		NodeSig:        nodeSig,
		InviteSig:      inviteSig,
		InviteID:       inviteID,
	}
	return inviteJoinFixture{coord: coord, req: req, inviteID: inviteID}
}

func handleInvite(t *testing.T, coord *fakeInviteCoordinator, req JoinRequest) *JoinReply {
	t.Helper()
	receiver := NewMetaJoinReceiver(coord).WithClusterID(transcriptClusterID)
	payload, err := encodeJoinRequest(req)
	require.NoError(t, err)
	resp := receiver.Handle(&transport.Message{Type: transport.StreamMetaJoin, Payload: payload})
	reply, err := decodeJoinReply(resp.Payload)
	require.NoError(t, err)
	return reply
}

func TestJoinReceiver_InvitePath_PendingLearnerNoKEK(t *testing.T) {
	fx := buildInviteJoinFixture(t)
	reply := handleInvite(t, fx.coord, fx.req)

	require.True(t, reply.Accepted)
	require.Equal(t, JoinStatusOK, reply.Status)

	// registry shows node-b as a pending learner (same-package direct read;
	// no lookupByNodeID accessor until Task 6 wires per-peer dial pinning).
	e, ok := fx.coord.registry.byNodeID["node-b"]
	require.True(t, ok)
	require.Equal(t, peerStatePendingLearner, e.State)

	// invite consumed → lookup now false.
	_, ok = fx.coord.invites.lookup(fx.inviteID, time.Now())
	require.False(t, ok)

	// reply carries the accept-set; there is NO KEK field at all.
	require.Len(t, reply.PeerSPKIs, 1)
	require.Equal(t, fx.req.SPKI, reply.PeerSPKIs[0])
}

func TestJoinReceiver_InvitePath_RejectsReusedInvite(t *testing.T) {
	fx := buildInviteJoinFixture(t)
	first := handleInvite(t, fx.coord, fx.req)
	require.True(t, first.Accepted)

	second := handleInvite(t, fx.coord, fx.req)
	require.False(t, second.Accepted)
	require.Equal(t, JoinStatusError, second.Status)
}

func TestJoinReceiver_InvitePath_RejectsForgedNodeSig(t *testing.T) {
	fx := buildInviteJoinFixture(t)

	// Sign NodeSig with a DIFFERENT ecdsa key than the presented cert.
	otherKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tr := encrypt.InviteTranscript{
		ClusterID: transcriptClusterID,
		Nonce:     fx.req.HandshakeNonce,
		NodeID:    fx.req.NodeID,
		Address:   fx.req.Address,
		SPKI:      fx.req.SPKI,
		Bind:      nil,
	}
	forged, err := encrypt.SignNodeTranscript(otherKey, tr)
	require.NoError(t, err)
	fx.req.NodeSig = forged

	reply := handleInvite(t, fx.coord, fx.req)
	require.False(t, reply.Accepted)
	require.Equal(t, JoinStatusError, reply.Status)
}

func TestJoinReceiver_InvitePath_RejectsSPKICertMismatch(t *testing.T) {
	fx := buildInviteJoinFixture(t)

	// CertDER from a DIFFERENT identity than the claimed SPKI.
	otherCert, _, err := transport.GenerateNodeIdentity("cluster-x", "node-c")
	require.NoError(t, err)
	fx.req.CertDER = otherCert.Leaf.Raw

	reply := handleInvite(t, fx.coord, fx.req)
	require.False(t, reply.Accepted)
	require.Equal(t, JoinStatusError, reply.Status)
	require.Equal(t, "SPKI does not match presented cert", reply.Message)
}

func TestJoinReceiver_InvitePath_RejectsDenylistedSPKI(t *testing.T) {
	fx := buildInviteJoinFixture(t)
	var spki [32]byte
	copy(spki[:], fx.req.SPKI)
	fx.coord.registry.denylist(spki)

	reply := handleInvite(t, fx.coord, fx.req)
	require.False(t, reply.Accepted)
	require.Equal(t, JoinStatusError, reply.Status)
	require.Contains(t, reply.Message, "SPKI denylisted")
}

func TestJoinReceiver_InvitePath_RejectsSPKIAlreadyRegisteredByOtherNode(t *testing.T) {
	fx := buildInviteJoinFixture(t)
	var spki [32]byte
	copy(spki[:], fx.req.SPKI)
	// Pre-register the same SPKI under a different node-id.
	err := fx.coord.registry.registerPendingLearner("node-other", spki, "10.0.0.99:7001")
	require.NoError(t, err)

	reply := handleInvite(t, fx.coord, fx.req)
	require.False(t, reply.Accepted)
	require.Equal(t, JoinStatusError, reply.Status)
}

func TestJoinReceiver_InvitePath_RejectsExpiredInvite(t *testing.T) {
	fx := buildInviteJoinFixture(t)
	// Overwrite the invite with an already-expired one.
	_, expiredPub, _, err := MintInviteKeypair()
	require.NoError(t, err)
	fx.coord.invites.applyMint(fx.inviteID, expiredPub, time.Now().Add(-time.Hour).UnixNano())

	reply := handleInvite(t, fx.coord, fx.req)
	require.False(t, reply.Accepted)
	require.Equal(t, JoinStatusError, reply.Status)
}

func TestJoinReceiver_InvitePath_RejectsForgedInviteSig(t *testing.T) {
	fx := buildInviteJoinFixture(t)
	// Sign InviteSig with a DIFFERENT ed25519 key than the one minted into the FSM.
	otherPriv, _, _, err := MintInviteKeypair()
	require.NoError(t, err)
	tr := encrypt.InviteTranscript{
		ClusterID: transcriptClusterID,
		Nonce:     fx.req.HandshakeNonce,
		NodeID:    fx.req.NodeID,
		Address:   fx.req.Address,
		SPKI:      fx.req.SPKI,
		Bind:      nil,
	}
	fx.req.InviteSig = encrypt.SignInviteTranscript(otherPriv, tr)

	reply := handleInvite(t, fx.coord, fx.req)
	require.False(t, reply.Accepted)
	require.Equal(t, JoinStatusError, reply.Status)
	require.Contains(t, reply.Message, "invite signature invalid")
}
