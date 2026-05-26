// §7 T55 — Cluster-join transport: KEK handshake via Challenge + Join.
//
// These tests exercise the Challenge/Join handler pair directly (no QUIC)
// using a shared *encrypt.HandshakeVerifier. The verifier instance must be
// shared between the two receivers — Challenge writes to issued-nonce map,
// Join reads from it.
package cluster

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/transport"
)

// newJoinHandshakeFixtures returns a KEKStore seeded with the given KEK
// at version 0 and a fixed 16-byte cluster_id, matching the Phase A wiring
// the production receivers use.
func newJoinHandshakeFixtures(t *testing.T, kek []byte) (*encrypt.KEKStore, []byte) {
	t.Helper()
	store := encrypt.NewKEKStore()
	if err := store.Add(0, kek); err != nil {
		t.Fatalf("seed KEKStore: %v", err)
	}
	clusterID := bytes.Repeat([]byte{0xAB}, 16)
	return store, clusterID
}

// joinerTranscript builds the JoinTranscript the joiner-side will sign.
// Mirrors the Phase A version pin (joiner=0, leader=0).
func joinerTranscript(clusterID, nonce []byte, nodeID, addr string) encrypt.JoinTranscript {
	return encrypt.JoinTranscript{
		ClusterID:           clusterID,
		Nonce:               nonce,
		NodeID:              nodeID,
		Address:             addr,
		JoinerVersion:       0,
		LeaderActiveVersion: 0,
	}
}

func TestChallengeRequestReply_RoundTrip(t *testing.T) {
	reqIn := ChallengeRequest{NodeID: "node-2"}
	reqBytes, err := encodeChallengeRequest(reqIn)
	require.NoError(t, err)
	reqOut, err := decodeChallengeRequest(reqBytes)
	require.NoError(t, err)
	require.Equal(t, reqIn, reqOut)

	replyIn := ChallengeReply{Nonce: []byte("\x01\x02\x03\x04"), Status: JoinStatusOK, Message: "ok"}
	replyBytes, err := encodeChallengeReply(replyIn)
	require.NoError(t, err)
	replyOut, err := decodeChallengeReply(replyBytes)
	require.NoError(t, err)
	require.Equal(t, replyIn.Status, replyOut.Status)
	require.Equal(t, replyIn.Message, replyOut.Message)
	require.Equal(t, replyIn.Nonce, replyOut.Nonce)
}

func TestJoinRequest_HandshakeFields_RoundTrip(t *testing.T) {
	in := JoinRequest{
		NodeID:            "node-2",
		Address:           "10.0.0.2:7001",
		HandshakeNonce:    []byte("\xaa\xbb\xcc"),
		HandshakeResponse: []byte("\x11\x22\x33\x44"),
	}
	data, err := encodeJoinRequest(in)
	require.NoError(t, err)
	out, err := decodeJoinRequest(data)
	require.NoError(t, err)
	require.Equal(t, in.NodeID, out.NodeID)
	require.Equal(t, in.Address, out.Address)
	require.Equal(t, in.HandshakeNonce, out.HandshakeNonce)
	require.Equal(t, in.HandshakeResponse, out.HandshakeResponse)
}

// TestClusterJoin_E2E_SameKEK exercises Challenge → Join with both sides
// holding the same KEK. The joiner gets a nonce, computes the HMAC under its
// KEK, and the leader admits it via AddVoter (here a fake coordinator).
func TestClusterJoin_E2E_SameKEK(t *testing.T) {
	kek := make([]byte, 32)
	for i := range kek {
		kek[i] = byte(i)
	}
	leaderStore, clusterID := newJoinHandshakeFixtures(t, kek)
	verifier := encrypt.NewHandshakeVerifier(leaderStore, clusterID)

	f := NewMetaFSM()
	coord := &fakeJoinCoordinator{
		leader: true,
		fsm:    f,
		onJoin: func(id, addr string) {
			require.NoError(t, f.applyCmd(makeAddNodeCmd(t, id, addr, 0)))
		},
	}
	joinRcv := NewMetaJoinReceiver(coord).WithHandshakeVerifier(verifier)
	chalRcv := NewMetaChallengeReceiver(verifier)

	// 1) Joiner asks for a nonce.
	chalReqBytes, err := encodeChallengeRequest(ChallengeRequest{NodeID: "node-2"})
	require.NoError(t, err)
	chalResp := chalRcv.Handle(&transport.Message{Type: transport.StreamMetaJoinChallenge, Payload: chalReqBytes})
	chalReply, err := decodeChallengeReply(chalResp.Payload)
	require.NoError(t, err)
	require.Equal(t, JoinStatusOK, chalReply.Status)
	require.Len(t, chalReply.Nonce, 32)

	// 2) Joiner computes HMAC(K_active, transcript) under its (matching) KEK.
	joinerStore, _ := newJoinHandshakeFixtures(t, kek)
	transcript := joinerTranscript(clusterID, chalReply.Nonce, "node-2", "10.0.0.2:7001")
	response, err := encrypt.ComputeHandshakeResponse(joinerStore, 0, transcript)
	require.NoError(t, err)

	// 3) Joiner calls Join with (nonce, response). The leader verifies and
	// admits — onJoin records the new member.
	joinReqBytes, err := encodeJoinRequest(JoinRequest{
		NodeID:            "node-2",
		Address:           "10.0.0.2:7001",
		HandshakeNonce:    chalReply.Nonce,
		HandshakeResponse: response,
	})
	require.NoError(t, err)
	joinResp := joinRcv.Handle(&transport.Message{Type: transport.StreamMetaJoin, Payload: joinReqBytes})
	joinReply, err := decodeJoinReply(joinResp.Payload)
	require.NoError(t, err)

	require.True(t, joinReply.Accepted, "join must be accepted with matching KEK; got status=%q msg=%q", joinReply.Status, joinReply.Message)
	require.Equal(t, JoinStatusOK, joinReply.Status)
	require.Equal(t, 1, coord.JoinCalls(), "AddVoter must run exactly once on success")
}

// TestClusterJoin_E2E_WrongKEK_403 exercises Challenge → Join with the joiner
// holding a different KEK. VerifyResponse fails; the receiver returns
// JoinStatusKEKMismatch and AddVoter is NOT called.
func TestClusterJoin_E2E_WrongKEK_403(t *testing.T) {
	leaderKEK := make([]byte, 32)
	for i := range leaderKEK {
		leaderKEK[i] = byte(i)
	}
	joinerKEK := make([]byte, 32)
	for i := range joinerKEK {
		joinerKEK[i] = byte(255 - i) // different KEK
	}
	leaderStore, clusterID := newJoinHandshakeFixtures(t, leaderKEK)
	verifier := encrypt.NewHandshakeVerifier(leaderStore, clusterID)

	f := NewMetaFSM()
	coord := &fakeJoinCoordinator{
		leader: true,
		fsm:    f,
		onJoin: func(id, addr string) {
			t.Fatalf("AddVoter must not run on KEK mismatch (id=%s addr=%s)", id, addr)
		},
	}
	joinRcv := NewMetaJoinReceiver(coord).WithHandshakeVerifier(verifier)
	chalRcv := NewMetaChallengeReceiver(verifier)

	chalReqBytes, err := encodeChallengeRequest(ChallengeRequest{NodeID: "node-2"})
	require.NoError(t, err)
	chalResp := chalRcv.Handle(&transport.Message{Type: transport.StreamMetaJoinChallenge, Payload: chalReqBytes})
	chalReply, err := decodeChallengeReply(chalResp.Payload)
	require.NoError(t, err)
	require.Equal(t, JoinStatusOK, chalReply.Status)

	// Joiner computes the HMAC under the WRONG KEK (but signs the same
	// transcript shape — the MAC will not match the verifier's expected
	// MAC under leaderKEK).
	joinerStore, _ := newJoinHandshakeFixtures(t, joinerKEK)
	transcript := joinerTranscript(clusterID, chalReply.Nonce, "node-2", "10.0.0.2:7001")
	badResponse, err := encrypt.ComputeHandshakeResponse(joinerStore, 0, transcript)
	require.NoError(t, err)

	joinReqBytes, err := encodeJoinRequest(JoinRequest{
		NodeID:            "node-2",
		Address:           "10.0.0.2:7001",
		HandshakeNonce:    chalReply.Nonce,
		HandshakeResponse: badResponse,
	})
	require.NoError(t, err)
	joinResp := joinRcv.Handle(&transport.Message{Type: transport.StreamMetaJoin, Payload: joinReqBytes})
	joinReply, err := decodeJoinReply(joinResp.Payload)
	require.NoError(t, err)

	require.False(t, joinReply.Accepted, "join must be refused on KEK mismatch")
	require.Equal(t, JoinStatusKEKMismatch, joinReply.Status)
	require.Contains(t, joinReply.Message, "KEK handshake failed")
	require.Equal(t, 0, coord.JoinCalls(), "AddVoter must not be called on KEK mismatch")
}

// TestClusterJoin_E2E_NoVerifier_AcceptsLegacy documents the §7 T55 transitional
// behavior: when no verifier is installed, Handle skips the HMAC gate and
// accepts on the existing fields. T57 will tighten this by requiring a
// verifier at boot.
func TestClusterJoin_E2E_NoVerifier_AcceptsLegacy(t *testing.T) {
	f := NewMetaFSM()
	coord := &fakeJoinCoordinator{
		leader: true,
		fsm:    f,
		onJoin: func(id, addr string) {
			require.NoError(t, f.applyCmd(makeAddNodeCmd(t, id, addr, 0)))
		},
	}
	joinRcv := NewMetaJoinReceiver(coord) // no verifier

	payload, err := encodeJoinRequest(JoinRequest{NodeID: "node-2", Address: "10.0.0.2:7001"})
	require.NoError(t, err)
	resp := joinRcv.Handle(&transport.Message{Type: transport.StreamMetaJoin, Payload: payload})
	reply, err := decodeJoinReply(resp.Payload)
	require.NoError(t, err)
	require.Equal(t, JoinStatusOK, reply.Status)
}

// TestChallengeReceiver_NoVerifier_Errors documents that a misconfigured
// Challenge receiver (no verifier) returns an error rather than panicking.
func TestChallengeReceiver_NoVerifier_Errors(t *testing.T) {
	rcv := NewMetaChallengeReceiver(nil)
	payload, err := encodeChallengeRequest(ChallengeRequest{NodeID: "node-2"})
	require.NoError(t, err)
	resp := rcv.Handle(&transport.Message{Type: transport.StreamMetaJoinChallenge, Payload: payload})
	reply, err := decodeChallengeReply(resp.Payload)
	require.NoError(t, err)
	require.Equal(t, JoinStatusError, reply.Status)
	require.Empty(t, reply.Nonce)
}

func TestMetaJoinChallenge_StreamTypeReserved(t *testing.T) {
	require.Equal(t, transport.StreamType(0x16), transport.StreamMetaJoinChallenge)
}
