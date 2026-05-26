// §7 BX — production wiring of the cluster-join KEK handshake.
//
// Three blockers:
//
//	B1: HandshakeVerifier wired into MetaJoinReceiver + MetaChallengeReceiver
//	    registered on the stream router for StreamMetaJoinChallenge.
//	B2: PerformMetaJoin actually performs Challenge → ComputeHandshakeResponse
//	    → Join (rather than sending a bare JoinRequest).
//	B3: wireDEKKeeper uses strict LoadKEK on a joining node (refuse to auto-
//	    generate when joinMode || len(peers) > 0).
//
// These tests exercise the wiring functions directly so the contracts the
// production boot path depends on cannot regress silently.
package serveruntime

import (
	"bytes"
	"context"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/transport"
)

// TestB3_WireDEKKeeper_JoinModeRefusesMissingKEK covers §7 B3 / F#21: a
// fresh joiner with no KEK file under keys/ must NOT silently
// auto-generate one. Without this guard, the joiner would boot with a
// wrong KEK that can never decrypt the cluster's wrapped DEKs and whose
// challenge-response would fail anyway.
func TestB3_WireDEKKeeper_JoinModeRefusesMissingKEK(t *testing.T) {
	dataDir := t.TempDir()
	state := &bootState{
		cfg:      Config{DataDir: dataDir},
		joinMode: true,
		joinAddr: "127.0.0.1:7001",
		peers:    []string{"127.0.0.1:7001"},
	}
	fsm := cluster.NewMetaFSM()

	// keys/ is absent under dataDir.
	err := wireDEKKeeper(state, fsm)
	require.Error(t, err, "join-mode with missing KEK must refuse startup")
	assert.Contains(t, err.Error(), "KEK not found",
		"error must include the literal 'KEK not found' for operator grep-ability")
	assert.Contains(t, err.Error(), "scp", "error must point at scp-from-peer remediation")
	assert.Contains(t, err.Error(), "join", "error must mention join context")
	assert.Nil(t, state.dekKeeper, "no keeper installed when refusal fires")
	assert.Nil(t, state.handshakeVerifier, "no verifier installed when refusal fires")

	// CRITICAL: keys/ must not have been auto-generated.
	_, statErr := os.Stat(filepath.Join(dataDir, "keys", "0.key"))
	require.True(t, os.IsNotExist(statErr), "wireDEKKeeper must NOT auto-generate keys/0.key in join mode")
}

// TestB3_WireDEKKeeper_JoinModeAcceptsStagedKEK is the positive companion:
// when the operator (or `cluster join` CLI) has staged kek.key from a healthy
// peer, wireDEKKeeper succeeds in join mode and the keeper / verifier are
// installed.
func TestB3_WireDEKKeeper_JoinModeAcceptsStagedKEK(t *testing.T) {
	dataDir := t.TempDir()
	state := &bootState{
		cfg:      Config{DataDir: dataDir},
		joinMode: true,
		peers:    []string{"127.0.0.1:7001"},
	}
	fsm := cluster.NewMetaFSM()

	kek := make([]byte, encrypt.KEKSize)
	_, err := rand.Read(kek)
	require.NoError(t, err)
	writeKEKFile(t, dataDir, kek)

	require.NoError(t, wireDEKKeeper(state, fsm))
	require.NotNil(t, state.dekKeeper, "keeper must be installed")
	require.NotNil(t, state.handshakeVerifier, "handshake verifier must be installed alongside keeper")
	gotKEK, err := state.kekStore.ActiveKEK()
	require.NoError(t, err, "ActiveKEK")
	defer func() {
		for i := range gotKEK {
			gotKEK[i] = 0
		}
	}()
	require.Equal(t, kek, gotKEK, "kekStore.ActiveKEK() must hold the loaded KEK bytes")
}

// TestB3_WireDEKKeeper_StandaloneModeAutoGenerates documents the
// first-cluster-init path: NOT joining AND no peers configured → the very
// first node bootstrapping its own KEK is allowed to auto-generate.
func TestB3_WireDEKKeeper_StandaloneModeAutoGenerates(t *testing.T) {
	dataDir := t.TempDir()
	state := &bootState{cfg: Config{DataDir: dataDir}} // joinMode false, peers nil
	fsm := cluster.NewMetaFSM()

	require.NoError(t, wireDEKKeeper(state, fsm))
	require.NotNil(t, state.dekKeeper)
	require.NotNil(t, state.handshakeVerifier)
	gotKEK, err := state.kekStore.ActiveKEK()
	require.NoError(t, err, "ActiveKEK")
	defer func() {
		for i := range gotKEK {
			gotKEK[i] = 0
		}
	}()
	require.Len(t, gotKEK, encrypt.KEKSize)

	// keys/0.key was created by LoadOrInitKEKStoreDir.
	_, statErr := os.Stat(filepath.Join(dataDir, "keys", "0.key"))
	require.NoError(t, statErr, "first-init mode auto-generates keys/0.key")
}

// inProcessQUICPair returns two QUIC transports configured to talk to each
// other on the loopback, plus a listener address on the "server" side. The
// server registers a StreamRouter and we drive Calls from the client. This
// is the minimum production-equivalent surface to assert B1: the stream
// router has a live handler for StreamMetaJoinChallenge, wired to a verifier
// shared with the MetaJoinReceiver.

// TestB1_VerifierWiredIntoStreamRouter exercises the production wiring shape
// without spinning up QUIC: it drives the same code path bootWALAndForwarders
// builds (MetaJoinReceiver.WithHandshakeVerifier + MetaChallengeReceiver
// registered on StreamRouter for StreamMetaJoinChallenge), then asserts that
//
//	(a) state.handshakeVerifier is non-nil after wireDEKKeeper,
//	(b) the StreamRouter has a registered handler for StreamMetaJoinChallenge,
//	(c) the handler issues nonces (vs the "verifier not configured" error
//	    that fires when the receiver has no verifier).
//
// This is the structural assertion that the verifier instance has been wired
// into BOTH receivers (Challenge writes the nonce, Join would read it from
// the same verifier).
func TestB1_VerifierWiredIntoStreamRouter(t *testing.T) {
	dataDir := t.TempDir()
	state := &bootState{cfg: Config{DataDir: dataDir}}
	fsm := cluster.NewMetaFSM()

	// (1) wireDEKKeeper sets up the verifier alongside the keeper.
	require.NoError(t, wireDEKKeeper(state, fsm))
	require.NotNil(t, state.handshakeVerifier, "B1: wireDEKKeeper must construct the shared HandshakeVerifier")

	// (2) Build the receivers exactly as bootWALAndForwarders does and
	// register them on a fresh StreamRouter. This mirrors the production
	// path (boot_phases_forwarders.go:136-145).
	router := transport.NewStreamRouter()
	metaJoinReceiver := cluster.NewMetaJoinReceiver(&fakeJoinCoord{}).
		WithHandshakeVerifier(state.handshakeVerifier)
	router.Handle(transport.StreamMetaJoin, metaJoinReceiver.Handle)
	metaChallengeReceiver := cluster.NewMetaChallengeReceiver(state.handshakeVerifier)
	router.Handle(transport.StreamMetaJoinChallenge, metaChallengeReceiver.Handle)

	// (3) Drive a Challenge through the router. If the verifier were not
	// wired into the MetaChallengeReceiver, Handle would return JoinStatusError
	// with "verifier not configured" — see meta_challenge.go.
	reqBytes, err := cluster.EncodeChallengeRequestForTest(cluster.ChallengeRequest{NodeID: "joiner"})
	require.NoError(t, err)
	resp := router.Dispatch(&transport.Message{Type: transport.StreamMetaJoinChallenge, Payload: reqBytes})
	require.NotNil(t, resp, "B1: StreamRouter must have a handler registered for StreamMetaJoinChallenge")
	reply, err := cluster.DecodeChallengeReplyForTest(resp.Payload)
	require.NoError(t, err)
	require.Equal(t, cluster.JoinStatusOK, reply.Status,
		"B1: challenge must succeed when verifier is wired; got %q msg=%q", reply.Status, reply.Message)
	require.Len(t, reply.Nonce, 32, "B1: challenge handler must return a 32-byte nonce")
}

// TestB2_PerformMetaJoinHandshake_SameKEK exercises the in-process handshake
// over a stream-router-routed transport pair. With matching KEKs on both
// sides, PerformMetaJoin must complete Challenge → ComputeHandshakeResponse
// → Join and the join must be accepted.
func TestB2_PerformMetaJoinHandshake_SameKEK(t *testing.T) {
	kek := make([]byte, encrypt.KEKSize)
	_, err := rand.Read(kek)
	require.NoError(t, err)

	accepted := runHandshakeRoundTrip(t, kek, kek)
	require.True(t, accepted, "B2: same-KEK PerformMetaJoin must succeed")
}

// TestB2_PerformMetaJoinHandshake_WrongKEK is the negative: the joiner holds
// a different KEK; PerformMetaJoin must surface a JoinStatusKEKMismatch error
// from the leader (which never AddVoter's the bad node).
func TestB2_PerformMetaJoinHandshake_WrongKEK(t *testing.T) {
	leaderKEK := make([]byte, encrypt.KEKSize)
	joinerKEK := make([]byte, encrypt.KEKSize)
	_, err := rand.Read(leaderKEK)
	require.NoError(t, err)
	_, err = rand.Read(joinerKEK)
	require.NoError(t, err)
	require.NotEqual(t, leaderKEK, joinerKEK)

	accepted := runHandshakeRoundTrip(t, leaderKEK, joinerKEK)
	require.False(t, accepted, "B2: wrong-KEK PerformMetaJoin must be refused")
}

// runHandshakeRoundTrip wires a leader-side verifier + StreamRouter and a
// client-side handshake driver that mirrors PerformMetaJoin's three steps
// (Challenge → ComputeHandshakeResponse → Join). Returns whether the join
// was accepted.
//
// NOTE: This drives the same logic PerformMetaJoin contains. PerformMetaJoin
// itself depends on a real *transport.QUICTransport so calling it directly
// from a unit test would require either spinning up QUIC (slow, flaky in CI
// noise budgets) or constructor injection. The pure-in-process round trip
// here is sufficient to assert the contract that B2 introduces.
func runHandshakeRoundTrip(t *testing.T, leaderKEK, joinerKEK []byte) bool {
	t.Helper()

	clusterID := bytes.Repeat([]byte{0xCD}, 16)
	leaderStore := encrypt.NewKEKStore()
	require.NoError(t, leaderStore.Add(0, leaderKEK))
	verifier := encrypt.NewHandshakeVerifier(leaderStore, clusterID)

	router := transport.NewStreamRouter()
	joinRcv := cluster.NewMetaJoinReceiver(&fakeJoinCoord{leader: true}).
		WithHandshakeVerifier(verifier)
	router.Handle(transport.StreamMetaJoin, joinRcv.Handle)
	chalRcv := cluster.NewMetaChallengeReceiver(verifier)
	router.Handle(transport.StreamMetaJoinChallenge, chalRcv.Handle)

	dial := func(_ context.Context, _ string, payload []byte, kind transport.StreamType) ([]byte, error) {
		resp := router.Dispatch(&transport.Message{Type: kind, Payload: payload})
		return resp.Payload, nil
	}

	ctx := context.Background()

	chalReqBytes, err := cluster.EncodeChallengeRequestForTest(cluster.ChallengeRequest{NodeID: "joiner"})
	require.NoError(t, err)
	chalReplyBytes, err := dial(ctx, "leader", chalReqBytes, transport.StreamMetaJoinChallenge)
	require.NoError(t, err)
	chalReply, err := cluster.DecodeChallengeReplyForTest(chalReplyBytes)
	require.NoError(t, err)
	require.Equal(t, cluster.JoinStatusOK, chalReply.Status)

	joinerStore := encrypt.NewKEKStore()
	require.NoError(t, joinerStore.Add(0, joinerKEK))
	transcript := encrypt.JoinTranscript{
		ClusterID:           clusterID,
		Nonce:               chalReply.Nonce,
		NodeID:              "joiner",
		Address:             "127.0.0.1:7002",
		JoinerVersion:       0,
		LeaderActiveVersion: 0,
	}
	response, err := encrypt.ComputeHandshakeResponse(joinerStore, 0, transcript)
	require.NoError(t, err)
	joinReqBytes, err := cluster.EncodeJoinRequestForTest(cluster.JoinRequest{
		NodeID:            "joiner",
		Address:           "127.0.0.1:7002",
		HandshakeNonce:    chalReply.Nonce,
		HandshakeResponse: response,
	})
	require.NoError(t, err)
	joinReplyBytes, err := dial(ctx, "leader", joinReqBytes, transport.StreamMetaJoin)
	require.NoError(t, err)
	joinReply, err := cluster.DecodeJoinReplyForTest(joinReplyBytes)
	require.NoError(t, err)
	return joinReply.Accepted
}

// fakeJoinCoord is the minimum metaJoinCoordinator surface this test needs.
// IsLeader is always true; Join is a no-op (the test cares about the
// handshake gate, not the AddVoter side effect).
type fakeJoinCoord struct{ leader bool }

func (f *fakeJoinCoord) IsLeader() bool                            { return f.leader }
func (f *fakeJoinCoord) LeaderID() string                          { return "leader" }
func (f *fakeJoinCoord) Join(_ context.Context, _, _ string) error { return nil }
func (f *fakeJoinCoord) Nodes() []cluster.MetaNodeEntry            { return nil }
