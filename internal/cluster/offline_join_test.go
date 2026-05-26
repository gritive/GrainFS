// §7 — Offline cluster-join handshake tests (pushed down from
// cmd/grainfs/cluster_join_test.go).
//
// Exercise runOfflineJoinHandshakeV2 directly with in-process
// MetaChallengeReceiver + MetaJoinReceiver, matching the pattern in
// meta_join_handshake_test.go. This isolates the handshake state machine
// (challenge → compute response → join) from KEK loading and QUIC.
package cluster

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/transport"
)

// offlineJoinInProcessSenders wires a MetaChallengeSender + MetaJoinSender
// whose dialers route bytes directly through the supplied receivers' Handle
// methods (no QUIC).
func offlineJoinInProcessSenders(
	chalRcv *MetaChallengeReceiver,
	joinRcv *MetaJoinReceiver,
) (*MetaChallengeSender, *MetaJoinSender) {
	chalSender := NewMetaChallengeSender(func(_ string, payload []byte) ([]byte, error) {
		out := chalRcv.Handle(&transport.Message{Type: transport.StreamMetaJoinChallenge, Payload: payload})
		return out.Payload, nil
	})
	joinSender := NewMetaJoinSender(func(_ string, payload []byte) ([]byte, error) {
		out := joinRcv.Handle(&transport.Message{Type: transport.StreamMetaJoin, Payload: payload})
		return out.Payload, nil
	})
	return chalSender, joinSender
}

func newTestKEKStore(t *testing.T, kek []byte) *encrypt.KEKStore {
	t.Helper()
	store := encrypt.NewKEKStore()
	if err := store.Add(0, kek); err != nil {
		t.Fatalf("seed KEKStore: %v", err)
	}
	return store
}

func TestPerformOfflineJoin_HappyPath(t *testing.T) {
	kek := make([]byte, encrypt.KEKSize)
	for i := range kek {
		kek[i] = byte(i)
	}
	clusterID := bytes.Repeat([]byte{0xAB}, 16)
	leaderStore := newTestKEKStore(t, kek)
	verifier := encrypt.NewHandshakeVerifier(leaderStore, clusterID)

	coord := &fakeJoinCoordinator{leader: true, leaderID: "node-1", fsm: NewMetaFSM()}
	chalRcv := NewMetaChallengeReceiver(verifier)
	joinRcv := NewMetaJoinReceiver(coord).WithHandshakeVerifier(verifier)

	chalSender, joinSender := offlineJoinInProcessSenders(chalRcv, joinRcv)

	joinerStore := newTestKEKStore(t, kek)

	var buf bytes.Buffer
	err := runOfflineJoinHandshakeV2(
		context.Background(),
		chalSender, joinSender,
		"peer.example:7001",
		"node-2", "10.0.0.2:7001",
		joinerStore, clusterID,
		&buf,
	)
	require.NoError(t, err, "happy-path join must exit 0")
	require.Equal(t, 1, coord.JoinCalls(), "AddVoter must run exactly once on success")
	require.Contains(t, buf.String(), "joined cluster successfully")
}

func TestPerformOfflineJoin_WrongKEK_NonZero(t *testing.T) {
	peerKEK := make([]byte, encrypt.KEKSize)
	for i := range peerKEK {
		peerKEK[i] = byte(i)
	}
	joinerKEK := make([]byte, encrypt.KEKSize)
	for i := range joinerKEK {
		joinerKEK[i] = byte(255 - i) // different KEK
	}
	clusterID := bytes.Repeat([]byte{0xAB}, 16)
	leaderStore := newTestKEKStore(t, peerKEK)
	verifier := encrypt.NewHandshakeVerifier(leaderStore, clusterID)

	coord := &fakeJoinCoordinator{leader: true, leaderID: "node-1", fsm: NewMetaFSM()}
	chalRcv := NewMetaChallengeReceiver(verifier)
	joinRcv := NewMetaJoinReceiver(coord).WithHandshakeVerifier(verifier)

	chalSender, joinSender := offlineJoinInProcessSenders(chalRcv, joinRcv)

	joinerStore := newTestKEKStore(t, joinerKEK)

	var buf bytes.Buffer
	err := runOfflineJoinHandshakeV2(
		context.Background(),
		chalSender, joinSender,
		"peer.example:7001",
		"node-2", "10.0.0.2:7001",
		joinerStore, clusterID,
		&buf,
	)
	require.Error(t, err, "wrong-KEK join must exit non-zero")
	msg := err.Error()
	require.True(t, strings.Contains(msg, "KEK mismatch"),
		"error must call out KEK mismatch; got %q", msg)
	require.True(t, strings.Contains(msg, "scp"),
		"error must include scp remediation hint; got %q", msg)
	require.Equal(t, 0, coord.JoinCalls(),
		"AddVoter must NOT be called on KEK mismatch")
}
