// §7 T56 — `grainfs cluster join` CLI tests.
//
// The CLI's outer Cobra layer (NewQUICTransport, peer dialing, flag wiring) is
// covered by integration tests elsewhere; here we exercise performClusterJoin
// directly with in-process MetaChallengeReceiver + MetaJoinReceiver, matching
// the pattern in internal/cluster/meta_join_handshake_test.go. This isolates
// the CLI flow (load KEK → challenge → compute response → join) from QUIC.
package main

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/transport"
)

// fakeJoinCoord satisfies cluster's unexported metaJoinCoordinator interface
// via Go's structural typing. Records AddVoter calls so the wrong-KEK test
// can assert it was NOT invoked.
type fakeJoinCoord struct {
	mu    sync.Mutex
	calls int
}

func (f *fakeJoinCoord) IsLeader() bool   { return true }
func (f *fakeJoinCoord) LeaderID() string { return "node-1" }
func (f *fakeJoinCoord) Join(ctx context.Context, id, addr string) error {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	return nil
}
func (f *fakeJoinCoord) Nodes() []cluster.MetaNodeEntry { return nil }
func (f *fakeJoinCoord) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// inProcessSenders wires a MetaChallengeSender + MetaJoinSender whose dialers
// route bytes directly through the supplied receivers' Handle methods.
func inProcessSenders(
	chalRcv *cluster.MetaChallengeReceiver,
	joinRcv *cluster.MetaJoinReceiver,
) (*cluster.MetaChallengeSender, *cluster.MetaJoinSender) {
	chalSender := cluster.NewMetaChallengeSender(func(_ string, payload []byte) ([]byte, error) {
		out := chalRcv.Handle(&transport.Message{Type: transport.StreamMetaJoinChallenge, Payload: payload})
		return out.Payload, nil
	})
	joinSender := cluster.NewMetaJoinSender(func(_ string, payload []byte) ([]byte, error) {
		out := joinRcv.Handle(&transport.Message{Type: transport.StreamMetaJoin, Payload: payload})
		return out.Payload, nil
	})
	return chalSender, joinSender
}

func TestCLI_ClusterJoin_HappyPath(t *testing.T) {
	kek := make([]byte, encrypt.KEKSize)
	for i := range kek {
		kek[i] = byte(i)
	}
	verifier := encrypt.NewHandshakeVerifier(kek)

	coord := &fakeJoinCoord{}
	chalRcv := cluster.NewMetaChallengeReceiver(verifier)
	joinRcv := cluster.NewMetaJoinReceiver(coord).WithHandshakeVerifier(verifier)

	chalSender, joinSender := inProcessSenders(chalRcv, joinRcv)

	var buf bytes.Buffer
	err := performClusterJoin(
		context.Background(),
		chalSender, joinSender,
		"peer.example:7001",
		"node-2", "10.0.0.2:7001",
		kek, // same KEK as peer
		&buf,
	)
	require.NoError(t, err, "happy-path join must exit 0")
	require.Equal(t, 1, coord.Calls(), "AddVoter must run exactly once on success")
	require.Contains(t, buf.String(), "joined cluster successfully")
}

func TestCLI_ClusterJoin_WrongKEK_NonZero(t *testing.T) {
	peerKEK := make([]byte, encrypt.KEKSize)
	for i := range peerKEK {
		peerKEK[i] = byte(i)
	}
	joinerKEK := make([]byte, encrypt.KEKSize)
	for i := range joinerKEK {
		joinerKEK[i] = byte(255 - i) // different KEK
	}
	verifier := encrypt.NewHandshakeVerifier(peerKEK)

	coord := &fakeJoinCoord{}
	chalRcv := cluster.NewMetaChallengeReceiver(verifier)
	joinRcv := cluster.NewMetaJoinReceiver(coord).WithHandshakeVerifier(verifier)

	chalSender, joinSender := inProcessSenders(chalRcv, joinRcv)

	var buf bytes.Buffer
	err := performClusterJoin(
		context.Background(),
		chalSender, joinSender,
		"peer.example:7001",
		"node-2", "10.0.0.2:7001",
		joinerKEK, // mismatched KEK
		&buf,
	)
	require.Error(t, err, "wrong-KEK join must exit non-zero")
	msg := err.Error()
	require.True(t, strings.Contains(msg, "KEK mismatch"),
		"error must call out KEK mismatch; got %q", msg)
	require.True(t, strings.Contains(msg, "scp"),
		"error must include scp remediation hint; got %q", msg)
	require.Equal(t, 0, coord.Calls(),
		"AddVoter must NOT be called on KEK mismatch")
}
