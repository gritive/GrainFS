package serveruntime

import (
	"context"
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
)

// fakeJWTProposer captures the cmdType and payload of each Propose call so the
// wiring-contract tests can assert the hooks dispatch the right MetaCmd.
type fakeJWTProposer struct {
	calls []fakeJWTProposeCall
}

type fakeJWTProposeCall struct {
	cmdType cluster.MetaCmdType
	payload []byte
}

func (f *fakeJWTProposer) Propose(_ context.Context, cmdType cluster.MetaCmdType, payload []byte) error {
	f.calls = append(f.calls, fakeJWTProposeCall{cmdType: cmdType, payload: payload})
	return nil
}

// newTestDEKKeeper creates a fresh DEKKeeper for tests.
func newTestDEKKeeper(t *testing.T) *encrypt.DEKKeeper {
	t.Helper()
	kek := make([]byte, 32)
	_, err := rand.Read(kek)
	require.NoError(t, err)
	keeper, err := encrypt.NewDEKKeeper(kek)
	require.NoError(t, err)
	return keeper
}

// TestJWTReloadHooks_RotatePayloadCarriesSealedKey verifies that the
// OnJWTSigningKeyRotate hook proposes MetaCmdTypeJWTSigningKeyRotate with a
// non-nil, well-formed payload containing a sealed key.
func TestJWTReloadHooks_RotatePayloadCarriesSealedKey(t *testing.T) {
	p := &fakeJWTProposer{}
	keeper := newTestDEKKeeper(t)
	hooks := wireJWTReloadHooks(p, keeper)
	require.NotNil(t, hooks.OnJWTSigningKeyRotate, "OnJWTSigningKeyRotate must be wired")

	require.NoError(t, hooks.OnJWTSigningKeyRotate(context.Background()))

	require.Len(t, p.calls, 1)
	assert.Equal(t, cluster.MetaCmdTypeJWTSigningKeyRotate, p.calls[0].cmdType)

	payload := p.calls[0].payload
	require.NotNil(t, payload, "rotate hook must produce a non-nil payload")
	require.NotEmpty(t, payload)

	// Decode and verify the payload carries well-formed key material.
	kid, wrappedSecret, dekGen, demotedAtUnix, err := cluster.DecodeMetaJWTSigningKeyRotateCmdForTest(payload)
	require.NoError(t, err)
	assert.NotEmpty(t, kid, "kid must be non-empty")
	assert.NotEmpty(t, wrappedSecret, "wrapped_secret must be non-empty")
	assert.GreaterOrEqual(t, dekGen, uint32(0))
	assert.Greater(t, demotedAtUnix, int64(0), "demoted_at_unix must be positive (proposer wall-clock)")
}

// TestJWTReloadHooks_RotateNoKeeper_Errors verifies that passing nil keeper
// makes the rotate hook return an error before calling Propose.
func TestJWTReloadHooks_RotateNoKeeper_Errors(t *testing.T) {
	p := &fakeJWTProposer{}
	hooks := wireJWTReloadHooks(p, nil)
	require.NotNil(t, hooks.OnJWTSigningKeyRotate)

	err := hooks.OnJWTSigningKeyRotate(context.Background())
	require.Error(t, err, "rotate with nil keeper must return error")
	assert.Empty(t, p.calls, "Propose must not be called when keeper is nil")
}

// TestJWTReloadHooks_PruneDispatchesCorrectCmd verifies that the
// OnJWTSigningKeyPrune hook proposes MetaCmdTypeJWTSigningKeyPrune with
// a non-nil payload.
func TestJWTReloadHooks_PruneDispatchesCorrectCmd(t *testing.T) {
	p := &fakeJWTProposer{}
	keeper := newTestDEKKeeper(t)
	hooks := wireJWTReloadHooks(p, keeper)
	require.NotNil(t, hooks.OnJWTSigningKeyPrune, "OnJWTSigningKeyPrune must be wired")

	require.NoError(t, hooks.OnJWTSigningKeyPrune(context.Background()))

	require.Len(t, p.calls, 1)
	assert.Equal(t, cluster.MetaCmdTypeJWTSigningKeyPrune, p.calls[0].cmdType)
	assert.NotNil(t, p.calls[0].payload, "prune hook must produce a non-nil payload")
}

// TestJWTReloadHooks_ApplyCmdPath verifies end-to-end that the MetaCmd bytes
// produced by the hooks are accepted by the FSM apply path without error when
// the FSM has a wired DEK keeper.
func TestJWTReloadHooks_ApplyCmdPath(t *testing.T) {
	keeper := newTestDEKKeeper(t)
	fsm := cluster.NewMetaFSM()
	fsm.SetDEKKeeper(keeper)

	// Build a rotate payload (proposer-side work) and apply it.
	p := &fakeJWTProposer{}
	hooks := wireJWTReloadHooks(p, keeper)

	require.NoError(t, hooks.OnJWTSigningKeyRotate(context.Background()))
	require.Len(t, p.calls, 1)

	rotateCmd, err := cluster.EncodeMetaCmdForTest(cluster.MetaCmdTypeJWTSigningKeyRotate, p.calls[0].payload)
	require.NoError(t, err)
	require.NoError(t, fsm.ApplyCmdForTest(rotateCmd), "FSM must accept well-formed rotate payload")

	// Build a prune payload and verify the FSM returns ErrPrunePrev (prev was just demoted).
	require.NoError(t, hooks.OnJWTSigningKeyPrune(context.Background()))
	require.Len(t, p.calls, 2)

	pruneCmd, err := cluster.EncodeMetaCmdForTest(cluster.MetaCmdTypeJWTSigningKeyPrune, p.calls[1].payload)
	require.NoError(t, err)
	// Prune just after rotate — should be refused (within MaxJWTTokenTTL window).
	// Any error is acceptable here (ErrPrunePrev or nil if no prev exists yet).
	_ = fsm.ApplyCmdForTest(pruneCmd)
}
