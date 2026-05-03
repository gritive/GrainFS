package raft

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newMuxForLookupTests builds a GroupRaftQUICMux without any QUIC listener.
// All we exercise here is the in-memory dispatch table: lookupNode + the
// metaNode pointer + Register's validation. Real network paths are covered
// by setupMuxTransportPair tests in the sibling file.
func newMuxForLookupTests(t *testing.T) *GroupRaftQUICMux {
	t.Helper()
	tr := transport.MustNewQUICTransport("test-meta-lookup")
	t.Cleanup(func() { _ = tr.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, tr.Listen(ctx, "127.0.0.1:0"))
	return NewGroupRaftQUICMux(tr)
}

// TestGroupMux_LookupNode_MetaUnregistered: codex P0 #1 + A2 race coverage.
// Before RegisterMetaNode is called, a __meta__ lookup must return the
// mux-unknown sentinel error so senders can distinguish it from a permanent
// failure and fall back to the legacy StreamMetaRaft path.
func TestGroupMux_LookupNode_MetaUnregistered(t *testing.T) {
	m := newMuxForLookupTests(t)

	node, err := m.lookupNode(metaGroupID)
	require.Error(t, err)
	require.Nil(t, node)
	assert.Contains(t, err.Error(), "mux: unknown group")
	assert.Contains(t, err.Error(), metaGroupID)
}

// TestGroupMux_LookupNode_MetaRegistered: after RegisterMetaNode, lookup
// returns the registered node. We use a sentinel pointer (not a real Node)
// because lookupNode doesn't call any methods on the pointer; full handler
// dispatch is exercised via e2e.
func TestGroupMux_LookupNode_MetaRegistered(t *testing.T) {
	m := newMuxForLookupTests(t)
	want := &Node{}
	m.RegisterMetaNode(want)

	got, err := m.lookupNode(metaGroupID)
	require.NoError(t, err)
	assert.Same(t, want, got)
}

// TestGroupMux_LookupNode_Regular: an ordinary groupID continues to hit
// nodes sync.Map; the meta branch must not steal regular traffic.
func TestGroupMux_LookupNode_Regular(t *testing.T) {
	m := newMuxForLookupTests(t)
	want := &Node{}
	m.Register("group-0", want)

	got, err := m.lookupNode("group-0")
	require.NoError(t, err)
	assert.Same(t, want, got)

	// Unregistered regular group still reports unknown.
	_, err = m.lookupNode("group-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mux: unknown group")
}

// TestGroupMux_RegisterMetaNode_Idempotent: last-writer-wins. RegisterMetaNode
// is called from NewMetaRaftQUICTransportMux at construction time, but tests
// or hot-reload scenarios may call it again — the latest pointer must win,
// without locking, without any "already set" error.
func TestGroupMux_RegisterMetaNode_Idempotent(t *testing.T) {
	m := newMuxForLookupTests(t)
	first := &Node{}
	second := &Node{}

	m.RegisterMetaNode(first)
	got, err := m.lookupNode(metaGroupID)
	require.NoError(t, err)
	assert.Same(t, first, got)

	m.RegisterMetaNode(second)
	got, err = m.lookupNode(metaGroupID)
	require.NoError(t, err)
	assert.Same(t, second, got)
}

// TestGroupMux_RegisterMetaNode_NilDeregisters: passing nil clears the
// pointer (used by tests during teardown). Subsequent lookups should hit
// the unknown-group path again.
func TestGroupMux_RegisterMetaNode_NilDeregisters(t *testing.T) {
	m := newMuxForLookupTests(t)
	m.RegisterMetaNode(&Node{})
	_, err := m.lookupNode(metaGroupID)
	require.NoError(t, err)

	m.RegisterMetaNode(nil)
	_, err = m.lookupNode(metaGroupID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mux: unknown group")
}

// TestGroupMux_Register_PanicsOnReserved: codex P0 #2 — Register is called
// from serve.go after FSM apply has already validated. If a reserved ID
// reaches Register, that's a programming bug — fail fast with a panic.
// Validation upstream ensures users never see this.
func TestGroupMux_Register_PanicsOnReserved(t *testing.T) {
	m := newMuxForLookupTests(t)
	cases := []string{"", "__meta__", "__sys", "__internal"}
	for _, id := range cases {
		t.Run("rejects "+id, func(t *testing.T) {
			require.Panics(t, func() {
				m.Register(id, &Node{})
			})
		})
	}
}

// TestIsMuxFallbackErr verifies the fallback decision matrix that drives
// MetaRaftQUICTransport's mux-then-legacy retry. Whitelist semantics:
// fallback ONLY for ctx-deadline / unknown-group / mux infra failure.
// Backpressure (handler overload), protocol mismatch (unknown op,
// unsupported rpc), and decode/payload errors must propagate so legacy
// retry doesn't amplify the load or mask the real problem.
func TestIsMuxFallbackErr(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil = no fallback", nil, false},
		// Whitelisted (fallback-worthy)
		{"ctx deadline", context.DeadlineExceeded, true},
		{"wrapped ctx", strErr("muxConnFor: context deadline exceeded"), true},
		{"unknown group remote", strErr("remote: mux: unknown group __meta__"), true},
		{"raft_conn closed", strErr("raft_conn: connection closed"), true},
		{"frame too big", strErr("raft_conn: frame exceeds MaxFrameSize"), true},
		{"no mux handler", strErr("dial: no mux handler registered"), true},
		{"dial mux", strErr("dial mux to peer: connection refused"), true},
		{"open mux streams", strErr("open mux streams to peer: tls handshake"), true},
		{"GetOrConnectMux", strErr("GetOrConnectMux: peer rejected"), true},
		// Propagated (not fallback)
		{"handler overload", strErr("raft_conn: handler pool overloaded"), false},
		{"unknown op", strErr("raft_conn: unknown op code"), false},
		{"remote handler panic", strErr("remote: handler panic: nil pointer"), false},
		{"remote unsupported rpc", strErr("remote: mux: unsupported rpc Foo"), false},
		{"decode rpc", strErr("remote: mux: decode rpc: schema mismatch"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := isMuxFallbackErr(tc.err)
			assert.Equal(t, tc.want, got)
		})
	}
}

type strErr string

func (s strErr) Error() string { return string(s) }

// TestMetaTransport_NewMux_AutoRegisters: the mux-aware constructor must
// auto-register the meta node on the supplied groupMux. This closes the
// codex P1 #3 startup race — by the time the constructor returns, the
// receiver-side __meta__ branch is wired up.
func TestMetaTransport_NewMux_AutoRegisters(t *testing.T) {
	tr := transport.MustNewQUICTransport("test-meta-autoreg")
	defer tr.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, tr.Listen(ctx, "127.0.0.1:0"))

	mux := NewGroupRaftQUICMux(tr)
	// Pre-condition: nothing registered.
	_, err := mux.lookupNode(metaGroupID)
	require.Error(t, err)

	node := &Node{}
	_ = NewMetaRaftQUICTransportMux(tr, node, mux)

	got, err := mux.lookupNode(metaGroupID)
	require.NoError(t, err)
	assert.Same(t, node, got)
}

// TestMetaTransport_NewMux_NilGroupMux: passing nil groupMux must not
// panic and must leave behavior identical to the legacy constructor.
func TestMetaTransport_NewMux_NilGroupMux(t *testing.T) {
	tr := transport.MustNewQUICTransport("test-meta-nil")
	defer tr.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, tr.Listen(ctx, "127.0.0.1:0"))

	require.NotPanics(t, func() {
		mt := NewMetaRaftQUICTransportMux(tr, &Node{}, nil)
		require.NotNil(t, mt)
		assert.False(t, mt.muxOn())
	})
}

// helper: ensure the magic ID literal hasn't drifted from the doc.
func TestMetaGroupIDStability(t *testing.T) {
	// The wire is shipped — changing this constant breaks rolling upgrades.
	assert.Equal(t, "__meta__", metaGroupID)
	// "__" prefix stays reserved so future internal IDs don't collide.
	assert.True(t, strings.HasPrefix(metaGroupID, "__"))
}
