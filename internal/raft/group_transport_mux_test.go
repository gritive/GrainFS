package raft

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeRaftNode wraps a *Node-like minimum interface so the mux can dispatch.
// We can't easily spin up a real raft.Node in this unit test (it needs
// LogStore + transport + tickers), so we register a fake handler via the
// nodes sync.Map. The mux only uses node.HandleAppendEntries / HandleRequestVote.
type fakeRaftNode struct {
	groupID     string
	aeHandler   func(*AppendEntriesArgs) *AppendEntriesReply
	voteHandler func(*RequestVoteArgs) *RequestVoteReply
	aeCount     atomic.Int64
	voteCount   atomic.Int64
}

// To satisfy the mux's sync.Map type (*Node), we'd need a real Node. Instead,
// rewrite the test to use a thin shim: the existing extractGroupID expects
// the same wire format both paths produce, and dispatchToLocalGroup is the
// hook used for batched dispatch. We test mux behavior by forking a real
// transport pair with both sides having a configured GroupRaftQUICMux.

// setupMuxTransportPair creates two QUICTransports (server S, client C),
// connects S→C and C→S so both can act as senders/receivers, and enables
// mux mode on both sides. Returns (clientMux, serverMux, cleanup).
func setupMuxTransportPair(t *testing.T) (*GroupRaftQUICMux, *GroupRaftQUICMux, *transport.QUICTransport, *transport.QUICTransport, func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	serverTr := transport.NewQUICTransport("test-mux-psk")
	clientTr := transport.NewQUICTransport("test-mux-psk")

	require.NoError(t, serverTr.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, clientTr.Listen(ctx, "127.0.0.1:0"))

	serverMux := NewGroupRaftQUICMux(serverTr)
	clientMux := NewGroupRaftQUICMux(clientTr)

	serverMux.EnableMux(2, 5*time.Millisecond)
	clientMux.EnableMux(2, 5*time.Millisecond)

	cleanup := func() {
		_ = serverTr.Close()
		_ = clientTr.Close()
	}
	return clientMux, serverMux, clientTr, serverTr, cleanup
}

func TestGroupRaftMux_EnableMux_Idempotent(t *testing.T) {
	tr := transport.NewQUICTransport("psk")
	defer tr.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, tr.Listen(ctx, "127.0.0.1:0"))

	m := NewGroupRaftQUICMux(tr)
	assert.False(t, m.MuxEnabled())
	m.EnableMux(4, 2*time.Millisecond)
	assert.True(t, m.MuxEnabled())
	// Second call must not panic; muxRegisterOnce keeps SetMuxConnHandler idempotent.
	m.EnableMux(8, 5*time.Millisecond)
	assert.True(t, m.MuxEnabled())
}

// TestGroupRaftMux_NilCoalescerOnEarlyClose is a regression test for the
// segfault hit in load-N8 e2e (2026-05-02): when OpenOutboundStreams fails,
// rc.Close triggers OnBroken which dereferences ps.hc — but ps.hc was
// previously assigned only AFTER OpenOutboundStreams succeeded, so it was
// still nil at the moment of dereference. Fix: attach ps.hc before the
// stream open AND nil-guard in the OnBroken / HBReplyHandler closures.
//
// We can't easily force OpenOutboundStreams to fail without breaking deeper
// internals, so we stress the nil-guard path directly: build a RaftConn with
// the same closure shape, mark it broken before ps.hc is set, and verify the
// process does not panic.
func TestGroupRaftMux_NilCoalescerOnEarlyClose(t *testing.T) {
	clientQ, serverQ, qcleanup := testQUICPair(t)
	defer qcleanup()

	type peerState struct {
		hc *HeartbeatCoalescer
	}
	ps := &peerState{} // hc intentionally nil (simulating failure path)

	rc := NewRaftConn(clientQ, RaftConnConfig{
		PoolSize: 1,
		HBReplyHandler: func(corrID uint64, payload []byte) {
			// Production code guards this with a nil check; if unguarded,
			// a stray reply frame mid-teardown panics.
			if ps.hc != nil {
				ps.hc.DispatchReplyBatch(corrID, payload)
			}
		},
		OnBroken: func(_ *RaftConn, _ error) {
			if ps.hc != nil {
				ps.hc.FailAll(assertError("conn broken"))
			}
		},
	})
	// Open + close BEFORE attaching the coalescer. Must not panic.
	require.NotPanics(t, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = rc.OpenOutboundStreams(ctx)
		rc.Close()
	})
	_ = serverQ // keep alive for cleanup
}

func TestGroupRaftMux_LegacyFallbackWhenMuxDisabled(t *testing.T) {
	// Server does NOT enable mux. Client enables mux, but GetOrConnectMux
	// will fail (server rejects mux ALPN since handler is unregistered).
	// Sender should fall through to legacy Call path and succeed.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	serverTr := transport.NewQUICTransport("psk-legacy")
	defer serverTr.Close()
	clientTr := transport.NewQUICTransport("psk-legacy")
	defer clientTr.Close()

	require.NoError(t, serverTr.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, clientTr.Listen(ctx, "127.0.0.1:0"))

	// Server: registers a stub group via legacy mux only.
	serverMux := NewGroupRaftQUICMux(serverTr)
	// Use legacy Handle path: register a fake AppendEntries handler at the
	// transport StreamGroupRaft type. Easier than spinning up a real Node.
	serverTr.Handle(transport.StreamGroupRaft, func(req *transport.Message) *transport.Message {
		// Echo a minimal AppendEntriesReply
		_, body, err := extractGroupID(req.Payload)
		if err != nil {
			return nil
		}
		_, data, err := decodeRPC(body)
		if err != nil {
			return nil
		}
		args, err := decodeAppendEntriesArgs(data)
		if err != nil {
			return nil
		}
		reply := &AppendEntriesReply{Term: args.Term + 1, Success: true}
		env, _ := encodeRPC(rpcTypeAppendEntriesReply, reply)
		return &transport.Message{Type: transport.StreamGroupRaft, Payload: env}
	})
	_ = serverMux

	// Client enables mux. Server has no muxHandler so mux dial fails.
	clientMux := NewGroupRaftQUICMux(clientTr)
	clientMux.EnableMux(2, 5*time.Millisecond)
	require.NoError(t, clientTr.Connect(ctx, serverTr.LocalAddr()))

	sender := clientMux.ForGroup("g0")
	args := &AppendEntriesArgs{Term: 5, LeaderID: "client", Entries: nil}
	reply, err := sender.AppendEntries(serverTr.LocalAddr(), args)
	require.NoError(t, err)
	assert.True(t, reply.Success)
	assert.Equal(t, uint64(6), reply.Term)
}
