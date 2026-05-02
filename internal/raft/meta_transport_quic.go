package raft

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/gritive/GrainFS/internal/transport"
)

const (
	// Meta-Raft elections run alongside data Raft, shard RPC, and S3 traffic in
	// multi-process tests. 80ms was too tight under local/CI CPU contention and
	// could leave the control plane without a leader even after data Raft elected.
	metaRaftRPCTimeout      = 500 * time.Millisecond
	metaRaftSnapshotTimeout = 60 * time.Second

	// metaMuxAttemptTimeout caps the mux-path attempt so a half-open mux call
	// cannot consume the whole legacy budget. Meta election is 750ms; we want
	// the mux attempt + a fresh-ctx legacy fallback to both fit. 200ms leaves
	// the 500ms metaRaftRPCTimeout budget intact for fallback.
	metaMuxAttemptTimeout = 200 * time.Millisecond
)

// RPC type constants for meta-Raft QUIC transport (legacy path).
//
// On the mux path the receiver's handleMuxRequest switches on the unified
// rpcType* (RequestVote / AppendEntries) constants because meta-raft *Node
// has the same Handle* methods as a per-group Node. The metaRPC* constants
// stay reserved for the legacy StreamMetaRaft envelope so cross-version
// peers keep talking to each other.
const (
	metaRPCRequestVote          = "MetaRequestVote"
	metaRPCRequestVoteReply     = "MetaRequestVoteReply"
	metaRPCAppendEntries        = "MetaAppendEntries"
	metaRPCAppendEntriesReply   = "MetaAppendEntriesReply"
	metaRPCInstallSnapshot      = "MetaInstallSnapshot"
	metaRPCInstallSnapshotReply = "MetaInstallSnapshotReply"
)

// MetaRaftQUICTransport delivers meta-Raft RPCs over the shared QUIC transport.
//
// Two paths coexist:
//
//   - Legacy: tr.Call(StreamMetaRaft, ...) per message. Used when groupMux
//     is nil, when groupMux.MuxEnabled() is false, or as fallback when the
//     mux attempt fails (dial / send / "unknown group" from older peer).
//   - Mux: shared with per-group raft via groupMux. Sender prefixes payload
//     with the magic groupID metaGroupID; receiver routes through
//     handleMuxRequest / dispatchToLocalGroup to metaNode.
//
// SendInstallSnapshot stays on the legacy path always — the 60s budget +
// large-payload semantics don't fit the per-message frame model.
type MetaRaftQUICTransport struct {
	tr       *transport.QUICTransport
	node     *Node
	groupMux *GroupRaftQUICMux // nil = legacy-only; set by NewMetaRaftQUICTransportMux.
}

// NewMetaRaftQUICTransport creates a meta-Raft RPC transport without mux
// support. Tests and legacy callers use this constructor.
func NewMetaRaftQUICTransport(tr *transport.QUICTransport, node *Node) *MetaRaftQUICTransport {
	m := &MetaRaftQUICTransport{tr: tr, node: node}
	tr.Handle(transport.StreamMetaRaft, m.handleRPC)
	return m
}

// NewMetaRaftQUICTransportMux is the mux-aware constructor. It auto-registers
// node on groupMux so the receiver-side __meta__ branch is wired up before
// EnableMux installs the mux accept handler. Auto-registration removes a
// startup-order race: if EnableMux ran before metaNode was registered, all
// inbound meta calls would hit "mux: unknown group __meta__".
//
// groupMux may be nil; in that case behaviour matches the legacy
// constructor.
func NewMetaRaftQUICTransportMux(tr *transport.QUICTransport, node *Node, groupMux *GroupRaftQUICMux) *MetaRaftQUICTransport {
	m := &MetaRaftQUICTransport{tr: tr, node: node, groupMux: groupMux}
	tr.Handle(transport.StreamMetaRaft, m.handleRPC) // legacy receiver always on for fallback
	if groupMux != nil {
		groupMux.RegisterMetaNode(node)
	}
	return m
}

func (m *MetaRaftQUICTransport) muxOn() bool {
	return m.groupMux != nil && m.groupMux.MuxEnabled()
}

// isMuxFallbackErr decides whether a mux-path error should fall back to
// legacy StreamMetaRaft. We treat:
//   - dial / send / RaftConn.Call errors (mux infrastructure broken or peer
//     not speaking mux ALPN) as fallback-worthy
//   - remote "mux: unknown group __meta__" (peer mux-enabled but on a binary
//     that doesn't yet RegisterMetaNode — codex P1 #6, mixed-version cluster)
//   - mux-attempt ctx deadline exceeded (budget exhausted; legacy gets fresh
//     ctx, codex P1 #4)
//
// All other errors propagate. Genuine RPC failures (peer says term mismatch
// etc.) are encoded in the reply, not as transport errors, so they don't
// reach this function.
func isMuxFallbackErr(err error) bool {
	if err == nil {
		return false
	}
	if errIsCtxBudget(err) {
		return true
	}
	s := err.Error()
	if strings.Contains(s, "mux: unknown group") {
		return true
	}
	// muxConnFor / RaftConn.Call wrap with their own prefixes; treat any
	// non-nil mux error as fallback to maximize meta availability.
	return true
}

func errIsCtxBudget(err error) bool {
	return err == context.DeadlineExceeded || (err != nil && strings.Contains(err.Error(), "context deadline exceeded"))
}

func (m *MetaRaftQUICTransport) SendRequestVote(peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	if m.muxOn() {
		muxCtx, cancel := context.WithTimeout(context.Background(), metaMuxAttemptTimeout)
		reply, err := m.muxRequestVote(muxCtx, peer, args)
		cancel()
		if err == nil {
			return reply, nil
		}
		if !isMuxFallbackErr(err) {
			return nil, err
		}
		// fall through to legacy with fresh ctx
	}

	ctx, cancel := context.WithTimeout(context.Background(), metaRaftRPCTimeout)
	defer cancel()
	env, err := encodeRPC(metaRPCRequestVote, args)
	if err != nil {
		return nil, err
	}
	resp, err := m.tr.Call(ctx, peer, &transport.Message{Type: transport.StreamMetaRaft, Payload: env})
	if err != nil {
		return nil, fmt.Errorf("meta RequestVote to %s: %w", peer, err)
	}
	rpcType, data, err := decodeRPC(resp.Payload)
	if err != nil {
		return nil, err
	}
	if rpcType != metaRPCRequestVoteReply {
		return nil, fmt.Errorf("meta RequestVote: unexpected reply type %s", rpcType)
	}
	return decodeRequestVoteReply(data)
}

func (m *MetaRaftQUICTransport) muxRequestVote(ctx context.Context, peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	ps, err := m.groupMux.muxConnFor(ctx, peer)
	if err != nil {
		return nil, err
	}
	env, err := encodeRPC(rpcTypeRequestVote, args)
	if err != nil {
		return nil, err
	}
	respBytes, err := ps.rc.Call(ctx, prefixGroupID(metaGroupID, env))
	if err != nil {
		return nil, err
	}
	_, data, err := decodeRPC(respBytes)
	if err != nil {
		return nil, fmt.Errorf("meta mux RequestVote reply: %w", err)
	}
	return decodeRequestVoteReply(data)
}

func (m *MetaRaftQUICTransport) SendAppendEntries(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	if m.muxOn() {
		muxCtx, cancel := context.WithTimeout(context.Background(), metaMuxAttemptTimeout)
		reply, err := m.muxAppendEntries(muxCtx, peer, args)
		cancel()
		if err == nil {
			return reply, nil
		}
		if !isMuxFallbackErr(err) {
			return nil, err
		}
		// fall through to legacy
	}

	ctx, cancel := context.WithTimeout(context.Background(), metaRaftRPCTimeout)
	defer cancel()
	env, err := encodeRPC(metaRPCAppendEntries, args)
	if err != nil {
		return nil, err
	}
	resp, err := m.tr.Call(ctx, peer, &transport.Message{Type: transport.StreamMetaRaft, Payload: env})
	if err != nil {
		return nil, fmt.Errorf("meta AppendEntries to %s: %w", peer, err)
	}
	rpcType, data, err := decodeRPC(resp.Payload)
	if err != nil {
		return nil, err
	}
	if rpcType != metaRPCAppendEntriesReply {
		return nil, fmt.Errorf("meta AppendEntries: unexpected reply type %s", rpcType)
	}
	return decodeAppendEntriesReply(data)
}

func (m *MetaRaftQUICTransport) muxAppendEntries(ctx context.Context, peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	ps, err := m.groupMux.muxConnFor(ctx, peer)
	if err != nil {
		return nil, err
	}
	// Entries-empty heartbeats ride the shared coalescer; entries-bearing
	// AE goes direct via RaftConn.Call. Same pattern as GroupRaftSender.
	if len(args.Entries) == 0 {
		reply, hcErr := ps.hc.AppendEntries(ctx, metaGroupID, args)
		if hcErr == nil {
			return reply, nil
		}
		// Fall through to direct mux call on coalescer error (rare).
	}
	env, err := encodeRPC(rpcTypeAppendEntries, args)
	if err != nil {
		return nil, err
	}
	respBytes, err := ps.rc.Call(ctx, prefixGroupID(metaGroupID, env))
	if err != nil {
		return nil, err
	}
	_, data, err := decodeRPC(respBytes)
	if err != nil {
		return nil, fmt.Errorf("meta mux AppendEntries reply: %w", err)
	}
	return decodeAppendEntriesReply(data)
}

func (m *MetaRaftQUICTransport) SendInstallSnapshot(peer string, args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	// Snapshot install bypasses mux unconditionally — large payloads + 60s
	// timeout don't fit the per-frame mux model, and mixing them in would
	// break the R+H invariant that the mux only carries small frames.
	ctx, cancel := context.WithTimeout(context.Background(), metaRaftSnapshotTimeout)
	defer cancel()
	env, err := encodeRPC(metaRPCInstallSnapshot, args)
	if err != nil {
		return nil, err
	}
	resp, err := m.tr.Call(ctx, peer, &transport.Message{Type: transport.StreamMetaRaft, Payload: env})
	if err != nil {
		return nil, fmt.Errorf("meta InstallSnapshot to %s: %w", peer, err)
	}
	rpcType, data, err := decodeRPC(resp.Payload)
	if err != nil {
		return nil, err
	}
	if rpcType != metaRPCInstallSnapshotReply {
		return nil, fmt.Errorf("meta InstallSnapshot: unexpected reply type %s", rpcType)
	}
	return decodeInstallSnapshotReply(data)
}

func (m *MetaRaftQUICTransport) handleRPC(req *transport.Message) *transport.Message {
	rpcType, data, err := decodeRPC(req.Payload)
	if err != nil {
		return nil
	}
	var replyEnv []byte
	switch rpcType {
	case metaRPCRequestVote:
		args, err := decodeRequestVoteArgs(data)
		if err != nil {
			return nil
		}
		reply := m.node.HandleRequestVote(args)
		replyEnv, _ = encodeRPC(metaRPCRequestVoteReply, reply)
	case metaRPCAppendEntries:
		args, err := decodeAppendEntriesArgs(data)
		if err != nil {
			return nil
		}
		reply := m.node.HandleAppendEntries(args)
		replyEnv, _ = encodeRPC(metaRPCAppendEntriesReply, reply)
	case metaRPCInstallSnapshot:
		args, err := decodeInstallSnapshotArgs(data)
		if err != nil {
			return nil
		}
		reply := m.node.HandleInstallSnapshot(args)
		replyEnv, _ = encodeRPC(metaRPCInstallSnapshotReply, reply)
	default:
		return nil
	}
	return &transport.Message{Type: transport.StreamMetaRaft, Payload: replyEnv}
}
