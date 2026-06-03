// raftv2_meta.go — v2 meta-raft RPC bridge.
//
// Mirrors internal/cluster/raft_rpc.go (per-group v2 bridge from PR 27)
// but registers transport.StreamMetaRaft instead of transport.StreamControl.
// v1's internal/raft/meta_transport.go is the reference; this file is
// the v2 equivalent. PR 30b deletes v1 and renames this file.
//
// Wire codec is shared with raft_rpc.go (raftv2_codec.go). The same
// RPC type strings ("RequestVote", "AppendEntries", "InstallSnapshot") flow
// over StreamMetaRaft here — that means this bridge is byte-identical to the
// per-group v2 bridge at the codec layer, not byte-identical to v1's
// meta-stream wire (which used "Meta*" prefixed type names). Mixed-binary
// rolling upgrade is therefore not supported; M6.3 clean-restart is the
// production rollout path (see plan §Out of scope).
//
// Both meta-Raft (this file) and per-group raft (raft_quic_rpc.go) register
// independent handlers on different StreamType values, so coexistence at
// runtime is safe.

package cluster

import (
	"context"
	"fmt"
	"time"

	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/transport"
)

const (
	// v2MetaRPCTimeout mirrors v1's metaRaftRPCTimeout. Meta-Raft elections
	// run alongside data Raft, shard RPC, and S3 traffic in multi-process
	// tests; 80ms was too tight under local/CI CPU contention.
	v2MetaRPCTimeout = 500 * time.Millisecond
	// v2MetaSnapshotTimeout mirrors v1's metaRaftSnapshotTimeout — 60s
	// budget for large InstallSnapshot payloads.
	v2MetaSnapshotTimeout = 60 * time.Second
	// v2MetaMuxAttemptTimeout caps the mux-path attempt so a half-open mux call
	// cannot consume the whole legacy budget. Mirrors v1's metaMuxAttemptTimeout:
	// 200ms leaves the 500ms v2MetaRPCTimeout intact for the Call fallback.
	v2MetaMuxAttemptTimeout = 200 * time.Millisecond
)

// metaMuxSender is the subset of *raft.GroupRaftMux the meta transport uses to
// ride the shared persistent carrier instead of connection-per-RPC Call. nil
// disables the mux fast path (Call-only, the pre-fix behavior).
type metaMuxSender interface {
	MuxEnabled() bool
	SendMetaAppendEntries(ctx context.Context, peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error)
	SendMetaRequestVote(ctx context.Context, peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error)
}

// RaftV2MetaTransport bridges meta-Raft RPCs over the cluster transport for raft v2. It
// registers an inbound handler on transport.StreamMetaRaft and exposes the
// three outbound Send* methods that satisfy cluster.MetaTransport.
type RaftV2MetaTransport struct {
	transport clusterRPCTransport
	node      RaftNode
	// mux, when non-nil and enabled, carries AppendEntries/RequestVote over the
	// shared persistent mux carrier (one TLS handshake per peer, reused) instead
	// of transport.Call (a fresh handshake per RPC). nil = Call-only.
	mux metaMuxSender
}

// compile-time check: RaftV2MetaTransport must satisfy MetaTransport.
var _ MetaTransport = (*RaftV2MetaTransport)(nil)

// NewRaftV2MetaTransport wires the inbound StreamMetaRaft handler.
func NewRaftV2MetaTransport(tr clusterRPCTransport, node RaftNode) *RaftV2MetaTransport {
	mt := &RaftV2MetaTransport{transport: tr, node: node}
	tr.Handle(transport.StreamMetaRaft, mt.handleRPC)
	return mt
}

// SendRequestVote mirrors v1's MetaRaftTransport.SendRequestVote — the
// wire envelope uses the shared v2 codec.
func (m *RaftV2MetaTransport) SendRequestVote(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	if m.mux != nil && m.mux.MuxEnabled() {
		muxCtx, cancel := context.WithTimeout(context.Background(), v2MetaMuxAttemptTimeout)
		reply, err := m.mux.SendMetaRequestVote(muxCtx, peer, args)
		cancel()
		if err == nil {
			return reply, nil
		}
		if !raft.IsMuxFallbackErr(err) {
			return nil, fmt.Errorf("meta RequestVote to %s: %w", peer, err)
		}
		// fall through to legacy Call with a fresh ctx
	}

	ctx, cancel := context.WithTimeout(context.Background(), v2MetaRPCTimeout)
	defer cancel()

	envelope, err := v2EncodeRPC(v2RPCTypeRequestVote, args)
	if err != nil {
		return nil, err
	}
	resp, err := m.transport.Call(ctx, peer, &transport.Message{Type: transport.StreamMetaRaft, Payload: envelope})
	if err != nil {
		return nil, fmt.Errorf("meta RequestVote to %s: %w", peer, err)
	}
	rpcType, data, err := v2DecodeRPC(resp.Payload)
	if err != nil {
		return nil, err
	}
	if rpcType != v2RPCTypeRequestVoteReply {
		return nil, fmt.Errorf("meta RequestVote: unexpected reply type %s", rpcType)
	}
	return v2DecodeRequestVoteReply(data)
}

// SendAppendEntries mirrors v1's MetaRaftTransport.SendAppendEntries.
func (m *RaftV2MetaTransport) SendAppendEntries(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	if m.mux != nil && m.mux.MuxEnabled() {
		muxCtx, cancel := context.WithTimeout(context.Background(), v2MetaMuxAttemptTimeout)
		reply, err := m.mux.SendMetaAppendEntries(muxCtx, peer, args)
		cancel()
		if err == nil {
			return reply, nil
		}
		if !raft.IsMuxFallbackErr(err) {
			return nil, fmt.Errorf("meta AppendEntries to %s: %w", peer, err)
		}
		// fall through to legacy Call with a fresh ctx
	}

	ctx, cancel := context.WithTimeout(context.Background(), v2MetaRPCTimeout)
	defer cancel()

	envelope, err := v2EncodeRPC(v2RPCTypeAppendEntries, args)
	if err != nil {
		return nil, err
	}
	resp, err := m.transport.Call(ctx, peer, &transport.Message{Type: transport.StreamMetaRaft, Payload: envelope})
	if err != nil {
		return nil, fmt.Errorf("meta AppendEntries to %s: %w", peer, err)
	}
	rpcType, data, err := v2DecodeRPC(resp.Payload)
	if err != nil {
		return nil, err
	}
	if rpcType != v2RPCTypeAppendEntriesReply {
		return nil, fmt.Errorf("meta AppendEntries: unexpected reply type %s", rpcType)
	}
	return v2DecodeAppendEntriesReply(data)
}

// SendTimeoutNow sends a TimeoutNow RPC to the transfer target, triggering an
// immediate election. Called by the raft node during TransferLeadership.
func (m *RaftV2MetaTransport) SendTimeoutNow(peer string, args *raft.TimeoutNowArgs) (*raft.TimeoutNowReply, error) {
	ctx, cancel := context.WithTimeout(context.Background(), v2MetaRPCTimeout)
	defer cancel()

	envelope, err := v2EncodeRPC(v2RPCTypeTimeoutNow, args)
	if err != nil {
		return nil, err
	}
	resp, err := m.transport.Call(ctx, peer, &transport.Message{Type: transport.StreamMetaRaft, Payload: envelope})
	if err != nil {
		return nil, fmt.Errorf("meta TimeoutNow to %s: %w", peer, err)
	}
	rpcType, _, err := v2DecodeRPC(resp.Payload)
	if err != nil {
		return nil, err
	}
	if rpcType != v2RPCTypeTimeoutNowReply {
		return nil, fmt.Errorf("meta TimeoutNow: unexpected reply type %s", rpcType)
	}
	return &raft.TimeoutNowReply{}, nil
}

// SendInstallSnapshot mirrors v1's MetaRaftTransport.SendInstallSnapshot.
// The 60s timeout accommodates large snapshot payloads.
func (m *RaftV2MetaTransport) SendInstallSnapshot(peer string, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	ctx, cancel := context.WithTimeout(context.Background(), v2MetaSnapshotTimeout)
	defer cancel()

	envelope, err := v2EncodeRPC(v2RPCTypeInstallSnapshot, args)
	if err != nil {
		return nil, err
	}
	resp, err := m.transport.Call(ctx, peer, &transport.Message{Type: transport.StreamMetaRaft, Payload: envelope})
	if err != nil {
		return nil, fmt.Errorf("meta InstallSnapshot to %s: %w", peer, err)
	}
	rpcType, data, err := v2DecodeRPC(resp.Payload)
	if err != nil {
		return nil, err
	}
	if rpcType != v2RPCTypeInstallSnapshotReply {
		return nil, fmt.Errorf("meta InstallSnapshot: unexpected reply type %s", rpcType)
	}
	return v2DecodeInstallSnapshotReply(data)
}

// handleRPC decodes an inbound StreamMetaRaft message and dispatches via the
// RaftNode interface (the v2 adapter translates v1 wire types ↔ v2 native).
// Mirrors raft_quic_rpc.go::handleRPC for the per-group bridge.
func (m *RaftV2MetaTransport) handleRPC(req *transport.Message) *transport.Message {
	rpcType, data, err := v2DecodeRPC(req.Payload)
	if err != nil {
		return nil
	}

	var replyEnvelope []byte

	switch rpcType {
	case v2RPCTypeRequestVote:
		args, err := v2DecodeRequestVoteArgs(data)
		if err != nil {
			return nil
		}
		reply := m.node.HandleRequestVote(args)
		replyEnvelope, _ = v2EncodeRPC(v2RPCTypeRequestVoteReply, reply)

	case v2RPCTypeAppendEntries:
		args, err := v2DecodeAppendEntriesArgs(data)
		if err != nil {
			return nil
		}
		reply := m.node.HandleAppendEntries(args)
		replyEnvelope, _ = v2EncodeRPC(v2RPCTypeAppendEntriesReply, reply)

	case v2RPCTypeInstallSnapshot:
		args, err := v2DecodeInstallSnapshotArgs(data)
		if err != nil {
			return nil
		}
		reply := m.node.HandleInstallSnapshot(args)
		replyEnvelope, _ = v2EncodeRPC(v2RPCTypeInstallSnapshotReply, reply)

	case v2RPCTypeTimeoutNow:
		m.node.HandleTimeoutNow()
		replyEnvelope, _ = v2EncodeRPC(v2RPCTypeTimeoutNowReply, nil)

	default:
		return nil
	}

	return &transport.Message{Type: transport.StreamMetaRaft, Payload: replyEnvelope}
}
