// raftv2_meta_quic.go — v2 meta-raft QUIC bridge.
//
// Mirrors internal/cluster/raft_quic_rpc.go (per-group v2 bridge from PR 27)
// but registers transport.StreamMetaRaft instead of transport.StreamControl.
// v1's internal/raft/meta_transport_quic.go is the reference; this file is
// the v2 equivalent. PR 30b deletes v1 and renames this file.
//
// Wire codec is shared with raft_quic_rpc.go (raftv2_quic_codec.go). The same
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
)

// RaftV2MetaQUICTransport bridges meta-Raft RPCs over QUIC for raft v2. It
// registers an inbound handler on transport.StreamMetaRaft and exposes the
// three outbound Send* methods that satisfy cluster.MetaTransport.
type RaftV2MetaQUICTransport struct {
	transport *transport.QUICTransport
	node      RaftNode
}

// compile-time check: RaftV2MetaQUICTransport must satisfy MetaTransport.
var _ MetaTransport = (*RaftV2MetaQUICTransport)(nil)

// NewRaftV2MetaQUICTransport wires the inbound StreamMetaRaft handler.
func NewRaftV2MetaQUICTransport(tr *transport.QUICTransport, node RaftNode) *RaftV2MetaQUICTransport {
	mt := &RaftV2MetaQUICTransport{transport: tr, node: node}
	tr.Handle(transport.StreamMetaRaft, mt.handleRPC)
	return mt
}

// SendRequestVote mirrors v1's MetaRaftQUICTransport.SendRequestVote — the
// wire envelope uses the shared v2 codec.
func (m *RaftV2MetaQUICTransport) SendRequestVote(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
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

// SendAppendEntries mirrors v1's MetaRaftQUICTransport.SendAppendEntries.
func (m *RaftV2MetaQUICTransport) SendAppendEntries(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
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

// SendInstallSnapshot mirrors v1's MetaRaftQUICTransport.SendInstallSnapshot.
// The 60s timeout accommodates large snapshot payloads.
func (m *RaftV2MetaQUICTransport) SendInstallSnapshot(peer string, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
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
func (m *RaftV2MetaQUICTransport) handleRPC(req *transport.Message) *transport.Message {
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

	default:
		return nil
	}

	return &transport.Message{Type: transport.StreamMetaRaft, Payload: replyEnvelope}
}
