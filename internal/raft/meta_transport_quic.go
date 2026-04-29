package raft

import (
	"context"
	"fmt"
	"time"

	"github.com/gritive/GrainFS/internal/transport"
)

const metaRaftRPCTimeout = 80 * time.Millisecond
const metaRaftSnapshotTimeout = 60 * time.Second

// RPC type constants for meta-Raft QUIC transport.
const (
	metaRPCRequestVote          = "MetaRequestVote"
	metaRPCRequestVoteReply     = "MetaRequestVoteReply"
	metaRPCAppendEntries        = "MetaAppendEntries"
	metaRPCAppendEntriesReply   = "MetaAppendEntriesReply"
	metaRPCInstallSnapshot      = "MetaInstallSnapshot"
	metaRPCInstallSnapshotReply = "MetaInstallSnapshotReply"
)

// MetaRaftQUICTransport delivers meta-Raft RPCs over the shared QUIC transport
// using the dedicated StreamMetaRaft stream type. This avoids interference with
// the data-plane Raft group that owns StreamControl.
type MetaRaftQUICTransport struct {
	tr   *transport.QUICTransport
	node *Node
}

// NewMetaRaftQUICTransport creates a meta-Raft RPC transport and registers its
// handler on the QUIC transport. Call SetTransport on the MetaRaft node afterward.
func NewMetaRaftQUICTransport(tr *transport.QUICTransport, node *Node) *MetaRaftQUICTransport {
	m := &MetaRaftQUICTransport{tr: tr, node: node}
	tr.Handle(transport.StreamMetaRaft, m.handleRPC)
	return m
}

func (m *MetaRaftQUICTransport) SendRequestVote(peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
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

func (m *MetaRaftQUICTransport) SendAppendEntries(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
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

func (m *MetaRaftQUICTransport) SendInstallSnapshot(peer string, args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
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
