// raftv2_quic_rpc.go — QUIC RPC bridge for raft v2.
//
// Mirrors internal/raft.QUICRPCTransport: it registers a transport.StreamControl
// handler that decodes inbound Raft RPCs via the v2 wire codec
// (raftv2_quic_codec.go) and dispatches them through cluster.RaftNode.Handle*
// (the v2 adapter forwards to raftv2.Node). Outbound RPCs go through the
// v1-style callback pair the cluster layer already wires into the v2 adapter.
//
// This file is the v2 counterpart to v1's quic_rpc.go and is byte-compatible
// on the wire. Only registered when cluster.IsV2Enabled("cluster") so v1 is
// the default until PR 28.

package cluster

import (
	"context"
	"fmt"
	"time"

	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/transport"
)

const (
	// v2RaftRPCTimeout mirrors v1's raftRPCTimeout (internal/raft/quic_rpc.go).
	// Must stay shorter than the minimum election timeout so heartbeats can
	// reconnect before a follower starts a spurious election.
	v2RaftRPCTimeout      = 80 * time.Millisecond
	v2RaftSnapshotTimeout = 60 * time.Second
)

// RaftV2QUICRPCTransport bridges Raft RPCs over QUIC for raft v2. It registers
// an inbound handler on transport.StreamControl and exposes v1-style outbound
// send callbacks (SetTransport / SetInstallSnapshotTransport /
// SetTimeoutNowTransport) that the cluster layer hands to the RaftNode adapter.
type RaftV2QUICRPCTransport struct {
	transport *transport.QUICTransport
	node      RaftNode
}

// NewRaftV2QUICRPCTransport wires the inbound StreamControl handler. The
// returned struct exposes the send callbacks the cluster layer pumps into
// RaftNode.SetTransport.
func NewRaftV2QUICRPCTransport(tr *transport.QUICTransport, node RaftNode) *RaftV2QUICRPCTransport {
	rpc := &RaftV2QUICRPCTransport{transport: tr, node: node}
	tr.Handle(transport.StreamControl, rpc.handleRPC)
	return rpc
}

// SetTransport wires the outbound callbacks into the RaftNode (matches the
// v1-style API used by *raft.QUICRPCTransport).
func (r *RaftV2QUICRPCTransport) SetTransport() {
	r.node.SetTransport(r.sendRequestVote, r.sendAppendEntries)
}

func (r *RaftV2QUICRPCTransport) sendRequestVote(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	ctx, cancel := context.WithTimeout(context.Background(), v2RaftRPCTimeout)
	defer cancel()

	envelope, err := v2EncodeRPC(v2RPCTypeRequestVote, args)
	if err != nil {
		return nil, err
	}
	msg := &transport.Message{Type: transport.StreamControl, Payload: envelope}
	resp, err := r.transport.Call(ctx, peer, msg)
	if err != nil {
		return nil, fmt.Errorf("RequestVote to %s: %w", peer, err)
	}
	rpcType, data, err := v2DecodeRPC(resp.Payload)
	if err != nil {
		return nil, err
	}
	if rpcType != v2RPCTypeRequestVoteReply {
		return nil, fmt.Errorf("unexpected reply type: %s", rpcType)
	}
	return v2DecodeRequestVoteReply(data)
}

func (r *RaftV2QUICRPCTransport) sendAppendEntries(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	ctx, cancel := context.WithTimeout(context.Background(), v2RaftRPCTimeout)
	defer cancel()

	envelope, err := v2EncodeRPC(v2RPCTypeAppendEntries, args)
	if err != nil {
		return nil, err
	}
	msg := &transport.Message{Type: transport.StreamControl, Payload: envelope}
	resp, err := r.transport.Call(ctx, peer, msg)
	if err != nil {
		return nil, fmt.Errorf("AppendEntries to %s: %w", peer, err)
	}
	rpcType, data, err := v2DecodeRPC(resp.Payload)
	if err != nil {
		return nil, err
	}
	if rpcType != v2RPCTypeAppendEntriesReply {
		return nil, fmt.Errorf("unexpected reply type: %s", rpcType)
	}
	return v2DecodeAppendEntriesReply(data)
}

// handleRPC dispatches inbound Raft RPCs to the v2 node via the RaftNode
// interface. The interface methods accept v1 wire types (raft.*); the v2
// adapter translates to v2 native types and back (see raftv2adapter.go).
func (r *RaftV2QUICRPCTransport) handleRPC(req *transport.Message) *transport.Message {
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
		reply := r.node.HandleRequestVote(args)
		replyEnvelope, _ = v2EncodeRPC(v2RPCTypeRequestVoteReply, reply)

	case v2RPCTypeAppendEntries:
		args, err := v2DecodeAppendEntriesArgs(data)
		if err != nil {
			return nil
		}
		reply := r.node.HandleAppendEntries(args)
		replyEnvelope, _ = v2EncodeRPC(v2RPCTypeAppendEntriesReply, reply)

	case v2RPCTypeInstallSnapshot:
		args, err := v2DecodeInstallSnapshotArgs(data)
		if err != nil {
			return nil
		}
		reply := r.node.HandleInstallSnapshot(args)
		replyEnvelope, _ = v2EncodeRPC(v2RPCTypeInstallSnapshotReply, reply)

	case v2RPCTypeTimeoutNow:
		r.node.HandleTimeoutNow()
		replyEnvelope, _ = v2EncodeRPC(v2RPCTypeTimeoutNowReply, nil)

	default:
		return nil
	}

	return &transport.Message{Type: transport.StreamControl, Payload: replyEnvelope}
}
