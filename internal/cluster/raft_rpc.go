// raft_rpc.go — Raft RPC bridge for the cluster data raft.
//
// It registers a transport.StreamControl handler that decodes inbound Raft RPCs
// via the wire codec (raft_codec.go) and dispatches them through
// cluster.RaftNode.Handle* (the adapter forwards to the raft Node). Outbound
// RPCs go through the callback pair the cluster layer wires into the adapter.
//
// The wire format is the FlatBuffers RPC envelope in raft_codec.go; it is the
// sole raft RPC codec (the QUIC-era v1 it once mirrored is deleted).

package cluster

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/transport"
)

const (
	// raftRPCTimeout bounds one raft RPC (RequestVote/AppendEntries/TimeoutNow)
	// over the HTTP cluster transport: a warm pooled POST plus the one stale-conn
	// retry (httpRetryIf). It MUST stay shorter than the minimum election timeout
	// (raft.DefaultElectionTimeout) so an in-flight heartbeat completes before a
	// follower starts a spurious election — guarded by
	// TestRaftRPCTimeout_BelowElectionTimeout.
	raftRPCTimeout      = 80 * time.Millisecond
	raftSnapshotTimeout = 60 * time.Second
)

// RaftRPCTransport bridges Raft RPCs over the cluster transport for raft v2. It registers
// an inbound handler on transport.StreamControl and exposes v1-style outbound
// send callbacks (SetTransport / SetInstallSnapshotTransport /
// SetTimeoutNowTransport) that the cluster layer hands to the RaftNode adapter.
type RaftRPCTransport struct {
	transport clusterRPCTransport

	nodeMu sync.RWMutex
	node   RaftNode
}

// NewRaftRPCTransport wires the inbound StreamControl handler. The
// returned struct exposes the send callbacks the cluster layer pumps into
// RaftNode.SetTransport.
func NewRaftRPCTransport(tr clusterRPCTransport, node RaftNode) *RaftRPCTransport {
	rpc := &RaftRPCTransport{transport: tr, node: node}
	// Native /raft/data/rpc buffered route. The wire payload is the v2 FB RPC
	// envelope; a decode failure / unknown RPC maps to a 500 exactly as the
	// tunnel's nil-response StatusError did.
	tr.RegisterBufferedRoute(transport.RouteRaftDataRPC, rpc.handleRPC)
	return rpc
}

// SetNode replaces the RaftNode the transport dispatches to. Safe for
// concurrent use with the inbound handler (handleRPC); callers that wrap the
// existing node should pair this with GetNode to read the current value.
func (r *RaftRPCTransport) SetNode(n RaftNode) {
	r.nodeMu.Lock()
	defer r.nodeMu.Unlock()
	r.node = n
}

// GetNode returns the current RaftNode. Safe for concurrent use.
func (r *RaftRPCTransport) GetNode() RaftNode {
	r.nodeMu.RLock()
	defer r.nodeMu.RUnlock()
	return r.node
}

// SetTransport wires the outbound callbacks into the RaftNode (matches the
// callback API the deleted v1 meta transport used).
func (r *RaftRPCTransport) SetTransport() {
	r.GetNode().SetTransport(r.sendRequestVote, r.sendAppendEntries)
}

func (r *RaftRPCTransport) sendRequestVote(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	ctx, cancel := context.WithTimeout(context.Background(), raftRPCTimeout)
	defer cancel()

	envelope, err := encodeRPC(rpcTypeRequestVote, args)
	if err != nil {
		return nil, err
	}
	reply, err := r.transport.CallBuffered(ctx, peer, transport.RouteRaftDataRPC, envelope)
	if err != nil {
		return nil, fmt.Errorf("RequestVote to %s: %w", peer, err)
	}
	rpcType, data, err := decodeRPC(reply)
	if err != nil {
		return nil, err
	}
	if rpcType != rpcTypeRequestVoteReply {
		return nil, fmt.Errorf("unexpected reply type: %s", rpcType)
	}
	return decodeRequestVoteReply(data)
}

func (r *RaftRPCTransport) sendAppendEntries(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	ctx, cancel := context.WithTimeout(context.Background(), raftRPCTimeout)
	defer cancel()

	envelope, err := encodeRPC(rpcTypeAppendEntries, args)
	if err != nil {
		return nil, err
	}
	reply, err := r.transport.CallBuffered(ctx, peer, transport.RouteRaftDataRPC, envelope)
	if err != nil {
		return nil, fmt.Errorf("AppendEntries to %s: %w", peer, err)
	}
	rpcType, data, err := decodeRPC(reply)
	if err != nil {
		return nil, err
	}
	if rpcType != rpcTypeAppendEntriesReply {
		return nil, fmt.Errorf("unexpected reply type: %s", rpcType)
	}
	return decodeAppendEntriesReply(data)
}

func (r *RaftRPCTransport) sendTimeoutNow(peer string, args *raft.TimeoutNowArgs) (*raft.TimeoutNowReply, error) {
	ctx, cancel := context.WithTimeout(context.Background(), raftRPCTimeout)
	defer cancel()

	envelope, err := encodeRPC(rpcTypeTimeoutNow, args)
	if err != nil {
		return nil, err
	}
	reply, err := r.transport.CallBuffered(ctx, peer, transport.RouteRaftDataRPC, envelope)
	if err != nil {
		return nil, fmt.Errorf("TimeoutNow to %s: %w", peer, err)
	}
	rpcType, _, err := decodeRPC(reply)
	if err != nil {
		return nil, err
	}
	if rpcType != rpcTypeTimeoutNowReply {
		return nil, fmt.Errorf("unexpected reply type: %s", rpcType)
	}
	return &raft.TimeoutNowReply{}, nil
}

// SetTimeoutNowTransport wires the outbound TimeoutNow callback into the RaftNode.
func (r *RaftRPCTransport) SetTimeoutNowTransport() {
	r.GetNode().SetTimeoutNowTransport(r.sendTimeoutNow)
}

// handleRPC dispatches inbound Raft RPCs to the v2 node via the RaftNode
// interface. The interface methods accept v1 wire types (raft.*); the v2
// adapter translates to v2 native types and back (see raftv2adapter.go).
func (r *RaftRPCTransport) handleRPC(payload []byte) ([]byte, error) {
	rpcType, data, err := decodeRPC(payload)
	if err != nil {
		return nil, fmt.Errorf("raft data RPC: bad request")
	}

	var replyEnvelope []byte

	node := r.GetNode()
	switch rpcType {
	case rpcTypeRequestVote:
		args, err := decodeRequestVoteArgs(data)
		if err != nil {
			return nil, fmt.Errorf("raft data RPC: bad request")
		}
		reply := node.HandleRequestVote(args)
		replyEnvelope, _ = encodeRPC(rpcTypeRequestVoteReply, reply)

	case rpcTypeAppendEntries:
		args, err := decodeAppendEntriesArgs(data)
		if err != nil {
			return nil, fmt.Errorf("raft data RPC: bad request")
		}
		reply := node.HandleAppendEntries(args)
		replyEnvelope, _ = encodeRPC(rpcTypeAppendEntriesReply, reply)

	case rpcTypeInstallSnapshot:
		args, err := decodeInstallSnapshotArgs(data)
		if err != nil {
			return nil, fmt.Errorf("raft data RPC: bad request")
		}
		reply := node.HandleInstallSnapshot(args)
		replyEnvelope, _ = encodeRPC(rpcTypeInstallSnapshotReply, reply)

	case rpcTypeTimeoutNow:
		node.HandleTimeoutNow()
		replyEnvelope, _ = encodeRPC(rpcTypeTimeoutNowReply, nil)

	default:
		return nil, fmt.Errorf("raft data RPC: bad request")
	}

	return replyEnvelope, nil
}
