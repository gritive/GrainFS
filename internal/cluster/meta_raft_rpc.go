// meta_raft_rpc.go — meta-raft RPC bridge. Mirrors raft_rpc.go (the per-group
// bridge) but registers the meta route (transport.RouteRaftMetaRPC) instead of
// the group route.
//
// Wire codec is shared with raft_rpc.go (raft_codec.go); the same RPC type
// strings ("RequestVote", "AppendEntries", "InstallSnapshot") flow over the
// meta route. History: this file descends from the M5 raft v1→v2 migration
// (the deleted v1 meta-stream wire used "Meta*" prefixed type names).
//
// Both meta-Raft (this file) and per-group raft (raft_rpc.go) register
// independent handlers on different routes, so coexistence at runtime is safe.

package cluster

import (
	"context"
	"fmt"
	"time"

	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/transport"
)

const (
	// metaRaftRPCTimeout bounds one meta-raft RPC. Meta-Raft elections
	// run alongside data Raft, shard RPC, and S3 traffic in multi-process
	// tests; 80ms was too tight under local/CI CPU contention, hence 500ms.
	metaRaftRPCTimeout = 500 * time.Millisecond
	// metaRaftSnapshotTimeout — 60s budget for large InstallSnapshot payloads.
	metaRaftSnapshotTimeout = 60 * time.Second
)

// MetaRaftTransport bridges meta-Raft RPCs over the cluster transport. It
// registers an inbound handler on transport.RouteRaftMetaRPC and exposes the
// outbound Send* methods (one buffered call per RPC) that satisfy cluster.MetaTransport.
type MetaRaftTransport struct {
	transport clusterRPCTransport
	node      RaftNode
}

// compile-time check: MetaRaftTransport must satisfy MetaTransport.
var _ MetaTransport = (*MetaRaftTransport)(nil)

// NewMetaTransport constructs the meta-Raft transport and wires the inbound
// meta-route handler. The node parameter is the cluster.RaftNode interface.
func NewMetaTransport(tr clusterRPCTransport, node RaftNode) *MetaRaftTransport {
	mt := &MetaRaftTransport{transport: tr, node: node}
	// Native /raft/meta/rpc buffered route. The wire payload is the shared
	// FB RPC envelope; a decode failure / unknown RPC maps to a 500 exactly as
	// the tunnel's nil-response StatusError did.
	tr.RegisterBufferedRoute(transport.RouteRaftMetaRPC, mt.handleRPC)
	return mt
}

// SendRequestVote sends a RequestVote RPC to the peer — the wire envelope
// uses the shared codec (raft_codec.go).
func (m *MetaRaftTransport) SendRequestVote(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	ctx, cancel := context.WithTimeout(context.Background(), metaRaftRPCTimeout)
	defer cancel()

	envelope, err := encodeRPC(rpcTypeRequestVote, args)
	if err != nil {
		return nil, err
	}
	reply, err := m.transport.CallBuffered(ctx, peer, transport.RouteRaftMetaRPC, envelope)
	if err != nil {
		return nil, fmt.Errorf("meta RequestVote to %s: %w", peer, err)
	}
	rpcType, data, err := decodeRPC(reply)
	if err != nil {
		return nil, err
	}
	if rpcType != rpcTypeRequestVoteReply {
		return nil, fmt.Errorf("meta RequestVote: unexpected reply type %s", rpcType)
	}
	return decodeRequestVoteReply(data)
}

// SendAppendEntries sends an AppendEntries RPC to the peer.
func (m *MetaRaftTransport) SendAppendEntries(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	ctx, cancel := context.WithTimeout(context.Background(), metaRaftRPCTimeout)
	defer cancel()

	envelope, err := encodeRPC(rpcTypeAppendEntries, args)
	if err != nil {
		return nil, err
	}
	reply, err := m.transport.CallBuffered(ctx, peer, transport.RouteRaftMetaRPC, envelope)
	if err != nil {
		return nil, fmt.Errorf("meta AppendEntries to %s: %w", peer, err)
	}
	rpcType, data, err := decodeRPC(reply)
	if err != nil {
		return nil, err
	}
	if rpcType != rpcTypeAppendEntriesReply {
		return nil, fmt.Errorf("meta AppendEntries: unexpected reply type %s", rpcType)
	}
	return decodeAppendEntriesReply(data)
}

// SendTimeoutNow sends a TimeoutNow RPC to the transfer target, triggering an
// immediate election. Called by the raft node during TransferLeadership.
func (m *MetaRaftTransport) SendTimeoutNow(peer string, args *raft.TimeoutNowArgs) (*raft.TimeoutNowReply, error) {
	ctx, cancel := context.WithTimeout(context.Background(), metaRaftRPCTimeout)
	defer cancel()

	envelope, err := encodeRPC(rpcTypeTimeoutNow, args)
	if err != nil {
		return nil, err
	}
	reply, err := m.transport.CallBuffered(ctx, peer, transport.RouteRaftMetaRPC, envelope)
	if err != nil {
		return nil, fmt.Errorf("meta TimeoutNow to %s: %w", peer, err)
	}
	rpcType, _, err := decodeRPC(reply)
	if err != nil {
		return nil, err
	}
	if rpcType != rpcTypeTimeoutNowReply {
		return nil, fmt.Errorf("meta TimeoutNow: unexpected reply type %s", rpcType)
	}
	return &raft.TimeoutNowReply{}, nil
}

// SendInstallSnapshot sends an InstallSnapshot RPC to the peer. The 60s
// timeout accommodates large snapshot payloads.
func (m *MetaRaftTransport) SendInstallSnapshot(peer string, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	ctx, cancel := context.WithTimeout(context.Background(), metaRaftSnapshotTimeout)
	defer cancel()

	envelope, err := encodeRPC(rpcTypeInstallSnapshot, args)
	if err != nil {
		return nil, err
	}
	reply, err := m.transport.CallBuffered(ctx, peer, transport.RouteRaftMetaRPC, envelope)
	if err != nil {
		return nil, fmt.Errorf("meta InstallSnapshot to %s: %w", peer, err)
	}
	rpcType, data, err := decodeRPC(reply)
	if err != nil {
		return nil, err
	}
	if rpcType != rpcTypeInstallSnapshotReply {
		return nil, fmt.Errorf("meta InstallSnapshot: unexpected reply type %s", rpcType)
	}
	return decodeInstallSnapshotReply(data)
}

// handleRPC decodes an inbound meta-raft RPC envelope and dispatches via the
// RaftNode interface. Mirrors raft_rpc.go::handleRPC for the per-group bridge.
func (m *MetaRaftTransport) handleRPC(payload []byte) ([]byte, error) {
	rpcType, data, err := decodeRPC(payload)
	if err != nil {
		return nil, fmt.Errorf("meta raft RPC: bad request")
	}

	var replyEnvelope []byte

	switch rpcType {
	case rpcTypeRequestVote:
		args, err := decodeRequestVoteArgs(data)
		if err != nil {
			return nil, fmt.Errorf("meta raft RPC: bad request")
		}
		reply := m.node.HandleRequestVote(args)
		replyEnvelope, _ = encodeRPC(rpcTypeRequestVoteReply, reply)

	case rpcTypeAppendEntries:
		args, err := decodeAppendEntriesArgs(data)
		if err != nil {
			return nil, fmt.Errorf("meta raft RPC: bad request")
		}
		reply := m.node.HandleAppendEntries(args)
		replyEnvelope, _ = encodeRPC(rpcTypeAppendEntriesReply, reply)

	case rpcTypeInstallSnapshot:
		args, err := decodeInstallSnapshotArgs(data)
		if err != nil {
			return nil, fmt.Errorf("meta raft RPC: bad request")
		}
		reply := m.node.HandleInstallSnapshot(args)
		replyEnvelope, _ = encodeRPC(rpcTypeInstallSnapshotReply, reply)

	case rpcTypeTimeoutNow:
		m.node.HandleTimeoutNow()
		replyEnvelope, _ = encodeRPC(rpcTypeTimeoutNowReply, nil)

	default:
		return nil, fmt.Errorf("meta raft RPC: bad request")
	}

	return replyEnvelope, nil
}
