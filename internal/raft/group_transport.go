package raft

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/gritive/GrainFS/internal/transport"
)

const groupRaftRPCTimeout = 80 * time.Millisecond

// RaftV2Handler is the subset of inbound Raft RPC entry points the per-group
// mux needs to dispatch into a v2 raft node. *Node (v1) satisfies this
// interface natively (HandleRequestVote / HandleAppendEntries are the same
// shape); the cluster-layer v2 adapter satisfies it via raftv2adapter.go.
//
// Defined locally inside internal/raft to avoid an import cycle with
// internal/cluster (cluster already imports raft). M5 PR 28b — once v1 is
// deleted in PR 30 this can fold into Node's method set.
type RaftV2Handler interface {
	HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply
	HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply
}

// GroupRaftMux dispatches per-group raft RPCs over the StreamGroupRaft stream
// type. A single instance is shared across all per-group raft nodes on a server;
// incoming RPCs are dispatched to the correct node by group ID, and outbound
// RPCs go through transport.Call (one request/response per RPC).
//
// Wire prefix: [4B BE groupIDLen][groupID bytes][raft RPC FlatBuffers payload]
//
// nodes stores RaftV2Handler. Both v1 (*Node) and the cluster-layer v2 adapter
// satisfy that interface, so the dispatch site (handleRPC) is agnostic to which
// raft engine owns the group.
type GroupRaftMux struct {
	tr    raftRPCTransport
	nodes sync.Map // string(groupID) → RaftV2Handler
}

// NewGroupRaftMux creates the dispatcher and registers its incoming RPC handler
// on the cluster transport. It must outlive all registered nodes.
func NewGroupRaftMux(tr raftRPCTransport) *GroupRaftMux {
	m := &GroupRaftMux{tr: tr}
	tr.Handle(transport.StreamGroupRaft, m.handleRPC)
	return m
}

// Register associates a raft handler with groupID for incoming RPC dispatch.
// Must be called after InstantiateLocalGroup returns so the handler pointer
// is stable. Panics on reserved or empty groupID — by the time we're here,
// upstream validation in applyPutShardGroup should have already rejected
// anything invalid, so reaching this with a bad ID is a programming bug.
// Passing nil is rejected to prevent a nil-deref crash on the dispatch path.
//
// As of M5 PR 29 the v1 Register(*Node) form is gone (v2 is the only path);
// callers pass the cluster-layer v2 adapter via the RaftV2Handler interface.
// *Node (v1) still satisfies RaftV2Handler — v1 tests use it — but the
// production path is v2-only.
func (m *GroupRaftMux) Register(groupID string, h RaftV2Handler) {
	if err := ValidateGroupID(groupID); err != nil {
		panic(fmt.Sprintf("GroupRaftMux.Register: invalid groupID: %v", err))
	}
	if h == nil {
		panic(fmt.Sprintf("GroupRaftMux.Register: nil handler for groupID %q", groupID))
	}
	m.nodes.Store(groupID, h)
}

// ForGroup returns a GroupRaftSender for the given group. The returned sender
// satisfies cluster.groupTransport and can be passed to GroupLifecycleConfig.
func (m *GroupRaftMux) ForGroup(groupID string) *GroupRaftSender {
	return &GroupRaftSender{mux: m, groupID: groupID}
}

func prefixGroupID(groupID string, payload []byte) []byte {
	gid := []byte(groupID)
	out := make([]byte, 4+len(gid)+len(payload))
	binary.BigEndian.PutUint32(out[:4], uint32(len(gid)))
	copy(out[4:], gid)
	copy(out[4+len(gid):], payload)
	return out
}

func extractGroupID(buf []byte) (groupID string, payload []byte, err error) {
	if len(buf) < 4 {
		return "", nil, fmt.Errorf("group raft mux: short header")
	}
	gidLen := int(binary.BigEndian.Uint32(buf[:4]))
	if gidLen > 256 || len(buf) < 4+gidLen {
		return "", nil, fmt.Errorf("group raft mux: truncated group ID")
	}
	return string(buf[4 : 4+gidLen]), buf[4+gidLen:], nil
}

func (m *GroupRaftMux) handleRPC(req *transport.Message) *transport.Message {
	groupID, payload, err := extractGroupID(req.Payload)
	if err != nil {
		return nil
	}
	v, ok := m.nodes.Load(groupID)
	if !ok {
		return nil
	}
	node := v.(RaftV2Handler)

	rpcType, data, err := decodeRPC(payload)
	if err != nil {
		return nil
	}
	var replyEnv []byte
	switch rpcType {
	case rpcTypeRequestVote:
		args, err := decodeRequestVoteArgs(data)
		if err != nil {
			return nil
		}
		reply := node.HandleRequestVote(args)
		replyEnv, _ = encodeRPC(rpcTypeRequestVoteReply, reply)
	case rpcTypeAppendEntries:
		args, err := decodeAppendEntriesArgs(data)
		if err != nil {
			return nil
		}
		reply := node.HandleAppendEntries(args)
		replyEnv, _ = encodeRPC(rpcTypeAppendEntriesReply, reply)
	default:
		return nil
	}
	return &transport.Message{Type: transport.StreamGroupRaft, Payload: replyEnv}
}

// GroupRaftSender is the per-group sender returned by GroupRaftMux.ForGroup.
// It satisfies cluster.groupTransport (RequestVote + AppendEntries).
type GroupRaftSender struct {
	mux     *GroupRaftMux
	groupID string
}

func (s *GroupRaftSender) RequestVote(peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	ctx, cancel := context.WithTimeout(context.Background(), groupRaftRPCTimeout)
	defer cancel()
	env, err := encodeRPC(rpcTypeRequestVote, args)
	if err != nil {
		return nil, err
	}
	payload := prefixGroupID(s.groupID, env)

	resp, err := s.mux.tr.Call(ctx, peer, &transport.Message{
		Type:    transport.StreamGroupRaft,
		Payload: payload,
	})
	if err != nil {
		return nil, fmt.Errorf("group %s RequestVote to %s: %w", s.groupID, peer, err)
	}
	_, data, err := decodeRPC(resp.Payload)
	if err != nil {
		return nil, fmt.Errorf("group %s RequestVote reply: %w", s.groupID, err)
	}
	return decodeRequestVoteReply(data)
}

func (s *GroupRaftSender) AppendEntries(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	ctx, cancel := context.WithTimeout(context.Background(), groupRaftRPCTimeout)
	defer cancel()

	env, err := encodeRPC(rpcTypeAppendEntries, args)
	if err != nil {
		return nil, err
	}
	resp, err := s.mux.tr.Call(ctx, peer, &transport.Message{
		Type:    transport.StreamGroupRaft,
		Payload: prefixGroupID(s.groupID, env),
	})
	if err != nil {
		return nil, fmt.Errorf("group %s AppendEntries to %s: %w", s.groupID, peer, err)
	}
	_, data, err := decodeRPC(resp.Payload)
	if err != nil {
		return nil, fmt.Errorf("group %s AppendEntries reply: %w", s.groupID, err)
	}
	return decodeAppendEntriesReply(data)
}
