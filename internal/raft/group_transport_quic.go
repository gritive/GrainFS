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

// GroupRaftQUICMux multiplexes per-group raft RPCs over the StreamGroupRaft
// stream type. A single mux instance is shared across all per-group raft nodes
// on a server; incoming RPCs are dispatched to the correct node by group ID.
//
// Wire prefix: [4B BE groupIDLen][groupID bytes][raft RPC FlatBuffers payload]
type GroupRaftQUICMux struct {
	tr    *transport.QUICTransport
	nodes sync.Map // string(groupID) → *Node
}

// NewGroupRaftQUICMux creates a mux and registers its incoming RPC handler on
// the QUIC transport. The mux must outlive all registered nodes.
func NewGroupRaftQUICMux(tr *transport.QUICTransport) *GroupRaftQUICMux {
	m := &GroupRaftQUICMux{tr: tr}
	tr.Handle(transport.StreamGroupRaft, m.handleRPC)
	return m
}

// Register associates node with groupID for incoming RPC dispatch. Must be
// called after InstantiateLocalGroup returns so the node pointer is stable.
func (m *GroupRaftQUICMux) Register(groupID string, node *Node) {
	m.nodes.Store(groupID, node)
}

// ForGroup returns a GroupRaftSender for the given group. The returned sender
// satisfies cluster.groupTransport and can be passed to GroupLifecycleConfig.
func (m *GroupRaftQUICMux) ForGroup(groupID string) *GroupRaftSender {
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
	gidLen := binary.BigEndian.Uint32(buf[:4])
	if uint32(len(buf)) < 4+gidLen {
		return "", nil, fmt.Errorf("group raft mux: truncated group ID")
	}
	return string(buf[4 : 4+gidLen]), buf[4+gidLen:], nil
}

func (m *GroupRaftQUICMux) handleRPC(req *transport.Message) *transport.Message {
	groupID, payload, err := extractGroupID(req.Payload)
	if err != nil {
		return nil
	}
	v, ok := m.nodes.Load(groupID)
	if !ok {
		return nil
	}
	node := v.(*Node)

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

// GroupRaftSender is the per-group sender returned by GroupRaftQUICMux.ForGroup.
// It satisfies cluster.groupTransport (RequestVote + AppendEntries).
type GroupRaftSender struct {
	mux     *GroupRaftQUICMux
	groupID string
}

func (s *GroupRaftSender) RequestVote(peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	ctx, cancel := context.WithTimeout(context.Background(), groupRaftRPCTimeout)
	defer cancel()
	env, err := encodeRPC(rpcTypeRequestVote, args)
	if err != nil {
		return nil, err
	}
	resp, err := s.mux.tr.Call(ctx, peer, &transport.Message{
		Type:    transport.StreamGroupRaft,
		Payload: prefixGroupID(s.groupID, env),
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
