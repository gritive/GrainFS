package raft

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gritive/GrainFS/internal/transport"
)

const groupRaftRPCTimeout = 80 * time.Millisecond

// GroupRaftQUICMux multiplexes per-group raft RPCs over the StreamGroupRaft
// stream type. A single mux instance is shared across all per-group raft nodes
// on a server; incoming RPCs are dispatched to the correct node by group ID.
//
// Wire prefix: [4B BE groupIDLen][groupID bytes][raft RPC FlatBuffers payload]
//
// Optional mux mode (--quic-mux=true): see group_transport_mux.go. When mux
// mode is enabled, sends use a persistent RaftConn and entries-empty
// AppendEntries calls are coalesced per peer via HeartbeatCoalescer.
type GroupRaftQUICMux struct {
	tr    *transport.QUICTransport
	nodes sync.Map // string(groupID) → *Node

	// metaNode is the meta-raft node registered for the magic groupID
	// metaGroupID ("__meta__"). Atomic so receiver paths
	// (handleMuxRequest, dispatchToLocalGroup) can read without locking.
	// Set via RegisterMetaNode; nil until then.
	metaNode atomic.Pointer[Node]

	// Mux mode state. Set by EnableMux. When muxEnabled is false, all sends
	// use the legacy per-message tr.Call path.
	muxEnabled      atomic.Bool
	muxPoolSize     int
	muxFlushWindow  time.Duration
	muxRegisterOnce sync.Once
	muxMu           sync.RWMutex
	muxPeers        map[string]*muxPeerState
}

// NewGroupRaftQUICMux creates a mux and registers its incoming RPC handler on
// the QUIC transport. The mux must outlive all registered nodes.
func NewGroupRaftQUICMux(tr *transport.QUICTransport) *GroupRaftQUICMux {
	m := &GroupRaftQUICMux{tr: tr, muxPeers: make(map[string]*muxPeerState)}
	tr.Handle(transport.StreamGroupRaft, m.handleRPC)
	return m
}

// Register associates node with groupID for incoming RPC dispatch. Must be
// called after InstantiateLocalGroup returns so the node pointer is stable.
// Panics on reserved or empty groupID — by the time we're here, upstream
// validation in applyPutShardGroup should have already rejected anything
// invalid, so reaching this with a bad ID is a programming bug.
func (m *GroupRaftQUICMux) Register(groupID string, node *Node) {
	if err := ValidateGroupID(groupID); err != nil {
		panic(fmt.Sprintf("GroupRaftQUICMux.Register: invalid groupID: %v", err))
	}
	m.nodes.Store(groupID, node)
}

// RegisterMetaNode wires the meta-raft node onto the shared mux. Idempotent;
// the last registration wins. Called by NewMetaRaftQUICTransport so the
// receiver-side __meta__ branch has somewhere to dispatch before
// EnableMux installs the accept handler. Passing nil is treated as
// deregistration (used by tests during teardown).
func (m *GroupRaftQUICMux) RegisterMetaNode(node *Node) {
	m.metaNode.Store(node)
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
	gidLen := int(binary.BigEndian.Uint32(buf[:4]))
	if gidLen > 256 || len(buf) < 4+gidLen {
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
	payload := prefixGroupID(s.groupID, env)

	// Mux path: persistent stream, no per-message OpenStreamSync.
	if s.mux.muxEnabled.Load() {
		respBytes, err := s.muxCall(ctx, peer, payload)
		if err == nil {
			_, data, err := decodeRPC(respBytes)
			if err != nil {
				return nil, fmt.Errorf("group %s RequestVote mux reply: %w", s.groupID, err)
			}
			return decodeRequestVoteReply(data)
		}
		// Fall back to legacy path on mux dial / send failure (peer may be
		// running an older binary or have mux disabled).
	}

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

// muxCall sends one Request via the per-peer RaftConn, returning the response
// payload bytes. Caller decodes via decodeRPC.
func (s *GroupRaftSender) muxCall(ctx context.Context, peer string, payload []byte) ([]byte, error) {
	ps, err := s.mux.muxConnFor(ctx, peer)
	if err != nil {
		return nil, err
	}
	return ps.rc.Call(ctx, payload)
}

func (s *GroupRaftSender) AppendEntries(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	ctx, cancel := context.WithTimeout(context.Background(), groupRaftRPCTimeout)
	defer cancel()

	// Mux path: route entries-empty heartbeats through the per-peer
	// HeartbeatCoalescer; entries-bearing AE goes direct via RaftConn.Call.
	// Both preserve the synchronous (*Reply, error) caller contract.
	if s.mux.muxEnabled.Load() {
		ps, err := s.mux.muxConnFor(ctx, peer)
		if err == nil {
			if len(args.Entries) == 0 {
				reply, hcErr := ps.hc.AppendEntries(ctx, s.groupID, args)
				if hcErr == nil {
					return reply, nil
				}
				// Coalescer or batch failure: fall through to direct mux call.
			}
			env, encErr := encodeRPC(rpcTypeAppendEntries, args)
			if encErr == nil {
				respBytes, callErr := ps.rc.Call(ctx, prefixGroupID(s.groupID, env))
				if callErr == nil {
					_, data, dErr := decodeRPC(respBytes)
					if dErr != nil {
						return nil, fmt.Errorf("group %s AppendEntries mux reply: %w", s.groupID, dErr)
					}
					return decodeAppendEntriesReply(data)
				}
			}
		}
		// Mux peer dial / send failure → fall back to legacy.
	}

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
