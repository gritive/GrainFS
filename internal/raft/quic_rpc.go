package raft

import (
	"context"
	"fmt"

	"github.com/gritive/GrainFS/internal/transport"
)

// RPC message types for QUIC transport.
const (
	rpcTypeRequestVote        = "RequestVote"
	rpcTypeRequestVoteReply   = "RequestVoteReply"
	rpcTypeAppendEntries      = "AppendEntries"
	rpcTypeAppendEntriesReply = "AppendEntriesReply"
)

// QUICRPCTransport bridges Raft RPCs over the QUIC transport layer
// using bidirectional streams (request-response per stream).
type QUICRPCTransport struct {
	transport *transport.QUICTransport
	node      *Node
}

// NewQUICRPCTransport creates an RPC layer on top of QUIC transport.
// It registers a stream handler for incoming Raft RPCs and provides
// send callbacks for the Raft node.
func NewQUICRPCTransport(tr *transport.QUICTransport, node *Node) *QUICRPCTransport {
	rpc := &QUICRPCTransport{
		transport: tr,
		node:      node,
	}
	tr.SetStreamHandler(rpc.handleRPC)
	return rpc
}

// SetTransport wires sendRequestVote and sendAppendEntries into the Raft node.
func (r *QUICRPCTransport) SetTransport() {
	r.node.SetTransport(r.sendRequestVote, r.sendAppendEntries)
}

func (r *QUICRPCTransport) sendRequestVote(peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	envelope, err := encodeRPC(rpcTypeRequestVote, args)
	if err != nil {
		return nil, err
	}

	msg := &transport.Message{Type: transport.StreamControl, Payload: envelope}
	resp, err := r.transport.Call(context.Background(), peer, msg)
	if err != nil {
		return nil, fmt.Errorf("RequestVote to %s: %w", peer, err)
	}

	rpcType, data, err := decodeRPC(resp.Payload)
	if err != nil {
		return nil, err
	}
	if rpcType != rpcTypeRequestVoteReply {
		return nil, fmt.Errorf("unexpected reply type: %s", rpcType)
	}
	return decodeRequestVoteReply(data)
}

func (r *QUICRPCTransport) sendAppendEntries(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	envelope, err := encodeRPC(rpcTypeAppendEntries, args)
	if err != nil {
		return nil, err
	}

	msg := &transport.Message{Type: transport.StreamControl, Payload: envelope}
	resp, err := r.transport.Call(context.Background(), peer, msg)
	if err != nil {
		return nil, fmt.Errorf("AppendEntries to %s: %w", peer, err)
	}

	rpcType, data, err := decodeRPC(resp.Payload)
	if err != nil {
		return nil, err
	}
	if rpcType != rpcTypeAppendEntriesReply {
		return nil, fmt.Errorf("unexpected reply type: %s", rpcType)
	}
	return decodeAppendEntriesReply(data)
}

// handleRPC dispatches incoming Raft RPCs to the node's handlers and returns the response.
func (r *QUICRPCTransport) handleRPC(req *transport.Message) *transport.Message {
	rpcType, data, err := decodeRPC(req.Payload)
	if err != nil {
		return nil
	}

	var replyEnvelope []byte

	switch rpcType {
	case rpcTypeRequestVote:
		args, err := decodeRequestVoteArgs(data)
		if err != nil {
			return nil
		}
		reply := r.node.HandleRequestVote(args)
		replyEnvelope, _ = encodeRPC(rpcTypeRequestVoteReply, reply)

	case rpcTypeAppendEntries:
		args, err := decodeAppendEntriesArgs(data)
		if err != nil {
			return nil
		}
		reply := r.node.HandleAppendEntries(args)
		replyEnvelope, _ = encodeRPC(rpcTypeAppendEntriesReply, reply)

	default:
		return nil
	}

	return &transport.Message{Type: transport.StreamControl, Payload: replyEnvelope}
}
