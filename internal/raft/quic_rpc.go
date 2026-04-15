package raft

import (
	"context"
	"fmt"
	"sync"

	"github.com/gritive/GrainFS/internal/transport"
)

// RPC message types for QUIC transport.
const (
	rpcTypeRequestVote        = "RequestVote"
	rpcTypeRequestVoteReply   = "RequestVoteReply"
	rpcTypeAppendEntries      = "AppendEntries"
	rpcTypeAppendEntriesReply = "AppendEntriesReply"
)

// QUICRPCTransport bridges Raft RPCs over the QUIC transport layer.
type QUICRPCTransport struct {
	transport *transport.QUICTransport
	node      *Node
	codec     *transport.BinaryCodec
	pending   sync.Map // correlationID -> chan []byte
	nextID    uint64
	mu        sync.Mutex
}

// NewQUICRPCTransport creates an RPC layer on top of QUIC transport.
func NewQUICRPCTransport(tr *transport.QUICTransport, node *Node) *QUICRPCTransport {
	rpc := &QUICRPCTransport{
		transport: tr,
		node:      node,
		codec:     &transport.BinaryCodec{},
	}
	go rpc.receiveLoop()
	return rpc
}

func (r *QUICRPCTransport) receiveLoop() {
	for msg := range r.transport.Receive() {
		go r.handleMessage(msg)
	}
}

func (r *QUICRPCTransport) handleMessage(recv *transport.ReceivedMessage) {
	rpcType, payload, err := decodeRPC(recv.Message.Payload)
	if err != nil {
		return
	}

	switch rpcType {
	case rpcTypeRequestVote:
		args, err := decodeRequestVoteArgs(payload)
		if err != nil {
			return
		}
		reply := r.node.HandleRequestVote(args)
		r.sendReply(recv.From, rpcTypeRequestVoteReply, reply)

	case rpcTypeAppendEntries:
		args, err := decodeAppendEntriesArgs(payload)
		if err != nil {
			return
		}
		reply := r.node.HandleAppendEntries(args)
		r.sendReply(recv.From, rpcTypeAppendEntriesReply, reply)

	case rpcTypeRequestVoteReply, rpcTypeAppendEntriesReply:
		// Deliver to waiting caller
		r.pending.Range(func(key, value any) bool {
			ch := value.(chan []byte)
			select {
			case ch <- recv.Message.Payload:
			default:
			}
			return false // only deliver to first waiter
		})
	}
}

func (r *QUICRPCTransport) sendReply(to, rpcType string, reply any) {
	envelope, err := encodeRPC(rpcType, reply)
	if err != nil {
		return
	}
	msg := &transport.Message{Type: transport.StreamControl, Payload: envelope}
	r.transport.Send(context.Background(), to, msg)
}

// SendRequestVote sends a RequestVote RPC over QUIC and waits for the reply.
func (r *QUICRPCTransport) SendRequestVote(ctx context.Context, peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	envelope, err := encodeRPC(rpcTypeRequestVote, args)
	if err != nil {
		return nil, err
	}

	// Register reply channel
	r.mu.Lock()
	r.nextID++
	id := r.nextID
	r.mu.Unlock()

	replyCh := make(chan []byte, 1)
	r.pending.Store(id, replyCh)
	defer r.pending.Delete(id)

	msg := &transport.Message{Type: transport.StreamControl, Payload: envelope}
	if err := r.transport.Send(ctx, peer, msg); err != nil {
		return nil, fmt.Errorf("send RequestVote: %w", err)
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case payload := <-replyCh:
		_, data, err := decodeRPC(payload)
		if err != nil {
			return nil, err
		}
		return decodeRequestVoteReply(data)
	}
}

// SendAppendEntries sends an AppendEntries RPC over QUIC and waits for the reply.
func (r *QUICRPCTransport) SendAppendEntries(ctx context.Context, peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	envelope, err := encodeRPC(rpcTypeAppendEntries, args)
	if err != nil {
		return nil, err
	}

	r.mu.Lock()
	r.nextID++
	id := r.nextID
	r.mu.Unlock()

	replyCh := make(chan []byte, 1)
	r.pending.Store(id, replyCh)
	defer r.pending.Delete(id)

	msg := &transport.Message{Type: transport.StreamControl, Payload: envelope}
	if err := r.transport.Send(ctx, peer, msg); err != nil {
		return nil, fmt.Errorf("send AppendEntries: %w", err)
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case payload := <-replyCh:
		_, data, err := decodeRPC(payload)
		if err != nil {
			return nil, err
		}
		return decodeAppendEntriesReply(data)
	}
}
