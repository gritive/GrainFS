package raft

import (
	"context"

	"github.com/gritive/GrainFS/internal/transport"
)

// raftRPCTransport is the transport-agnostic RPC surface the group/meta raft
// senders use: Call to send one request/response RPC, Handle to register the
// inbound handler. Both the HTTP and TCP cluster transports satisfy it.
type raftRPCTransport interface {
	Call(ctx context.Context, addr string, req *transport.Message) (*transport.Message, error)
	Handle(st transport.StreamType, h transport.StreamHandler)
}

// HTTPTransport satisfies the RPC surface.
var _ raftRPCTransport = (*transport.HTTPTransport)(nil)
