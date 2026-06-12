package raft

import (
	"context"

	"github.com/gritive/GrainFS/internal/transport"
)

// raftRPCTransport is the transport-agnostic RPC surface the group/meta raft
// senders use. It carries BOTH the tunnel surface (Call/Handle — deleted in
// Phase 8 N8) and the native buffered-route surface (CallBuffered/
// RegisterBufferedRoute) while the per-family migrations are staged.
type raftRPCTransport interface {
	Call(ctx context.Context, addr string, req *transport.Message) (*transport.Message, error)
	Handle(st transport.StreamType, h transport.StreamHandler)
	CallBuffered(ctx context.Context, addr, path string, payload []byte) ([]byte, error)
	RegisterBufferedRoute(path string, h transport.BufferedRouteHandler)
}

// HTTPTransport satisfies the RPC surface.
var _ raftRPCTransport = (*transport.HTTPTransport)(nil)
