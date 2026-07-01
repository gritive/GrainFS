package raft

import (
	"context"

	"github.com/gritive/GrainFS/internal/transport"
)

// raftRPCTransport is the transport-agnostic RPC surface the group/meta raft
// senders use: the native buffered-route surface (CallBuffered/
// RegisterBufferedRoute).
type raftRPCTransport interface {
	CallBuffered(ctx context.Context, addr, path string, payload []byte) ([]byte, error)
	RegisterBufferedRoute(path string, h transport.BufferedRouteHandler)
}

// HTTPTransport satisfies the RPC surface.
var _ raftRPCTransport = (*transport.HTTPTransport)(nil)
