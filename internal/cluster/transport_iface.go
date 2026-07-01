package cluster

import (
	"context"
	"io"

	"github.com/gritive/GrainFS/internal/transport"
)

// Use-site transport role interfaces. Each consumer in this package depends on
// the minimal method subset it actually calls, so the cluster transport can be
// swapped without changing consumers. *transport.HTTPTransport satisfies all of
// them (compile-time assertions below).

// clusterRPCTransport: buffered request/response RPC + native route
// registration (the raft bridges' surface).
type clusterRPCTransport interface {
	CallBuffered(ctx context.Context, addr, path string, payload []byte) ([]byte, error)
	RegisterBufferedRoute(path string, h transport.BufferedRouteHandler)
}

// callerTransport: outbound buffered RPC only.
type callerTransport interface {
	CallBuffered(ctx context.Context, addr, path string, payload []byte) ([]byte, error)
}

// shardTransport: the rich surface ShardService uses (buffered RPC + native
// typed shard/append-segment routes).
type shardTransport interface {
	CallBuffered(ctx context.Context, addr, path string, payload []byte) ([]byte, error)
	RegisterBufferedRoute(path string, h transport.BufferedRouteHandler)
	ShardWrite(ctx context.Context, addr string, req transport.ShardWriteRequest, body io.Reader) error
	ShardRead(ctx context.Context, addr string, req transport.ShardReadRequest) (io.ReadCloser, error)
	AppendSegmentRead(ctx context.Context, addr string, frame []byte) ([]byte, io.ReadCloser, error)
	Close() error
}

// appendSegRegistrar: append-segment read handler registration only.
type appendSegRegistrar interface {
	RegisterAppendSegmentReadHandler(h transport.AppendSegmentReadHandler)
}

// Compile-time conformance: the HTTP transport (the sole cluster transport)
// satisfies every transport-agnostic role interface in this package.
var (
	_ clusterRPCTransport = (*transport.HTTPTransport)(nil)
	_ callerTransport     = (*transport.HTTPTransport)(nil)
	_ shardTransport      = (*transport.HTTPTransport)(nil)
	_ appendSegRegistrar  = (*transport.HTTPTransport)(nil)
)
