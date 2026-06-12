package cluster

import (
	"context"
	"io"

	"github.com/gritive/GrainFS/internal/transport"
)

// Use-site transport role interfaces. Each consumer in this package depends on
// the minimal method subset it actually calls, so the cluster transport can be
// swapped without changing consumers. *transport.TCPTransport
// satisfies all of them today (compile-time assertions below).

// clusterRPCTransport: small request/response RPC + inbound handler registration.
type clusterRPCTransport interface {
	Call(ctx context.Context, addr string, req *transport.Message) (*transport.Message, error)
	Handle(st transport.StreamType, h transport.StreamHandler)
}

// callerTransport: outbound Call only.
type callerTransport interface {
	Call(ctx context.Context, addr string, req *transport.Message) (*transport.Message, error)
}

// shardTransport: the rich surface ShardService uses (bulk body + flatbuffer + handlers).
type shardTransport interface {
	Call(ctx context.Context, addr string, req *transport.Message) (*transport.Message, error)
	CallPooled(ctx context.Context, addr string, req *transport.Message) (*transport.Message, error)
	CallWithBody(ctx context.Context, addr string, req *transport.Message, body io.Reader) (*transport.Message, error)
	CallRead(ctx context.Context, addr string, req *transport.Message) (*transport.Message, io.ReadCloser, error)
	CallFlatBuffer(ctx context.Context, addr string, fw *transport.FlatBuffersWriter) (*transport.Message, error)
	ShardWrite(ctx context.Context, addr string, req transport.ShardWriteRequest, body io.Reader) error
	Handle(st transport.StreamType, h transport.StreamHandler)
	HandleBody(st transport.StreamType, h transport.StreamBodyHandler)
	HandleRead(st transport.StreamType, h transport.StreamReadHandler)
	Close() error
}

// appendSegRegistrar: append-segment read handler registration only.
type appendSegRegistrar interface {
	HandleRead(st transport.StreamType, h transport.StreamReadHandler)
}

// Compile-time conformance: the HTTP transport (the sole cluster transport)
// satisfies every transport-agnostic role interface in this package.
var (
	_ clusterRPCTransport = (*transport.HTTPTransport)(nil)
	_ callerTransport     = (*transport.HTTPTransport)(nil)
	_ shardTransport      = (*transport.HTTPTransport)(nil)
	_ appendSegRegistrar  = (*transport.HTTPTransport)(nil)
)
