package cluster

import (
	"context"
	"io"

	"github.com/gritive/GrainFS/internal/transport"
)

// Use-site transport role interfaces. Each consumer in this package depends on
// the minimal method subset it actually calls, so a TCP transport (later QUIC->TCP
// migration slices) can be slotted in without changing consumers. *transport.QUICTransport
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
	CallWithBody(ctx context.Context, addr string, req *transport.Message, body io.Reader) (*transport.Message, error)
	CallRead(ctx context.Context, addr string, req *transport.Message) (*transport.Message, io.ReadCloser, error)
	CallFlatBuffer(ctx context.Context, addr string, fw *transport.FlatBuffersWriter) (*transport.Message, error)
	Handle(st transport.StreamType, h transport.StreamHandler)
	HandleBody(st transport.StreamType, h transport.StreamBodyHandler)
	HandleRead(st transport.StreamType, h transport.StreamReadHandler)
	Close() error
}

// appendSegRegistrar: append-segment read handler registration only.
type appendSegRegistrar interface {
	HandleRead(st transport.StreamType, h transport.StreamReadHandler)
}

// Compile-time conformance: both the live QUIC transport and the S1 TCP transport
// satisfy every transport-agnostic role interface in this package.
var (
	_ clusterRPCTransport = (*transport.QUICTransport)(nil)
	_ callerTransport     = (*transport.QUICTransport)(nil)
	_ shardTransport      = (*transport.QUICTransport)(nil)
	_ appendSegRegistrar  = (*transport.QUICTransport)(nil)

	_ clusterRPCTransport = (*transport.TCPTransport)(nil)
	_ callerTransport     = (*transport.TCPTransport)(nil)
	_ shardTransport      = (*transport.TCPTransport)(nil)
	_ appendSegRegistrar  = (*transport.TCPTransport)(nil)
)
