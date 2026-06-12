package transport

import (
	"context"
	"crypto/tls"
	"io"
)

// ClusterTransport is the full node-to-node transport surface the composition
// root (serveruntime/boot) holds. It is a superset of every use-site role
// interface in cluster/raft, so the boot-held value can be passed to those
// narrower constructors (Go interface-to-interface assignment).
//
// Raft and shard RPCs are request/response over Call*/Handle*; there is no
// stream-multiplexing surface (the raft mux subsystem and the TCP transport that
// carried it were removed in Phase 8). HTTPTransport is the only implementation.
type ClusterTransport interface {
	// Transport covers Listen/Connect/Send/Receive/Close.
	Transport

	Call(ctx context.Context, addr string, req *Message) (*Message, error)
	CallPooled(ctx context.Context, addr string, req *Message) (*Message, error)
	CallWithBody(ctx context.Context, addr string, req *Message, body io.Reader) (*Message, error)
	CallRead(ctx context.Context, addr string, req *Message) (*Message, io.ReadCloser, error)
	CallFlatBuffer(ctx context.Context, addr string, fw *FlatBuffersWriter) (*Message, error)

	Handle(st StreamType, h StreamHandler)
	HandleBody(st StreamType, h StreamBodyHandler)
	HandleRead(st StreamType, h StreamReadHandler)
	SetStreamHandler(h StreamHandler)

	// Native route surfaces (Phase 8 N6+; the envelope-free per-family wire).
	RegisterShardWriteHandler(h ShardWriteHandler)
	ShardWrite(ctx context.Context, addr string, req ShardWriteRequest, body io.Reader) error
	RegisterShardReadHandler(h ShardReadHandler)
	ShardRead(ctx context.Context, addr string, req ShardReadRequest) (io.ReadCloser, error)
	RegisterForwardWriteHandler(h ForwardWriteHandler)
	RegisterForwardReadHandler(h ForwardReadHandler)
	ForwardWrite(ctx context.Context, addr string, frame []byte, body io.Reader) ([]byte, error)
	ForwardRead(ctx context.Context, addr string, frame []byte) ([]byte, io.ReadCloser, error)
	RegisterAppendSegmentReadHandler(h AppendSegmentReadHandler)
	AppendSegmentRead(ctx context.Context, addr string, frame []byte) ([]byte, io.ReadCloser, error)

	// Generic native primitives (Phase 8 N7-3): buffered-Call and gossip
	// routes for the long-tail families (route table in http_buffered_route.go).
	RegisterBufferedRoute(path string, h BufferedRouteHandler)
	CallBuffered(ctx context.Context, addr, path string, payload []byte) ([]byte, error)
	RegisterGossipRoute(path string, h GossipHandler)
	GossipSend(ctx context.Context, addr, path string, payload []byte) error

	RecycleConns()
	ClosePeer(addr string)
	LocalAddr() string

	SwapIdentity(snap *IdentitySnapshot)
	ApplyRotation(window [][32]byte, present tls.Certificate, presentSPKI [32]byte, newBase *[32]byte)
	FlipPresent(cert tls.Certificate, spki [32]byte)
	UpdateRegistryAccept(spkis [][32]byte)
	SeedInitialPeerSPKIs(spkis [][32]byte)
	SetTrafficLimits(l TrafficLimits)
	SetDropped()
}

// HTTPTransport is the sole cluster transport.
var _ ClusterTransport = (*HTTPTransport)(nil)
