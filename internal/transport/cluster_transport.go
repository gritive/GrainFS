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
// The mux-connection methods speak MuxCarrier (S2b-1), so this interface is no
// longer QUIC-shaped: a TCP mux carrier (S2b-2) can satisfy it. *TCPTransport
// satisfies it today via muxCarrier.
type ClusterTransport interface {
	// Transport covers Listen/Connect/Send/Receive/Close.
	Transport

	Call(ctx context.Context, addr string, req *Message) (*Message, error)
	CallWithBody(ctx context.Context, addr string, req *Message, body io.Reader) (*Message, error)
	CallRead(ctx context.Context, addr string, req *Message) (*Message, io.ReadCloser, error)
	CallFlatBuffer(ctx context.Context, addr string, fw *FlatBuffersWriter) (*Message, error)

	Handle(st StreamType, h StreamHandler)
	HandleBody(st StreamType, h StreamBodyHandler)
	HandleRead(st StreamType, h StreamReadHandler)
	SetStreamHandler(h StreamHandler)

	SetMuxConnHandler(h MuxConnHandler)
	GetOrConnectMux(ctx context.Context, addr string) (MuxCarrier, error)
	EvictMux(addr string, carrier MuxCarrier)

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

// TCPTransport is the sole cluster transport (S6 removed the QUIC stack).
var _ ClusterTransport = (*TCPTransport)(nil)
