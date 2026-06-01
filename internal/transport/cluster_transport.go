package transport

import (
	"context"
	"crypto/tls"
	"io"

	"github.com/quic-go/quic-go"
)

// ClusterTransport is the full node-to-node transport surface the composition
// root (serveruntime/boot) holds. It is a superset of every use-site role
// interface in cluster/raft, so the boot-held value can be passed to those
// narrower constructors (Go interface-to-interface assignment).
//
// It is intentionally QUIC-shaped — GetOrConnectMux/EvictMux/SetMuxConnHandler
// reference *quic.Conn — because the per-group mux driver still multiplexes
// over QUIC streams. The S1 TCPTransport will satisfy the transport-agnostic
// subset; the mux-connection methods are reshaped in S2 when the RaftConn
// connection model changes. *QUICTransport satisfies this today.
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
	GetOrConnectMux(ctx context.Context, addr string) (*quic.Conn, error)
	EvictMux(addr string, conn *quic.Conn)

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

var _ ClusterTransport = (*QUICTransport)(nil)
