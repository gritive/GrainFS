package raft

import (
	"context"

	"github.com/quic-go/quic-go"

	"github.com/gritive/GrainFS/internal/transport"
)

// raftRPCTransport is the transport-agnostic RPC sender surface used by the
// meta-Raft transport (Call to send, Handle to register the inbound handler).
// *transport.QUICTransport satisfies this today.
type raftRPCTransport interface {
	Call(ctx context.Context, addr string, req *transport.Message) (*transport.Message, error)
	Handle(st transport.StreamType, h transport.StreamHandler)
}

// muxDriverTransport is the surface the QUIC per-group mux driver
// (GroupRaftQUICMux / muxPeerState) uses: RPC plus the mux-connection
// lifecycle. It is intentionally QUIC-shaped — GetOrConnectMux/EvictMux/
// MuxConnHandler reference *quic.Conn — because this driver multiplexes raft
// RPCs over long-lived QUIC streams. The S2 RaftConn restructure replaces this
// driver (and its connection model) for TCP; until then this interface only
// removes the dependency on the concrete *transport.QUICTransport god-type.
type muxDriverTransport interface {
	raftRPCTransport
	SetMuxConnHandler(h transport.MuxConnHandler)
	GetOrConnectMux(ctx context.Context, addr string) (*quic.Conn, error)
	EvictMux(addr string, conn *quic.Conn)
}

var (
	_ raftRPCTransport   = (*transport.QUICTransport)(nil)
	_ muxDriverTransport = (*transport.QUICTransport)(nil)

	// S1 TCP transport satisfies the transport-agnostic RPC surface. It does NOT
	// satisfy muxDriverTransport (QUIC-shaped, *quic.Conn) — that is the S2
	// RaftConn restructure's job.
	_ raftRPCTransport = (*transport.TCPTransport)(nil)
)
