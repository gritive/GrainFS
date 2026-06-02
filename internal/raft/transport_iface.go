package raft

import (
	"context"

	"github.com/gritive/GrainFS/internal/transport"
)

// raftRPCTransport is the transport-agnostic RPC sender surface used by the
// meta-Raft transport (Call to send, Handle to register the inbound handler).
// *transport.QUICTransport satisfies this today.
type raftRPCTransport interface {
	Call(ctx context.Context, addr string, req *transport.Message) (*transport.Message, error)
	Handle(st transport.StreamType, h transport.StreamHandler)
}

// muxDriverTransport is the surface the per-group mux driver (GroupRaftQUICMux /
// muxPeerState) uses: RPC plus the mux-connection lifecycle. As of S2b-1 it is
// carrier-agnostic — GetOrConnectMux/EvictMux/MuxConnHandler speak
// transport.MuxCarrier (not *quic.Conn) — so a TCP mux carrier (S2b-2) can
// satisfy it without touching this driver. *transport.QUICTransport satisfies it
// today via quicMuxCarrier.
type muxDriverTransport interface {
	raftRPCTransport
	SetMuxConnHandler(h transport.MuxConnHandler)
	GetOrConnectMux(ctx context.Context, addr string) (transport.MuxCarrier, error)
	EvictMux(addr string, carrier transport.MuxCarrier)
}

var (
	_ raftRPCTransport   = (*transport.QUICTransport)(nil)
	_ muxDriverTransport = (*transport.QUICTransport)(nil)

	// S1 TCP transport satisfies the transport-agnostic RPC surface. It does NOT
	// yet satisfy muxDriverTransport — the TCP mux carrier + GetOrConnectMux/
	// EvictMux/SetMuxConnHandler on TCPTransport are the S2b-2 driver's job.
	_ raftRPCTransport = (*transport.TCPTransport)(nil)
)
