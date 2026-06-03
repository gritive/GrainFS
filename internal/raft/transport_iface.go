package raft

import (
	"context"

	"github.com/gritive/GrainFS/internal/transport"
)

// raftRPCTransport is the transport-agnostic RPC sender surface (Call to send,
// Handle to register the inbound handler) embedded by muxDriverTransport below.
// *transport.TCPTransport satisfies this today.
type raftRPCTransport interface {
	Call(ctx context.Context, addr string, req *transport.Message) (*transport.Message, error)
	Handle(st transport.StreamType, h transport.StreamHandler)
}

// muxDriverTransport is the surface the per-group mux driver (GroupRaftMux /
// muxPeerState) uses: RPC plus the mux-connection lifecycle. As of S2b-1 it is
// carrier-agnostic — GetOrConnectMux/EvictMux/MuxConnHandler speak
// transport.MuxCarrier (not *quic.Conn) — so a TCP mux carrier (S2b-2) can
// satisfy it without touching this driver. *transport.TCPTransport satisfies it
// today via muxCarrier.
type muxDriverTransport interface {
	raftRPCTransport
	SetMuxConnHandler(h transport.MuxConnHandler)
	GetOrConnectMux(ctx context.Context, addr string) (transport.MuxCarrier, error)
	EvictMux(addr string, carrier transport.MuxCarrier)
}

var (
	// TCPTransport (the sole cluster transport, S6) satisfies the transport-agnostic
	// RPC surface (S1) AND, since S2b-2 added the mux carrier
	// (GetOrConnectMux/EvictMux/SetMuxConnHandler), muxDriverTransport — proven
	// multi-node over the carrier by S5b's raftv2_group_mux_tcp_test.go.
	_ raftRPCTransport   = (*transport.TCPTransport)(nil)
	_ muxDriverTransport = (*transport.TCPTransport)(nil)
)
