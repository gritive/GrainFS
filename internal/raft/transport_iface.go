package raft

import (
	"context"

	"github.com/gritive/GrainFS/internal/transport"
)

// raftRPCTransport is the transport-agnostic RPC sender surface used by the
// meta-Raft transport (Call to send, Handle to register the inbound handler).
// It deliberately excludes the QUIC mux methods (GetOrConnectMux/EvictMux/
// SetMuxConnHandler): GroupRaftQUICMux and muxPeerState are the QUIC mux driver
// and stay on the concrete *transport.QUICTransport until the S2 RaftConn
// restructure replaces them. *transport.QUICTransport satisfies this today.
type raftRPCTransport interface {
	Call(ctx context.Context, addr string, req *transport.Message) (*transport.Message, error)
	Handle(st transport.StreamType, h transport.StreamHandler)
}

var _ raftRPCTransport = (*transport.QUICTransport)(nil)
