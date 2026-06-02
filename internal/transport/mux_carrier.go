package transport

import (
	"context"
	"io"
)

// MuxCarrier is a per-peer mux connection that yields bidirectional byte streams
// for the raft control-plane mux driver (internal/raft). It abstracts the driver
// off the concrete carrier: a QUIC mux conn yields N streams (S2b-1); a TCP
// carrier (S2b-2) yields N conns. The driver opens/accepts streams one at a time,
// frames raft RPCs over them (corrID mux), and tears the carrier down on break.
//
// The carrier is used for EXACTLY these operations — no datagrams, conn-level
// deadlines, or carrier-specific identity — which is what makes the abstraction
// clean and TCP-implementable.
type MuxCarrier interface {
	// OpenStream opens one bidirectional stream (dialer side).
	OpenStream(ctx context.Context) (io.ReadWriteCloser, error)
	// AcceptStream accepts one bidirectional stream (acceptor side).
	AcceptStream(ctx context.Context) (io.ReadWriteCloser, error)
	// RemoteAddr is the peer address label (used for the RaftConn peer id).
	RemoteAddr() string
	// Close tears the carrier down on break (QUIC: CloseWithError(0, cause)).
	Close(cause error) error
}

// MuxConnHandler is invoked once per accepted mux connection (mux ALPN). The
// handler owns the carrier lifetime: it must arrange stream accept/read and
// carrier close. Registered by internal/raft (the only consumer of mux
// connections); the transport package does not interpret mux frames.
type MuxConnHandler func(ctx context.Context, carrier MuxCarrier)
