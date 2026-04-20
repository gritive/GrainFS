package transport

import (
	"context"
	"io"
)

// StreamType distinguishes the purpose of a QUIC stream.
type StreamType byte

const (
	StreamControl StreamType = 0x01 // Raft messages (votes, heartbeats, AppendEntries)
	StreamData    StreamType = 0x02 // Shard transfers (bulk data)
	StreamAdmin   StreamType = 0x03 // Cluster management, health checks
	StreamReceipt StreamType = 0x04 // Heal-receipt rolling-window gossip (Phase 16 Slice 2)
)

// Message is a framed message sent over a transport stream.
// Wire format: [1 byte StreamType][4 bytes big-endian length][payload]
type Message struct {
	Type    StreamType
	Payload []byte
}

// ReceivedMessage wraps a Message with sender information.
type ReceivedMessage struct {
	From    string
	Message *Message
}

// Transport provides node-to-node communication over QUIC.
type Transport interface {
	// Listen starts accepting incoming connections on the given address.
	Listen(ctx context.Context, addr string) error

	// Connect opens a connection to a remote peer.
	Connect(ctx context.Context, addr string) error

	// Send sends a message to a peer identified by address.
	Send(ctx context.Context, addr string, msg *Message) error

	// Receive returns a channel that delivers incoming messages.
	Receive() <-chan *ReceivedMessage

	// Close shuts down the transport and all connections.
	Close() error
}

// Codec handles message framing: encoding and decoding on the wire.
type Codec interface {
	Encode(w io.Writer, msg *Message) error
	Decode(r io.Reader) (*Message, error)
}
