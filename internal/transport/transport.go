package transport

import (
	"context"
	"errors"
	"io"
)

// StreamType distinguishes the purpose of a QUIC stream.
type StreamType byte

const (
	StreamControl             StreamType = 0x01 // Raft messages (votes, heartbeats, AppendEntries)
	StreamData                StreamType = 0x02 // Shard transfers (bulk data)
	StreamAdmin               StreamType = 0x03 // Cluster management, health checks
	StreamReceipt             StreamType = 0x04 // Heal-receipt rolling-window gossip (Phase 16 Slice 2, one-way)
	StreamReceiptQuery        StreamType = 0x05 // Heal-receipt broadcast-fallback RPC (Phase 16 Slice 2, request/response)
	StreamProposeForward      StreamType = 0x06 // Follower → leader ProposeForward RPC (consistent hash ring) — UNCHANGED for wire-compat
	StreamMetaRaft            StreamType = 0x07 // meta-Raft control-plane RPCs (membership, shard-map)
	StreamProposeGroupForward StreamType = 0x08 // Per-group ProposeForward RPC, payload prefixed with [4B groupIDLen][groupID][cmdData]
	StreamGroupRaft           StreamType = 0x09 // Per-group Raft RPCs (RequestVote, AppendEntries), payload prefixed with [4B groupIDLen][groupID][raftRPC]
	StreamReadIndex           StreamType = 0x0A // Follower → leader ReadIndex RPC; response: [8B commitIndex big-endian][4B errLen][errBytes]
	StreamMetaProposeForward  StreamType = 0x0B // Follower → meta-Raft leader Iceberg catalog proposal forwarding
	StreamMetaCatalogRead     StreamType = 0x0C // Follower → meta-Raft leader Iceberg catalog linearized reads
	StreamGroupForwardBody    StreamType = 0x0D // Per-group forwarded write metadata frame followed by raw request body bytes
	StreamMetaJoin            StreamType = 0x0E // New node → meta-Raft leader dynamic join admin RPC
)

type StreamClass byte

const (
	StreamClassControl StreamClass = iota
	StreamClassMeta
	StreamClassData
	StreamClassBulk
)

func ClassOf(st StreamType) StreamClass {
	switch st {
	case StreamMetaRaft, StreamMetaProposeForward, StreamMetaCatalogRead, StreamMetaJoin, StreamReadIndex:
		return StreamClassMeta
	case StreamData, StreamProposeForward, StreamProposeGroupForward, StreamGroupRaft:
		return StreamClassData
	case StreamGroupForwardBody:
		return StreamClassBulk
	default:
		return StreamClassControl
	}
}

type MessageStatus byte

const (
	StatusOK MessageStatus = iota
	StatusOverloaded
	StatusError
)

// Message is a framed message sent over a transport stream.
// Wire format: [1 byte StreamType][8 bytes request ID][1 byte status][4 bytes big-endian length][payload]
type Message struct {
	Type    StreamType
	ID      uint64
	Status  MessageStatus
	Payload []byte
}

func NewResponse(req *Message, payload []byte) *Message {
	if req == nil {
		return &Message{Status: StatusOK, Payload: payload}
	}
	return &Message{Type: req.Type, ID: req.ID, Status: StatusOK, Payload: payload}
}

func NewErrorResponse(req *Message, status MessageStatus, err error) *Message {
	if status == StatusOK {
		status = StatusError
	}
	if err == nil {
		err = errors.New("transport error")
	}
	if req == nil {
		return &Message{Status: status, Payload: []byte(err.Error())}
	}
	return &Message{Type: req.Type, ID: req.ID, Status: status, Payload: []byte(err.Error())}
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
