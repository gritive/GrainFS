package transport

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	// headerSize is StreamType (1 byte) + payload length (4 bytes).
	headerSize = 5
	// maxPayloadSize prevents allocation of unreasonably large buffers.
	maxPayloadSize = 64 * 1024 * 1024 // 64MB
)

// BinaryCodec implements length-prefixed binary framing.
// Wire format: [1 byte StreamType][4 bytes big-endian payload length][payload]
type BinaryCodec struct{}

// Encode writes a framed message to w.
func (c *BinaryCodec) Encode(w io.Writer, msg *Message) error {
	header := [headerSize]byte{}
	header[0] = byte(msg.Type)
	binary.BigEndian.PutUint32(header[1:], uint32(len(msg.Payload)))

	if _, err := w.Write(header[:]); err != nil {
		return fmt.Errorf("write header: %w", err)
	}
	if _, err := w.Write(msg.Payload); err != nil {
		return fmt.Errorf("write payload: %w", err)
	}
	return nil
}

// Decode reads a framed message from r.
func (c *BinaryCodec) Decode(r io.Reader) (*Message, error) {
	header := [headerSize]byte{}
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}

	streamType := StreamType(header[0])
	payloadLen := binary.BigEndian.Uint32(header[1:])

	if payloadLen > maxPayloadSize {
		return nil, fmt.Errorf("payload size %d exceeds max %d", payloadLen, maxPayloadSize)
	}

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, fmt.Errorf("read payload: %w", err)
	}

	return &Message{Type: streamType, Payload: payload}, nil
}
