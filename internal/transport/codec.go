package transport

import (
	"encoding/binary"
	"fmt"
	"io"

	flatbuffers "github.com/google/flatbuffers/go"
)

const (
	// headerSize is StreamType (1 byte) + request ID (8 bytes) + status
	// (1 byte) + payload length (4 bytes).
	headerSize = 14
	// maxPayloadSize prevents allocation of unreasonably large buffers.
	maxPayloadSize = 64 * 1024 * 1024 // 64MB
)

// BinaryCodec implements length-prefixed binary framing.
// Wire format: [1 byte StreamType][8 bytes request ID][1 byte status][4 bytes big-endian payload length][payload]
type BinaryCodec struct{}

// Encode writes a framed message to w.
func (c *BinaryCodec) Encode(w io.Writer, msg *Message) error {
	header := [headerSize]byte{}
	header[0] = byte(msg.Type)
	binary.BigEndian.PutUint64(header[1:9], msg.ID)
	header[9] = byte(msg.Status)
	binary.BigEndian.PutUint32(header[10:], uint32(len(msg.Payload)))

	if _, err := w.Write(header[:]); err != nil {
		return fmt.Errorf("write header: %w", err)
	}
	if _, err := w.Write(msg.Payload); err != nil {
		return fmt.Errorf("write payload: %w", err)
	}
	return nil
}

// FlatBuffersWriter holds a live FlatBuffers builder for zero-copy encoding.
// Builder는 EncodeWriterTo 반환 후 caller가 pool에 반환해야 한다.
type FlatBuffersWriter struct {
	Typ     StreamType
	ID      uint64
	Status  MessageStatus
	Builder *flatbuffers.Builder
}

// EncodeWriterTo writes the FlatBuffers payload directly from Builder.FinishedBytes()
// without make+copy. Builder must remain alive until this returns.
func (c *BinaryCodec) EncodeWriterTo(w io.Writer, fw *FlatBuffersWriter) error {
	raw := fw.Builder.FinishedBytes()
	header := [headerSize]byte{}
	header[0] = byte(fw.Typ)
	binary.BigEndian.PutUint64(header[1:9], fw.ID)
	header[9] = byte(fw.Status)
	binary.BigEndian.PutUint32(header[10:], uint32(len(raw)))
	if _, err := w.Write(header[:]); err != nil {
		return fmt.Errorf("write header: %w", err)
	}
	if _, err := w.Write(raw); err != nil {
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
	id := binary.BigEndian.Uint64(header[1:9])
	status := MessageStatus(header[9])
	payloadLen := binary.BigEndian.Uint32(header[10:])

	if payloadLen > maxPayloadSize {
		return nil, fmt.Errorf("payload size %d exceeds max %d", payloadLen, maxPayloadSize)
	}

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, fmt.Errorf("read payload: %w", err)
	}

	return &Message{Type: streamType, ID: id, Status: status, Payload: payload}, nil
}
