package transport

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBinaryCodec_RoundTrip(t *testing.T) {
	tests := []struct {
		name string
		msg  *Message
	}{
		{"control stream message", &Message{Type: StreamControl, Payload: []byte("vote-request")}},
		{"data stream message", &Message{Type: StreamData, Payload: bytes.Repeat([]byte("X"), 4096)}},
		{"admin stream message", &Message{Type: StreamAdmin, Payload: []byte("health-check")}},
		{"empty payload", &Message{Type: StreamControl, Payload: []byte{}}},
	}

	codec := &BinaryCodec{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			require.NoError(t, codec.Encode(&buf, tt.msg))

			got, err := codec.Decode(&buf)
			require.NoError(t, err)
			require.Equal(t, tt.msg.Type, got.Type)
			require.Equal(t, tt.msg.Payload, got.Payload)
		})
	}
}

func TestBinaryCodec_MultipleMessages(t *testing.T) {
	codec := &BinaryCodec{}
	var buf bytes.Buffer

	messages := []*Message{
		{Type: StreamControl, Payload: []byte("msg1")},
		{Type: StreamData, Payload: []byte("msg2")},
		{Type: StreamAdmin, Payload: []byte("msg3")},
	}

	for _, msg := range messages {
		require.NoError(t, codec.Encode(&buf, msg))
	}

	for i, want := range messages {
		got, err := codec.Decode(&buf)
		require.NoError(t, err, "message %d", i)
		require.Equal(t, want.Type, got.Type)
		require.Equal(t, want.Payload, got.Payload)
	}
}

func TestBinaryCodec_WireFormat(t *testing.T) {
	codec := &BinaryCodec{}
	var buf bytes.Buffer

	msg := &Message{Type: StreamControl, Payload: []byte("hi")}
	require.NoError(t, codec.Encode(&buf, msg))

	data := buf.Bytes()
	require.Len(t, data, 7) // 5 header + 2 payload
	require.Equal(t, byte(StreamControl), data[0])
	require.Equal(t, uint32(2), binary.BigEndian.Uint32(data[1:5]))
	require.Equal(t, "hi", string(data[5:]))
}

func TestBinaryCodec_DecodeErrors(t *testing.T) {
	codec := &BinaryCodec{}

	t.Run("truncated header", func(t *testing.T) {
		_, err := codec.Decode(bytes.NewReader([]byte{0x01, 0x00}))
		require.Error(t, err)
	})

	t.Run("truncated payload", func(t *testing.T) {
		header := [5]byte{byte(StreamControl)}
		binary.BigEndian.PutUint32(header[1:], 100)
		_, err := codec.Decode(bytes.NewReader(append(header[:], []byte("hi")...)))
		require.Error(t, err)
	})

	t.Run("empty reader", func(t *testing.T) {
		_, err := codec.Decode(bytes.NewReader([]byte{}))
		require.Error(t, err)
	})

	t.Run("payload exceeds max size", func(t *testing.T) {
		header := [5]byte{byte(StreamControl)}
		binary.BigEndian.PutUint32(header[1:], maxPayloadSize+1)
		r := io.MultiReader(bytes.NewReader(header[:]), io.LimitReader(zeroReader{}, int64(maxPayloadSize+1)))
		_, err := codec.Decode(r)
		require.Error(t, err)
	})
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}
