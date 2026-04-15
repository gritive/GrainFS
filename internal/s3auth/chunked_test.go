package s3auth

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeAWSChunkedBody(t *testing.T) {
	// Simulate aws-chunked encoding: two chunks + final zero chunk
	input := "5;chunk-signature=abc123\r\nhello\r\n6;chunk-signature=def456\r\n world\r\n0;chunk-signature=final\r\n\r\n"

	data, err := DecodeAWSChunkedBody([]byte(input))
	require.NoError(t, err)
	assert.Equal(t, "hello world", string(data))
}

func TestDecodeAWSChunkedBody_SingleChunk(t *testing.T) {
	input := "d;chunk-signature=sig1\r\nhello, world!\r\n0;chunk-signature=sig0\r\n\r\n"

	data, err := DecodeAWSChunkedBody([]byte(input))
	require.NoError(t, err)
	assert.Equal(t, "hello, world!", string(data))
}

func TestDecodeAWSChunkedBody_EmptyBody(t *testing.T) {
	input := "0;chunk-signature=sig0\r\n\r\n"

	data, err := DecodeAWSChunkedBody([]byte(input))
	require.NoError(t, err)
	assert.Empty(t, data)
}

func TestDecodeAWSChunkedBody_NoSignature(t *testing.T) {
	// Some clients may omit the signature
	input := "5\r\nhello\r\n0\r\n\r\n"

	data, err := DecodeAWSChunkedBody([]byte(input))
	require.NoError(t, err)
	assert.Equal(t, "hello", string(data))
}
