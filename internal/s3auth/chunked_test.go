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

func TestDecodeAWSChunkedBody_LargeChunks(t *testing.T) {
	// 100개 청크, 각 100바이트: Grow 없으면 buffer가 여러 번 재할당
	// 기능 정확성만 검증 (Grow는 구현 수준 최적화)
	var input string
	for i := 0; i < 100; i++ {
		input += "64;chunk-signature=x\r\n"
		for j := 0; j < 100; j++ {
			input += "a"
		}
		input += "\r\n"
	}
	input += "0;chunk-signature=final\r\n\r\n"

	data, err := DecodeAWSChunkedBody([]byte(input))
	require.NoError(t, err)
	assert.Equal(t, 100*100, len(data))
}
