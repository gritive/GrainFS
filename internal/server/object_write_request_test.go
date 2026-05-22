package server

import (
	"bytes"
	"io"
	"testing"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/stretchr/testify/require"
)

func TestPutObjectBodyReader_UsesStreamingRequestBody(t *testing.T) {
	payload := []byte("streaming upload body")
	body := &countingRequestBody{Reader: bytes.NewReader(payload)}
	c := app.NewContext(0)
	c.Request.SetBodyStream(body, len(payload))

	r, err := putObjectPayloadReader(c)
	require.NoError(t, err)
	require.Zero(t, body.reads, "putObjectPayloadReader should not read streaming body during setup")

	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

func TestPutObjectBodyReader_DecodesAWSChunkedStreamingBody(t *testing.T) {
	input := []byte("5;chunk-signature=abc\r\nhello\r\n6;chunk-signature=def\r\n world\r\n0;chunk-signature=final\r\n\r\n")
	body := &countingRequestBody{Reader: bytes.NewReader(input)}
	c := app.NewContext(0)
	c.Request.SetBodyStream(body, len(input))
	c.Request.Header.Set("Content-Encoding", "aws-chunked")

	r, err := putObjectPayloadReader(c)
	require.NoError(t, err)
	require.Zero(t, body.reads, "putObjectPayloadReader should not read streaming body during setup")

	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, "hello world", string(got))
}

func TestPutObjectShouldStreamLargeNonChunkedBody(t *testing.T) {
	c := app.NewContext(0)
	c.Request.SetBodyStream(bytes.NewReader(nil), putObjectStreamingThresholdBytes)

	require.True(t, putObjectShouldStream(c))
}

func TestPutObjectShouldNotStreamAWSChunkedBody(t *testing.T) {
	c := app.NewContext(0)
	c.Request.SetBodyStream(bytes.NewReader(nil), putObjectStreamingThresholdBytes)
	c.Request.Header.Set("Content-Encoding", "aws-chunked")

	require.False(t, putObjectShouldStream(c))
}

func TestExactLengthReaderReturnsUnexpectedEOFOnShortBody(t *testing.T) {
	r := newExactLengthReader(bytes.NewReader([]byte("abc")), 5)

	got, err := io.ReadAll(r)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.Equal(t, []byte("abc"), got)
}
