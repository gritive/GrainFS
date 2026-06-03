package server

import (
	"bytes"
	"io"
	"strconv"
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

// Real S3 clients (minio-go, AWS SDK) over HTTP send aws-chunked
// (STREAMING-AWS4-HMAC-SHA256-PAYLOAD) with X-Amz-Decoded-Content-Length set to
// the true object size. When that decoded size is at/above the streaming
// threshold the body must stream (not buffer the whole object), since
// putObjectPayloadReader already decodes the chunked stream incrementally.
func TestPutObjectShouldStreamAWSChunkedBodyWithDecodedLength(t *testing.T) {
	c := app.NewContext(0)
	// Content-Length is the ENCODED length (chunk framing); larger than decoded.
	c.Request.SetBodyStream(bytes.NewReader(nil), putObjectStreamingThresholdBytes+4096)
	c.Request.Header.Set("Content-Encoding", "aws-chunked")
	c.Request.Header.Set("X-Amz-Decoded-Content-Length", strconv.Itoa(putObjectStreamingThresholdBytes))

	require.True(t, putObjectShouldStream(c))
}

// aws-chunked WITHOUT a usable X-Amz-Decoded-Content-Length must NOT stream: the
// exact-length reader has no trustworthy decoded size, so fall back to the
// buffered path (safe default, no regression for malformed/edge clients).
func TestPutObjectShouldNotStreamAWSChunkedBodyWithoutDecodedLength(t *testing.T) {
	c := app.NewContext(0)
	c.Request.SetBodyStream(bytes.NewReader(nil), putObjectStreamingThresholdBytes)
	c.Request.Header.Set("Content-Encoding", "aws-chunked")

	require.False(t, putObjectShouldStream(c))
}

// A decoded size below the threshold stays on the buffered path (small objects
// are cheap to buffer; streaming overhead is not worth it).
func TestPutObjectShouldNotStreamAWSChunkedBodyBelowThreshold(t *testing.T) {
	c := app.NewContext(0)
	c.Request.SetBodyStream(bytes.NewReader(nil), putObjectStreamingThresholdBytes)
	c.Request.Header.Set("Content-Encoding", "aws-chunked")
	c.Request.Header.Set("X-Amz-Decoded-Content-Length", strconv.Itoa(putObjectStreamingThresholdBytes-1))

	require.False(t, putObjectShouldStream(c))
}

func TestPutObjectDecodedContentLength(t *testing.T) {
	cases := []struct {
		name   string
		header string
		set    bool
		want   int64
	}{
		{"absent", "", false, -1},
		{"valid", "10485760", true, 10485760},
		{"zero", "0", true, 0},
		{"malformed", "abc", true, -1},
		{"negative", "-5", true, -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := app.NewContext(0)
			if tc.set {
				c.Request.Header.Set("X-Amz-Decoded-Content-Length", tc.header)
			}
			require.Equal(t, tc.want, putObjectDecodedContentLength(c))
		})
	}
}

func TestExactLengthReaderReturnsUnexpectedEOFOnShortBody(t *testing.T) {
	r := newExactLengthReader(bytes.NewReader([]byte("abc")), 5)

	got, err := io.ReadAll(r)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.Equal(t, []byte("abc"), got)
}
