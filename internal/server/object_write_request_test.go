package server

import (
	"bytes"
	"io"
	"strconv"
	"testing"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

func TestPutObjectContentMD5Hex(t *testing.T) {
	mk := func(v string) *app.RequestContext {
		c := app.NewContext(0)
		if v != "" {
			c.Request.Header.Set("Content-MD5", v)
		}
		return c
	}
	// absent → "", no error
	h, err := putObjectContentMD5Hex(mk(""))
	require.NoError(t, err)
	require.Equal(t, "", h)
	// valid base64 of md5("hello") → hex, no error
	h, err = putObjectContentMD5Hex(mk("XUFAKrxLKna5cZ2REBfFkg=="))
	require.NoError(t, err)
	require.Equal(t, "5d41402abc4b2a76b9719d911017c592", h)
	// malformed (not base64) → ErrInvalidDigest
	_, err = putObjectContentMD5Hex(mk("not-base64!!!"))
	require.ErrorIs(t, err, storage.ErrInvalidDigest)
	// valid base64 but not 16 bytes → ErrInvalidDigest
	_, err = putObjectContentMD5Hex(mk("aGVsbG8="))
	require.ErrorIs(t, err, storage.ErrInvalidDigest)
}

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
	c.Request.SetBodyStream(bytes.NewReader(nil), 8<<20)

	require.True(t, putObjectShouldStream(c))
}

// Every known-length body streams now, including small ones (no size threshold):
// Hertz hands us a body stream for all sizes, so buffering small objects only
// added RSS without benefit.
func TestPutObjectShouldStreamSmallKnownLengthBody(t *testing.T) {
	c := app.NewContext(0)
	c.Request.SetBodyStream(bytes.NewReader(nil), 1024)

	require.True(t, putObjectShouldStream(c))
}

func TestPutObjectShouldStreamSmallChunkedBody(t *testing.T) {
	c := app.NewContext(0)
	c.Request.SetBodyStream(bytes.NewReader(nil), 4096)
	c.Request.Header.Set("Content-Encoding", "aws-chunked")
	c.Request.Header.Set("X-Amz-Decoded-Content-Length", "1024")

	require.True(t, putObjectShouldStream(c))
}

// Real S3 clients (minio-go, AWS SDK) over HTTP send aws-chunked
// (STREAMING-AWS4-HMAC-SHA256-PAYLOAD) with X-Amz-Decoded-Content-Length set to
// the true object size. Any usable decoded size streams (no threshold), since
// putObjectPayloadReader already decodes the chunked stream incrementally.
func TestPutObjectShouldStreamAWSChunkedBodyWithDecodedLength(t *testing.T) {
	c := app.NewContext(0)
	// Content-Length is the ENCODED length (chunk framing); larger than decoded.
	c.Request.SetBodyStream(bytes.NewReader(nil), (8<<20)+4096)
	c.Request.Header.Set("Content-Encoding", "aws-chunked")
	c.Request.Header.Set("X-Amz-Decoded-Content-Length", strconv.Itoa(8<<20))

	require.True(t, putObjectShouldStream(c))
}

// aws-chunked WITHOUT a usable X-Amz-Decoded-Content-Length must NOT stream: the
// exact-length reader has no trustworthy decoded size, so fall back to the
// buffered path (safe default, no regression for malformed/edge clients).
func TestPutObjectShouldNotStreamAWSChunkedBodyWithoutDecodedLength(t *testing.T) {
	c := app.NewContext(0)
	c.Request.SetBodyStream(bytes.NewReader(nil), 1024)
	c.Request.Header.Set("Content-Encoding", "aws-chunked")

	require.False(t, putObjectShouldStream(c))
}

// A malformed/negative X-Amz-Decoded-Content-Length is treated as no usable size
// and falls back to the buffered path rather than streaming an untrustworthy len.
func TestPutObjectShouldNotStreamAWSChunkedBodyMalformedDecodedLength(t *testing.T) {
	c := app.NewContext(0)
	c.Request.SetBodyStream(bytes.NewReader(nil), 1024)
	c.Request.Header.Set("Content-Encoding", "aws-chunked")
	c.Request.Header.Set("X-Amz-Decoded-Content-Length", "-1")

	require.False(t, putObjectShouldStream(c))
}

func TestPutObjectStreamLength(t *testing.T) {
	// non-chunked: Content-Length (set via SetBodyStream's length arg)
	t.Run("plain uses content-length", func(t *testing.T) {
		c := app.NewContext(0)
		c.Request.SetBodyStream(bytes.NewReader(nil), 10485760)
		require.Equal(t, int64(10485760), putObjectStreamLength(c))
	})
	// aws-chunked: decoded length, NOT the larger encoded Content-Length
	t.Run("aws-chunked uses decoded length", func(t *testing.T) {
		c := app.NewContext(0)
		c.Request.SetBodyStream(bytes.NewReader(nil), 10500246) // encoded (framing)
		c.Request.Header.Set("Content-Encoding", "aws-chunked")
		c.Request.Header.Set("X-Amz-Decoded-Content-Length", "10485760") // decoded
		require.Equal(t, int64(10485760), putObjectStreamLength(c))
	})
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
