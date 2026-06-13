package server

import (
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/internal/storage"
)

// putObjectContentMD5Hex decodes the client-supplied Content-MD5 header
// (base64-encoded per RFC 1864) to hex so the PUT path can validate it against
// the body. Returns ("", nil) when the header is absent. Returns
// storage.ErrInvalidDigest when the header is present but not a valid 16-byte
// base64 digest (S3 400 InvalidDigest), so the handler rejects before the PUT.
func putObjectContentMD5Hex(c *app.RequestContext) (string, error) {
	raw := string(c.GetHeader("Content-MD5"))
	if raw == "" {
		return "", nil
	}
	decoded, err := base64.StdEncoding.DecodeString(raw)
	if err != nil || len(decoded) != 16 {
		return "", fmt.Errorf("malformed Content-MD5 %q: %w", raw, storage.ErrInvalidDigest)
	}
	return hex.EncodeToString(decoded), nil
}

const putObjectStreamingThresholdBytes = 8 << 20

func putObjectContentType(c *app.RequestContext) string {
	if contentType := string(c.GetHeader("Content-Type")); contentType != "" {
		return contentType
	}
	return "application/octet-stream"
}

// putObjectDecodedContentLength parses the X-Amz-Decoded-Content-Length header
// (the true object size that real S3 clients send alongside an aws-chunked
// STREAMING-* payload, since Content-Length there is the encoded size including
// per-chunk framing). Returns -1 when the header is absent or malformed.
func putObjectDecodedContentLength(c *app.RequestContext) int64 {
	raw := string(c.GetHeader("X-Amz-Decoded-Content-Length"))
	if raw == "" {
		return -1
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n < 0 {
		return -1
	}
	return n
}

// putObjectStreamLength is the object's true (decoded) byte length for the
// streaming PUT/append path. For aws-chunked the wire Content-Length is the
// encoded size (incl. per-chunk framing), so the decoded header is the object
// size the exact-length reader must enforce. putObjectShouldStream guarantees a
// usable value before this is consulted on the streaming branch.
func putObjectStreamLength(c *app.RequestContext) int64 {
	if isAWSChunkedPayload(c) {
		return putObjectDecodedContentLength(c)
	}
	return int64(c.Request.Header.ContentLength())
}

func putObjectShouldStream(c *app.RequestContext) bool {
	if !c.Request.IsBodyStream() {
		return false
	}
	// aws-chunked: Content-Length is the ENCODED size (chunk framing), so gate on
	// the decoded object size. putObjectPayloadReader decodes the chunked stream
	// incrementally (NewAWSChunkedReader), so a large chunked body streams without
	// buffering the whole object. Falls back to buffered when the decoded size is
	// absent/unusable so the exact-length reader always has a trustworthy size.
	if isAWSChunkedPayload(c) {
		return putObjectDecodedContentLength(c) >= putObjectStreamingThresholdBytes
	}
	return int64(c.Request.Header.ContentLength()) >= putObjectStreamingThresholdBytes
}

func putObjectBody(c *app.RequestContext) ([]byte, error) {
	rawBody := c.Request.Body()
	if isAWSChunkedPayload(c) {
		decoded, err := s3auth.DecodeAWSChunkedBody(rawBody)
		if err != nil {
			return nil, fmt.Errorf("invalid aws-chunked encoding: %w", err)
		}
		return decoded, nil
	}
	if expected := c.Request.Header.ContentLength(); expected >= 0 && expected != len(rawBody) {
		return nil, fmt.Errorf("request body size mismatch: content-length=%d actual=%d", expected, len(rawBody))
	}
	return rawBody, nil
}

func putObjectPayloadReader(c *app.RequestContext) (io.Reader, error) {
	if c.Request.IsBodyStream() {
		body := c.Request.BodyStream()
		if isAWSChunkedPayload(c) {
			return s3auth.NewAWSChunkedReader(body), nil
		}
		return body, nil
	}
	body, err := putObjectBody(c)
	if err != nil {
		return nil, err
	}
	return newPutObjectBodyReader(body), nil
}

func isAWSChunkedPayload(c *app.RequestContext) bool {
	contentEncoding := string(c.GetHeader("Content-Encoding"))
	contentSHA := string(c.GetHeader("X-Amz-Content-Sha256"))
	decodedContentLength := string(c.GetHeader("X-Amz-Decoded-Content-Length"))
	trailer := string(c.GetHeader("X-Amz-Trailer"))
	return contentEncoding == "aws-chunked" ||
		strings.HasPrefix(contentSHA, "STREAMING-") ||
		decodedContentLength != "" ||
		trailer != ""
}

type putObjectBodyReader struct {
	body []byte
	off  int64
}

func newPutObjectBodyReader(body []byte) *putObjectBodyReader {
	return &putObjectBodyReader{body: body}
}

func (r *putObjectBodyReader) Read(p []byte) (int, error) {
	if r.off >= int64(len(r.body)) {
		return 0, io.EOF
	}
	n := copy(p, r.body[r.off:])
	r.off += int64(n)
	return n, nil
}

func (r *putObjectBodyReader) Seek(offset int64, whence int) (int64, error) {
	var next int64
	switch whence {
	case io.SeekStart:
		next = offset
	case io.SeekCurrent:
		next = r.off + offset
	case io.SeekEnd:
		next = int64(len(r.body)) + offset
	default:
		return 0, fmt.Errorf("invalid whence: %d", whence)
	}
	if next < 0 {
		return 0, fmt.Errorf("negative position: %d", next)
	}
	r.off = next
	return next, nil
}

func (r *putObjectBodyReader) Len() int {
	if r.off >= int64(len(r.body)) {
		return 0
	}
	return len(r.body) - int(r.off)
}

func (r *putObjectBodyReader) Size() int64 {
	return int64(len(r.body))
}

func (r *putObjectBodyReader) ForwardBodyBytes() []byte {
	if r.off >= int64(len(r.body)) {
		return nil
	}
	return r.body[r.off:]
}

type exactLengthReader struct {
	r         io.Reader
	remain    int64
	shortRead bool
}

func newExactLengthReader(r io.Reader, n int64) *exactLengthReader {
	return &exactLengthReader{r: r, remain: n}
}

func (r *exactLengthReader) Read(p []byte) (int, error) {
	if r.shortRead {
		return 0, io.ErrUnexpectedEOF
	}
	if r.remain == 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > r.remain {
		p = p[:int(r.remain)]
	}
	n, err := r.r.Read(p)
	r.remain -= int64(n)
	if errors.Is(err, io.EOF) && r.remain > 0 {
		r.shortRead = true
		return n, io.ErrUnexpectedEOF
	}
	if r.remain == 0 && errors.Is(err, io.EOF) {
		return n, nil
	}
	return n, err
}

func putObjectACL(c *app.RequestContext) *uint8 {
	aclHeader := string(c.GetHeader("x-amz-acl"))
	if aclHeader == "" {
		return nil
	}
	parsed := uint8(s3auth.ParseACLHeader(aclHeader))
	return &parsed
}
