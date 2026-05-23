package server

import (
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/gritive/GrainFS/internal/s3auth"
)

// putObjectContentMD5Hex decodes the client-supplied Content-MD5 header
// (base64-encoded per RFC 1864) to hex so the actor PUT pipeline can use
// it as the object ETag without recomputing MD5. Returns "" if the
// header is absent or malformed; the pipeline falls back to recompute.
func putObjectContentMD5Hex(c *app.RequestContext) string {
	raw := string(c.GetHeader("Content-MD5"))
	if raw == "" {
		return ""
	}
	decoded, err := base64.StdEncoding.DecodeString(raw)
	if err != nil || len(decoded) != 16 {
		return ""
	}
	return hex.EncodeToString(decoded)
}

const putObjectStreamingThresholdBytes = 8 << 20

func putObjectContentType(c *app.RequestContext) string {
	if contentType := string(c.GetHeader("Content-Type")); contentType != "" {
		return contentType
	}
	return "application/octet-stream"
}

func putObjectShouldStream(c *app.RequestContext) bool {
	contentLength := c.Request.Header.ContentLength()
	return c.Request.IsBodyStream() &&
		contentLength >= putObjectStreamingThresholdBytes &&
		!isAWSChunkedPayload(c)
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
