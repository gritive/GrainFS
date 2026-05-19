package server

import (
	"fmt"
	"io"
	"strings"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/gritive/GrainFS/internal/s3auth"
)

func putObjectContentType(c *app.RequestContext) string {
	if contentType := string(c.GetHeader("Content-Type")); contentType != "" {
		return contentType
	}
	return "application/octet-stream"
}

func putObjectBody(c *app.RequestContext) ([]byte, error) {
	rawBody := c.Request.Body()
	contentEncoding := string(c.GetHeader("Content-Encoding"))
	contentSHA := string(c.GetHeader("X-Amz-Content-Sha256"))
	decodedContentLength := string(c.GetHeader("X-Amz-Decoded-Content-Length"))
	trailer := string(c.GetHeader("X-Amz-Trailer"))
	if contentEncoding == "aws-chunked" ||
		strings.HasPrefix(contentSHA, "STREAMING-") ||
		decodedContentLength != "" ||
		trailer != "" {
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

func putObjectACL(c *app.RequestContext) *uint8 {
	aclHeader := string(c.GetHeader("x-amz-acl"))
	if aclHeader == "" {
		return nil
	}
	parsed := uint8(s3auth.ParseACLHeader(aclHeader))
	return &parsed
}
