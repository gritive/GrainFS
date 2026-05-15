package server

import (
	"fmt"

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
	if contentEncoding == "aws-chunked" || contentSHA == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" {
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

func putObjectACL(c *app.RequestContext) *uint8 {
	aclHeader := string(c.GetHeader("x-amz-acl"))
	if aclHeader == "" {
		return nil
	}
	parsed := uint8(s3auth.ParseACLHeader(aclHeader))
	return &parsed
}
