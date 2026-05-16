package server

import (
	"fmt"
	"strings"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/storage"
)

const (
	sseHeader = "x-amz-server-side-encryption"
	sseAES256 = "AES256"
)

type sseHeaderError struct {
	status  int
	code    string
	message string
}

func (e sseHeaderError) Error() string {
	return e.message
}

func parseObjectSSEHeaders(c *app.RequestContext) (storage.ObjectSystemMetadata, *sseHeaderError) {
	if hasAnyHeader(c,
		"x-amz-server-side-encryption-customer-algorithm",
		"x-amz-server-side-encryption-customer-key",
		"x-amz-server-side-encryption-customer-key-md5",
		"x-amz-copy-source-server-side-encryption-customer-algorithm",
		"x-amz-copy-source-server-side-encryption-customer-key",
		"x-amz-copy-source-server-side-encryption-customer-key-md5",
	) {
		return storage.ObjectSystemMetadata{}, unsupportedSSEHeader("SSE-C headers are not supported")
	}
	if hasAnyHeader(c,
		"x-amz-server-side-encryption-aws-kms-key-id",
		"x-amz-server-side-encryption-context",
		"x-amz-server-side-encryption-bucket-key-enabled",
	) {
		return storage.ObjectSystemMetadata{}, unsupportedSSEHeader("SSE-KMS headers are not supported")
	}

	algorithm := strings.TrimSpace(string(c.GetHeader(sseHeader)))
	switch algorithm {
	case "":
		return storage.ObjectSystemMetadata{}, nil
	case sseAES256:
		return storage.ObjectSystemMetadata{SSEAlgorithm: sseAES256}, nil
	case "aws:kms":
		return storage.ObjectSystemMetadata{}, unsupportedSSEHeader("SSE-KMS headers are not supported")
	default:
		return storage.ObjectSystemMetadata{}, &sseHeaderError{
			status:  consts.StatusBadRequest,
			code:    "InvalidArgument",
			message: fmt.Sprintf("unsupported %s value %q", sseHeader, algorithm),
		}
	}
}

func unsupportedSSEHeader(message string) *sseHeaderError {
	return &sseHeaderError{
		status:  consts.StatusNotImplemented,
		code:    "NotImplemented",
		message: message,
	}
}

func hasAnyHeader(c *app.RequestContext, names ...string) bool {
	for _, name := range names {
		if len(c.GetHeader(name)) > 0 {
			return true
		}
	}
	return false
}

func writeSSEHeaderError(c *app.RequestContext, err *sseHeaderError) {
	writeXMLError(c, err.status, err.code, err.message)
}

func writeSSEAlgorithmHeader(c *app.RequestContext, algorithm string) {
	if algorithm != "" {
		c.Header(sseHeader, algorithm)
	}
}

func writeSSEObjectHeaders(c *app.RequestContext, obj *storage.Object) {
	if obj != nil {
		writeSSEAlgorithmHeader(c, obj.SSEAlgorithm)
	}
}
