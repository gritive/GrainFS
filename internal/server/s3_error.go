package server

import (
	"encoding/xml"
	"errors"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/compat"
	"github.com/gritive/GrainFS/internal/storage"
)

type s3Error struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	RequestID string   `xml:"RequestId,omitempty"`
}

func writeXMLError(c *app.RequestContext, status int, code, message string) {
	c.SetContentType("application/xml")
	data, _ := xml.Marshal(s3Error{
		Code:      code,
		Message:   message,
		RequestID: requestIDFromHertz(c),
	})
	c.Data(status, "application/xml", data)
}

func mapError(c *app.RequestContext, err error) {
	if err != nil {
		c.Set(auditErrReasonKey, err.Error())
	}
	switch {
	case errors.Is(err, storage.ErrPreconditionFailed):
		writeXMLError(c, consts.StatusPreconditionFailed, "PreconditionFailed", err.Error())
	case errors.Is(err, storage.ErrInvalidCopySource):
		writeXMLError(c, consts.StatusBadRequest, "InvalidArgument", err.Error())
	case errors.Is(err, storage.ErrBucketNotFound), errors.Is(err, storage.ErrNoSuchBucket):
		writeXMLError(c, consts.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist")
	case errors.Is(err, storage.ErrBucketAlreadyExists):
		writeXMLError(c, consts.StatusConflict, "BucketAlreadyOwnedByYou", "Your previous request to create the named bucket succeeded")
	case errors.Is(err, storage.ErrBucketNotEmpty):
		writeXMLError(c, consts.StatusConflict, "BucketNotEmpty", "The bucket you tried to delete is not empty")
	case errors.Is(err, storage.ErrObjectNotFound):
		writeXMLError(c, consts.StatusNotFound, "NoSuchKey", "The specified key does not exist")
	case errors.Is(err, storage.ErrUploadNotFound):
		writeXMLError(c, consts.StatusNotFound, "NoSuchUpload", "The specified upload does not exist")
	case errors.Is(err, storage.ErrEntityTooLarge):
		writeXMLError(c, consts.StatusRequestEntityTooLarge, "EntityTooLarge", "Your proposed upload exceeds the maximum allowed object size")
	case errors.Is(err, storage.ErrForwardBackpressure):
		writeXMLError(c, consts.StatusServiceUnavailable, "SlowDown", "too many forwarded upload streams in flight")
	case errors.Is(err, cluster.ErrPlacementTargetsUnavailable):
		writeXMLError(c, consts.StatusServiceUnavailable, "ServiceUnavailable", err.Error())
	case errors.Is(err, compat.ErrCapabilityRejected):
		msg := "finish the cluster rolling upgrade before retrying this S3 operation"
		var gateErr *compat.GateRejectError
		if errors.As(err, &gateErr) {
			msg = gateErr.PublicMessage()
		}
		writeXMLError(c, consts.StatusServiceUnavailable, "ServiceUnavailable", msg)
	case errors.Is(err, storage.ErrUnsupportedOperation):
		writeXMLError(c, consts.StatusNotImplemented, "NotImplemented", err.Error())
	default:
		writeXMLError(c, consts.StatusInternalServerError, "InternalError", err.Error())
	}
}
