package server

import (
	"context"
	"encoding/xml"
	"errors"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/storage"
)

type deleteObjectsRequest struct {
	Objects []deleteObjectsRequestObject `xml:"Object"`
	Quiet   bool                         `xml:"Quiet"`
}

type deleteObjectsRequestObject struct {
	Key string `xml:"Key"`
}

type deleteObjectsResult struct {
	XMLName xml.Name               `xml:"DeleteResult"`
	Deleted []deleteObjectsDeleted `xml:"Deleted,omitempty"`
	Errors  []deleteObjectsError   `xml:"Error,omitempty"`
}

type deleteObjectsDeleted struct {
	Key string `xml:"Key"`
}

type deleteObjectsError struct {
	Key     string `xml:"Key"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

func (s *Server) deleteObjects(ctx context.Context, c *app.RequestContext, bucket string) {
	var req deleteObjectsRequest
	if err := xml.Unmarshal(c.Request.Body(), &req); err != nil {
		writeXMLError(c, consts.StatusBadRequest, "MalformedXML", "invalid XML body")
		return
	}

	result := deleteObjectsResult{}
	for _, obj := range req.Objects {
		if obj.Key == "" {
			result.Errors = append(result.Errors, deleteObjectsError{
				Code:    "InvalidArgument",
				Message: "object key is required",
			})
			continue
		}

		_, err := s.deleteObjectWithMutation(ctx, bucket, obj.Key)
		switch {
		case err == nil, errors.Is(err, storage.ErrObjectNotFound):
			if !req.Quiet {
				result.Deleted = append(result.Deleted, deleteObjectsDeleted{Key: obj.Key})
			}
		case errors.Is(err, storage.ErrBucketNotFound):
			mapError(c, err)
			return
		default:
			result.Errors = append(result.Errors, deleteObjectsError{
				Key:     obj.Key,
				Code:    "InternalError",
				Message: err.Error(),
			})
		}
	}

	data, err := xml.Marshal(result)
	if err != nil {
		writeXMLError(c, consts.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	c.Data(consts.StatusOK, "application/xml", data)
}
