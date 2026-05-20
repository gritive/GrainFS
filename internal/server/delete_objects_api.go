package server

import (
	"context"
	"encoding/xml"
	"errors"
	"runtime"
	"sync"

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

const maxDeleteObjectsConcurrency = 16

func (s *Server) deleteObjects(ctx context.Context, c *app.RequestContext, bucket string) {
	var req deleteObjectsRequest
	if err := xml.Unmarshal(c.Request.Body(), &req); err != nil {
		writeXMLError(c, consts.StatusBadRequest, "MalformedXML", "invalid XML body")
		return
	}

	result := deleteObjectsResult{}
	outcomes := s.deleteObjectsBatch(ctx, bucket, req.Objects)
	for i, obj := range req.Objects {
		outcome := outcomes[i]
		if obj.Key == "" {
			result.Errors = append(result.Errors, deleteObjectsError{
				Code:    "InvalidArgument",
				Message: "object key is required",
			})
			continue
		}

		switch {
		case outcome.err == nil, errors.Is(outcome.err, storage.ErrObjectNotFound):
			if !req.Quiet {
				result.Deleted = append(result.Deleted, deleteObjectsDeleted(obj))
			}
		case errors.Is(outcome.err, storage.ErrBucketNotFound):
			mapError(c, outcome.err)
			return
		default:
			result.Errors = append(result.Errors, deleteObjectsError{
				Key:     obj.Key,
				Code:    "InternalError",
				Message: outcome.err.Error(),
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

type deleteObjectsOutcome struct {
	err error
}

func (s *Server) deleteObjectsBatch(ctx context.Context, bucket string, objects []deleteObjectsRequestObject) []deleteObjectsOutcome {
	outcomes := make([]deleteObjectsOutcome, len(objects))
	if len(objects) == 0 {
		return outcomes
	}
	workers := min(maxDeleteObjectsConcurrency, runtime.GOMAXPROCS(0), len(objects))
	if workers <= 1 {
		for i, obj := range objects {
			if obj.Key != "" {
				_, outcomes[i].err = s.deleteObjectWithMutation(ctx, bucket, obj.Key)
			}
		}
		return outcomes
	}

	jobs := make(chan int)
	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for i := range jobs {
				obj := objects[i]
				if obj.Key == "" {
					continue
				}
				_, outcomes[i].err = s.deleteObjectWithMutation(ctx, bucket, obj.Key)
			}
		}()
	}
	for i := range objects {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	return outcomes
}
