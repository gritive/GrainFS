package server

import (
	"context"
	"encoding/xml"
	"fmt"
	"strconv"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

func (s *Server) listObjects(ctx context.Context, c *app.RequestContext) {
	bucket := c.Param("bucket")

	if c.QueryArgs().Has("lifecycle") {
		s.getBucketLifecycle(ctx, c, bucket)
		return
	}
	if c.QueryArgs().Has("policy") {
		s.getBucketPolicy(c, bucket)
		return
	}
	if c.QueryArgs().Has("versioning") {
		s.getBucketVersioning(ctx, c, bucket)
		return
	}
	if c.QueryArgs().Has("versions") {
		s.listObjectVersions(ctx, c, bucket)
		return
	}
	if c.QueryArgs().Has("uploads") {
		s.listMultipartUploads(ctx, c, bucket)
		return
	}

	prefix := string(c.QueryArgs().Peek("prefix"))
	maxKeys := 1000
	if mk := string(c.QueryArgs().Peek("max-keys")); mk != "" {
		if v, err := strconv.Atoi(mk); err == nil && v > 0 {
			maxKeys = v
		}
	}

	objects, err := s.listBucketObjects(ctx, bucket, prefix, maxKeys)
	if err != nil {
		mapError(c, err)
		return
	}

	result := listObjectsResult{
		Xmlns:   "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:    bucket,
		Prefix:  prefix,
		MaxKeys: maxKeys,
	}
	for _, obj := range objects {
		result.Contents = append(result.Contents, objectResult{
			Key:          obj.Key,
			LastModified: time.Unix(obj.LastModified, 0).UTC().Format(time.RFC3339),
			ETag:         fmt.Sprintf("\"%s\"", obj.ETag),
			Size:         obj.Size,
		})
	}

	data, _ := xml.Marshal(result)
	c.Data(consts.StatusOK, "application/xml", data)
}
