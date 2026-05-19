package server

import (
	"context"
	"encoding/xml"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

func (s *Server) listBuckets(ctx context.Context, c *app.RequestContext) {
	buckets, err := s.visibleBuckets(ctx)
	if err != nil {
		mapError(c, err)
		return
	}

	result := listBucketsResult{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/"}
	for _, name := range buckets {
		result.Buckets = append(result.Buckets, bucketResult{
			Name:         name,
			CreationDate: time.Now().UTC().Format(time.RFC3339),
		})
	}

	data, _ := xml.Marshal(result)
	c.Data(consts.StatusOK, "application/xml", data)
}

func (s *Server) createBucket(ctx context.Context, c *app.RequestContext) {
	bucket := c.Param("bucket")

	if c.QueryArgs().Has("policy") {
		// D#8: bucket-policy mutation is admin-UDS-only on the data plane.
		writeXMLError(c, consts.StatusForbidden, "AccessDenied", "Bucket policy is admin-UDS-only (D#8)")
		return
	}
	if c.QueryArgs().Has("lifecycle") {
		s.putBucketLifecycle(ctx, c, bucket)
		return
	}
	if c.QueryArgs().Has("versioning") {
		s.putBucketVersioning(c, bucket)
		return
	}

	// D#8: bucket creation is admin-UDS-only on the data plane.
	writeXMLError(c, consts.StatusForbidden, "AccessDenied", "Bucket lifecycle is admin-UDS-only (D#8)")
}

func (s *Server) headBucket(ctx context.Context, c *app.RequestContext) {
	bucket := c.Param("bucket")
	if err := s.requireBucket(ctx, bucket); err != nil {
		mapError(c, err)
		return
	}
	c.Status(consts.StatusOK)
}

func (s *Server) deleteBucket(ctx context.Context, c *app.RequestContext) {
	bucket := c.Param("bucket")

	if c.QueryArgs().Has("lifecycle") {
		s.deleteBucketLifecycle(ctx, c, bucket)
		return
	}
	if c.QueryArgs().Has("policy") {
		// D#8: bucket-policy mutation is admin-UDS-only on the data plane.
		writeXMLError(c, consts.StatusForbidden, "AccessDenied", "Bucket policy is admin-UDS-only (D#8)")
		return
	}

	// D#8: bucket deletion is admin-UDS-only on the data plane.
	writeXMLError(c, consts.StatusForbidden, "AccessDenied", "Bucket lifecycle is admin-UDS-only (D#8)")
}
