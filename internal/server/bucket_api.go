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
		s.putBucketPolicy(c, bucket)
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

	if err := s.createS3Bucket(ctx, bucket); err != nil {
		mapError(c, err)
		return
	}
	c.Header("Location", "/"+bucket)
	c.Status(consts.StatusOK)
}

// issueCreatorGrant was the legacy auto-grant path (Role/Grant model removed in §2).
// Kept as a no-op so call sites in bucket_mutation_runtime.go compile unchanged;
// bucket ownership is now enforced via policy.Evaluate on subsequent requests.
func (s *Server) issueCreatorGrant(_ context.Context, _ string) {}

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
		s.deleteBucketPolicy(c, bucket)
		return
	}

	if err := s.deleteS3Bucket(ctx, bucket); err != nil {
		mapError(c, err)
		return
	}
	c.Status(consts.StatusNoContent)
}
