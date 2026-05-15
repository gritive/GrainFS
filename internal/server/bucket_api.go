package server

import (
	"context"
	"encoding/xml"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/iam"
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

// issueCreatorGrant issues an explicit Admin grant to the request principal
// on the newly-created bucket. Best-effort: if the proposer is not wired or the
// propose fails, the bucket is still created.
func (s *Server) issueCreatorGrant(ctx context.Context, bucket string) {
	if s.iamProposer == nil {
		return
	}
	principal := iam.PrincipalFromContext(ctx)
	if principal == "" {
		return
	}
	g := iam.Grant{
		SAID:      principal,
		Bucket:    bucket,
		Role:      iam.RoleAdmin,
		CreatedAt: time.Now().UTC(),
		CreatedBy: principal,
	}
	if err := s.iamProposer.ProposeGrantPut(ctx, g); err != nil {
		log.Warn().Err(err).Str("sa", principal).Str("bucket", bucket).
			Msg("iam: failed to issue creator grant; bucket created without explicit grant")
	}
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
		s.deleteBucketPolicy(c, bucket)
		return
	}

	if err := s.deleteS3Bucket(ctx, bucket); err != nil {
		mapError(c, err)
		return
	}
	c.Status(consts.StatusNoContent)
}
