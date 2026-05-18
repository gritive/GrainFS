package server

import (
	"context"
	"encoding/base64"
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
	if c.QueryArgs().Has("location") {
		s.getBucketLocation(ctx, c, bucket)
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

	// ListObjectsV2 is opted in via ?list-type=2. minio-go and the AWS SDKs
	// default to V2; V1 stays available for the handful of legacy clients
	// that still send Marker pagination instead of ContinuationToken.
	isV2 := string(c.QueryArgs().Peek("list-type")) == "2"
	var marker, startAfter, continuationToken string
	if isV2 {
		continuationToken = string(c.QueryArgs().Peek("continuation-token"))
		startAfter = string(c.QueryArgs().Peek("start-after"))
		// ContinuationToken is opaque to the client; we encode the resume
		// point as base64(lastKey) on response and decode it here.
		if continuationToken != "" {
			if decoded, err := base64.StdEncoding.DecodeString(continuationToken); err == nil {
				marker = string(decoded)
			}
		}
		if marker == "" && startAfter != "" {
			// start-after acts as an exclusive marker for the first page.
			marker = startAfter
		}
	} else {
		marker = string(c.QueryArgs().Peek("marker"))
	}

	objects, truncated, err := s.listBucketObjectsPage(ctx, bucket, prefix, marker, maxKeys)
	if err != nil {
		mapError(c, err)
		return
	}

	result := listObjectsResult{
		Xmlns:       "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:        bucket,
		Prefix:      prefix,
		MaxKeys:     maxKeys,
		IsTruncated: truncated,
	}
	if isV2 {
		result.KeyCount = len(objects)
		result.ContinuationToken = continuationToken
		result.StartAfter = startAfter
	} else {
		result.Marker = marker
	}
	for _, obj := range objects {
		result.Contents = append(result.Contents, objectResult{
			Key:          obj.Key,
			LastModified: time.Unix(obj.LastModified, 0).UTC().Format(time.RFC3339),
			ETag:         fmt.Sprintf("\"%s\"", obj.ETag),
			Size:         obj.Size,
		})
	}
	if truncated && len(result.Contents) > 0 {
		lastKey := result.Contents[len(result.Contents)-1].Key
		if isV2 {
			result.NextContinuationToken = base64.StdEncoding.EncodeToString([]byte(lastKey))
		} else {
			result.NextMarker = lastKey
		}
	}

	data, _ := xml.Marshal(result)
	c.Data(consts.StatusOK, "application/xml", data)
}

func (s *Server) getBucketLocation(ctx context.Context, c *app.RequestContext, bucket string) {
	if err := s.requireBucket(ctx, bucket); err != nil {
		mapError(c, err)
		return
	}

	data, _ := xml.Marshal(bucketLocationResult{
		Xmlns:              "http://s3.amazonaws.com/doc/2006-03-01/",
		LocationConstraint: "us-east-1",
	})
	c.Data(consts.StatusOK, "application/xml", data)
}
