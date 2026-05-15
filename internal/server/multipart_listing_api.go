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

// listMultipartUploads renders the in-progress multipart uploads for a bucket
// as the S3 ListMultipartUploadsResult XML payload. Honors ?prefix= and
// ?max-uploads= (default 1000). IsTruncated is set conservatively when the
// returned count equals max-uploads.
func (s *Server) listMultipartUploads(ctx context.Context, c *app.RequestContext, bucket string) {
	prefix := string(c.QueryArgs().Peek("prefix"))
	maxUploads := 1000
	if mu := string(c.QueryArgs().Peek("max-uploads")); mu != "" {
		if v, err := strconv.Atoi(mu); err == nil && v > 0 {
			maxUploads = v
		}
	}

	uploads, err := s.listMultipartSessions(ctx, bucket, prefix, maxUploads)
	if err != nil {
		mapError(c, err)
		return
	}

	result := listMultipartUploadsResult{
		Xmlns:       "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket:      bucket,
		Prefix:      prefix,
		MaxUploads:  maxUploads,
		IsTruncated: len(uploads) >= maxUploads,
	}
	for _, up := range uploads {
		result.Uploads = append(result.Uploads, multipartUploadXML{
			Key:       up.Key,
			UploadId:  up.UploadID,
			Initiated: time.Unix(up.CreatedAt, 0).UTC().Format(time.RFC3339),
		})
	}
	data, _ := xml.Marshal(result)
	c.Data(consts.StatusOK, "application/xml", data)
}

// listParts renders the parts uploaded so far for one multipart upload as the
// S3 ListPartsResult XML payload. Honors ?max-parts= (default 1000).
func (s *Server) listParts(ctx context.Context, c *app.RequestContext, bucket, key, uploadID string) {
	maxParts := 1000
	if mp := string(c.QueryArgs().Peek("max-parts")); mp != "" {
		if v, err := strconv.Atoi(mp); err == nil && v > 0 {
			maxParts = v
		}
	}

	parts, err := s.listMultipartParts(ctx, bucket, key, uploadID, maxParts)
	if err != nil {
		mapError(c, err)
		return
	}

	result := listPartsResult{
		Xmlns:       "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket:      bucket,
		Key:         key,
		UploadId:    uploadID,
		MaxParts:    maxParts,
		IsTruncated: len(parts) >= maxParts,
	}
	for _, p := range parts {
		result.Parts = append(result.Parts, partInfoXML{
			PartNumber: p.PartNumber,
			ETag:       fmt.Sprintf("\"%s\"", p.ETag),
			Size:       p.Size,
		})
	}
	data, _ := xml.Marshal(result)
	c.Data(consts.StatusOK, "application/xml", data)
}
