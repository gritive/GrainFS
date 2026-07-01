package server

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/internal/storage"
)

// uploadPartCopy handles UploadPartCopy (PUT /b/k?partNumber=N&uploadId=ID with
// an x-amz-copy-source header): a multipart part whose bytes are server-side
// copied from an existing source object instead of read from the request body.
//
// It mirrors handleCopyObject for the source side (full GetObject authorization
// chain + source load) and uploadPart for the part side (write via
// uploadMultipartPart). The source bytes stream through this coordinator node;
// the destination part write reuses the tested UploadPart path, so no new
// cluster command or backend method is needed.
func (s *Server) uploadPartCopy(ctx context.Context, c *app.RequestContext, dstBucket, dstKey, uploadID, partNumberStr, copySource string) {
	partNumber, err := strconv.Atoi(partNumberStr)
	if err != nil || partNumber < 1 {
		writeXMLError(c, consts.StatusBadRequest, "InvalidArgument", "invalid part number")
		return
	}

	src, ok := parseCopySource(copySource)
	if !ok {
		writeXMLError(c, consts.StatusBadRequest, "InvalidArgument", "invalid x-amz-copy-source format")
		return
	}

	// Source GetObject authorization chain (mirror handleCopyObject). The
	// middleware gated the destination (PUT) at PhasePreLoad; the source needs
	// its own pre-load (IAM + bucket policy) before we touch storage so an
	// unauthorized caller cannot probe object existence, then post-load (ACL)
	// after the source object is loaded. Omitting any link here would turn
	// UploadPartCopy into an unauthenticated cross-bucket read primitive.
	if s.mustAuthorize(ctx, c, src.Bucket, src.Key, s3auth.GetObject) {
		return
	}
	srcMeta, srcErr := s.loadCopySourceObject(ctx, src)
	if srcErr != nil {
		mapError(c, srcErr)
		return
	}
	if s.mustAuthorizePostLoad(ctx, c, src.Bucket, src.Key, s3auth.GetObject, srcMeta.ACL) {
		return
	}

	// Read the source bytes. Stamp the SOURCE bucket's versioning decision so a
	// per-version source read activates on forwarded/leaf data-group nodes
	// (mirror copyObjectWithMutation's SourceVersioningCtx).
	srcCtx := s.ctxWithBucketVersioning(ctx, src.Bucket)
	var (
		rc     io.ReadCloser
		srcObj *storage.Object
	)
	if src.VersionID != "" {
		rc, srcObj, err = s.ops.GetObjectVersion(srcCtx, src.Bucket, src.Key, src.VersionID)
	} else {
		rc, srcObj, err = s.ops.GetObject(srcCtx, src.Bucket, src.Key)
	}
	if err != nil {
		mapError(c, err)
		return
	}
	defer rc.Close()

	// Apply x-amz-copy-source-range (inclusive, 0-indexed) when present. The
	// returned srcObj is version-accurate, so validate the range against its
	// Size (not the latest-only loadCopySourceObject head).
	reader := io.Reader(rc)
	if rangeHeader := string(c.GetHeader("x-amz-copy-source-range")); rangeHeader != "" {
		start, end, ok := parseCopySourceRange(rangeHeader, srcObj.Size)
		if !ok {
			writeXMLError(c, consts.StatusBadRequest, "InvalidArgument", "invalid x-amz-copy-source-range")
			return
		}
		// ops.GetObject is not seekable, so skip the prefix by discarding it.
		// Accepted tradeoff: range reads the full prefix then discards — a
		// correctness fix, not a perf path.
		if start > 0 {
			if _, derr := io.CopyN(io.Discard, rc, start); derr != nil {
				mapError(c, fmt.Errorf("seek copy-source-range start: %w", derr))
				return
			}
		}
		reader = io.LimitReader(rc, end-start+1)
	}

	// Write the part via the tested UploadPart path. Empty contentMD5Hex:
	// UploadPartCopy carries no request-body Content-MD5.
	part, err := s.uploadMultipartPart(ctx, dstBucket, dstKey, uploadID, partNumber, reader, "")
	if err != nil {
		mapError(c, err)
		return
	}

	// Respond with CopyPartResult XML (NOT a bare ETag header): the SDK reads
	// the part ETag from this body. LastModified comes from the version-accurate
	// source object.
	response := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<CopyPartResult>
  <ETag>"%s"</ETag>
  <LastModified>%s</LastModified>
</CopyPartResult>`, part.ETag, time.Unix(srcObj.LastModified, 0).UTC().Format(time.RFC3339))
	c.Data(consts.StatusOK, "application/xml", []byte(response))
}

// parseCopySourceRange parses an x-amz-copy-source-range header of the form
// "bytes=START-END" (inclusive, 0-indexed) and validates it against the source
// object size. Returns (start, end, true) on success. A malformed or
// out-of-range header (not 0 <= start <= end < size) returns ok=false.
func parseCopySourceRange(raw string, size int64) (start, end int64, ok bool) {
	const prefix = "bytes="
	if !strings.HasPrefix(raw, prefix) {
		return 0, 0, false
	}
	spec := strings.TrimPrefix(raw, prefix)
	dash := strings.IndexByte(spec, '-')
	if dash < 0 {
		return 0, 0, false
	}
	start, err := strconv.ParseInt(spec[:dash], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	end, err = strconv.ParseInt(spec[dash+1:], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	if start < 0 || start > end || end >= size {
		return 0, 0, false
	}
	return start, end, true
}
