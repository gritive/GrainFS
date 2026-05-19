package server

import (
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/gritive/GrainFS/internal/storage"
)

func parseCopySource(raw string) (storage.ObjectRef, bool) {
	raw = strings.TrimPrefix(raw, "/")
	u, err := url.Parse(raw)
	if err != nil {
		return storage.ObjectRef{}, false
	}
	path, err := url.PathUnescape(u.Path)
	if err != nil {
		return storage.ObjectRef{}, false
	}
	parts := strings.SplitN(path, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return storage.ObjectRef{}, false
	}
	return storage.ObjectRef{Bucket: parts[0], Key: parts[1], VersionID: u.Query().Get("versionId")}, true
}

func parseCopyMetadataDirective(raw string) (storage.CopyMetadataDirective, bool) {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "", string(storage.CopyMetadataCopy):
		return storage.CopyMetadataCopy, true
	case string(storage.CopyMetadataReplace):
		return storage.CopyMetadataReplace, true
	default:
		return "", false
	}
}

func parseCopyTaggingDirective(raw string) (storage.TaggingDirective, bool) {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "", "COPY":
		return storage.TaggingDirectiveCopy, true
	case "REPLACE":
		return storage.TaggingDirectiveReplace, true
	default:
		return storage.TaggingDirectiveCopy, false
	}
}

func copyUserMetadata(c *app.RequestContext) map[string]string {
	var metadata map[string]string
	c.Request.Header.VisitAll(func(k, v []byte) {
		key := strings.ToLower(string(k))
		if !strings.HasPrefix(key, "x-amz-meta-") {
			return
		}
		if metadata == nil {
			metadata = make(map[string]string)
		}
		metadata[key] = string(v)
	})
	return metadata
}

func writeUserMetadataHeaders(c *app.RequestContext, metadata map[string]string) {
	for k, v := range metadata {
		if strings.HasPrefix(strings.ToLower(k), "x-amz-meta-") {
			c.Header(k, v)
		}
	}
}

func copyPreconditions(c *app.RequestContext) (storage.CopyPreconditions, bool) {
	modifiedSince, ok := parseOptionalHTTPTime(string(c.GetHeader("x-amz-copy-source-if-modified-since")))
	if !ok {
		return storage.CopyPreconditions{}, false
	}
	unmodifiedSince, ok := parseOptionalHTTPTime(string(c.GetHeader("x-amz-copy-source-if-unmodified-since")))
	if !ok {
		return storage.CopyPreconditions{}, false
	}
	return storage.CopyPreconditions{
		IfMatch:           string(c.GetHeader("x-amz-copy-source-if-match")),
		IfNoneMatch:       string(c.GetHeader("x-amz-copy-source-if-none-match")),
		IfModifiedSince:   modifiedSince,
		IfUnmodifiedSince: unmodifiedSince,
	}, true
}

func parseOptionalHTTPTime(raw string) (*time.Time, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, true
	}
	t, err := http.ParseTime(raw)
	if err != nil {
		return nil, false
	}
	return &t, true
}
