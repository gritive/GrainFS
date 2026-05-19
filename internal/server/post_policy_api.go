package server

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/storage/tagging"
)

// handleFormUpload processes S3 POST form-based uploads (browser direct upload).
// The form contains: key, Content-Type, policy, X-Amz-Signature, file, etc.
func (s *Server) handleFormUpload(ctx context.Context, c *app.RequestContext, bucket string) {
	form, err := c.MultipartForm()
	if err != nil {
		writeXMLError(c, consts.StatusBadRequest, "MalformedPOSTRequest", "cannot parse multipart form")
		return
	}

	keys := form.Value["key"]
	if len(keys) == 0 {
		writeXMLError(c, consts.StatusBadRequest, "InvalidArgument", "missing 'key' form field")
		return
	}
	key := keys[0]
	c.Set(auditObjectKeyKey, key)

	if !s.validateFormUploadPolicyIfConfigured(c, form.Value, bucket, key) {
		return
	}

	contentType := "application/octet-stream"
	if cts := form.Value["Content-Type"]; len(cts) > 0 {
		contentType = cts[0]
	}

	// S3 POST policy: tagging field contains XML (not URL-encoded key=value).
	var formTags []storage.Tag
	if tagFields := form.Value["tagging"]; len(tagFields) > 0 && tagFields[0] != "" {
		parsed, err := ParseTaggingXML([]byte(tagFields[0]))
		if err != nil {
			writeXMLError(c, consts.StatusBadRequest, "MalformedXML", err.Error())
			return
		}
		if err := tagging.Validate(parsed); err != nil {
			writeXMLError(c, consts.StatusBadRequest, "InvalidTag", err.Error())
			return
		}
		formTags = parsed
	}

	files := form.File["file"]
	if len(files) == 0 {
		writeXMLError(c, consts.StatusBadRequest, "InvalidArgument", "missing 'file' form field")
		return
	}

	file, err := files[0].Open()
	if err != nil {
		writeXMLError(c, consts.StatusInternalServerError, "InternalError", "cannot open uploaded file")
		return
	}
	defer file.Close()

	result, err := s.putFormObject(ctx, bucket, key, file, contentType)
	if err != nil {
		mapError(c, err)
		return
	}
	obj := result.Object

	if len(formTags) > 0 {
		if err := s.ops.SetObjectTags(bucket, key, obj.VersionID, formTags); err != nil {
			mapError(c, err)
			return
		}
	}

	if obj.VersionID != "" {
		c.Header("X-Amz-Version-Id", obj.VersionID)
	}

	writeFormUploadSuccess(c, form.Value, bucket, key, obj.ETag)
}
