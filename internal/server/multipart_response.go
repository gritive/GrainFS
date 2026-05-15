package server

import (
	"encoding/xml"
	"fmt"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/storage"
)

func writeInitiateMultipartUploadResult(c *app.RequestContext, bucket, key string, upload *storage.MultipartUpload) {
	result := initiateMultipartUploadResult{
		Xmlns:    "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket:   bucket,
		Key:      key,
		UploadId: upload.UploadID,
	}
	data, _ := xml.Marshal(result)
	c.Data(consts.StatusOK, "application/xml", data)
}

func writeCompleteMultipartUploadResult(c *app.RequestContext, bucket, key string, obj storage.ObjectFacts) {
	if obj.VersionID != "" {
		c.Header("X-Amz-Version-Id", obj.VersionID)
	}
	response := completeMultipartUploadResult{
		Xmlns:  "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket: bucket,
		Key:    key,
		ETag:   fmt.Sprintf("\"%s\"", obj.ETag),
	}
	data, _ := xml.Marshal(response)
	c.Data(consts.StatusOK, "application/xml", data)
}
