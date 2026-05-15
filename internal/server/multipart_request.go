package server

import (
	"encoding/xml"
	"strings"

	"github.com/gritive/GrainFS/internal/storage"
)

func parseCompleteMultipartUpload(body []byte) ([]storage.Part, error) {
	var req completeMultipartUploadRequest
	if err := xml.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	parts := make([]storage.Part, len(req.Parts))
	for i, p := range req.Parts {
		parts[i] = storage.Part{
			PartNumber: p.PartNumber,
			ETag:       strings.Trim(p.ETag, "\""),
		}
	}
	return parts, nil
}
