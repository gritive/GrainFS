package audit

import (
	"net/http"
	"strings"
)

type OperationInfo struct {
	Operation   string
	Subresource string
}

func ClassifyS3Operation(method string, hasKey bool, rawQuery string, h http.Header) OperationInfo {
	has := func(name string) bool {
		if rawQuery == name || strings.HasPrefix(rawQuery, name+"&") {
			return true
		}
		return strings.Contains(rawQuery, "&"+name) || strings.Contains(rawQuery, name+"=")
	}
	if method == "PUT" && hasKey && h.Get("x-amz-copy-source") != "" {
		return OperationInfo{Operation: "CopyObject"}
	}
	if has("policy") {
		switch method {
		case "GET":
			return OperationInfo{Operation: "GetBucketPolicy", Subresource: "policy"}
		case "PUT":
			return OperationInfo{Operation: "PutBucketPolicy", Subresource: "policy"}
		case "DELETE":
			return OperationInfo{Operation: "DeleteBucketPolicy", Subresource: "policy"}
		}
	}
	if has("versioning") {
		return OperationInfo{Operation: methodTitle(method) + "BucketVersioning", Subresource: "versioning"}
	}
	if has("lifecycle") {
		return OperationInfo{Operation: methodTitle(method) + "BucketLifecycle", Subresource: "lifecycle"}
	}
	if has("versions") {
		return OperationInfo{Operation: "ListObjectVersions", Subresource: "versions"}
	}
	if method == "GET" && !hasKey && has("uploads") {
		return OperationInfo{Operation: "ListMultipartUploads", Subresource: "uploads"}
	}
	if method == "POST" && hasKey && has("uploads") {
		return OperationInfo{Operation: "CreateMultipartUpload", Subresource: "uploads"}
	}
	if has("uploadId") {
		switch method {
		case "PUT":
			return OperationInfo{Operation: "UploadPart", Subresource: "uploadId"}
		case "POST":
			return OperationInfo{Operation: "CompleteMultipartUpload", Subresource: "uploadId"}
		case "DELETE":
			return OperationInfo{Operation: "AbortMultipartUpload", Subresource: "uploadId"}
		case "GET":
			return OperationInfo{Operation: "ListParts", Subresource: "uploadId"}
		}
	}
	if !hasKey {
		switch method {
		case "GET":
			return OperationInfo{Operation: "ListObjects"}
		case "HEAD":
			return OperationInfo{Operation: "HeadBucket"}
		case "PUT":
			return OperationInfo{Operation: "CreateBucket"}
		case "DELETE":
			return OperationInfo{Operation: "DeleteBucket"}
		}
	}
	switch method {
	case "GET":
		return OperationInfo{Operation: "GetObject"}
	case "HEAD":
		return OperationInfo{Operation: "HeadObject"}
	case "PUT":
		return OperationInfo{Operation: "PutObject"}
	case "DELETE":
		return OperationInfo{Operation: "DeleteObject"}
	case "POST":
		return OperationInfo{Operation: "PostObject"}
	default:
		return OperationInfo{Operation: "Unknown"}
	}
}

func methodTitle(method string) string {
	switch method {
	case "GET":
		return "Get"
	case "PUT":
		return "Put"
	case "DELETE":
		return "Delete"
	default:
		return "Unknown"
	}
}
