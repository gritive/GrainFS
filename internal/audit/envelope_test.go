package audit_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/audit"
)

func TestClassifyS3Operation(t *testing.T) {
	tests := []struct {
		method  string
		hasKey  bool
		query   string
		header  http.Header
		wantOp  string
		wantSub string
	}{
		{method: "PUT", hasKey: false, wantOp: "CreateBucket"},
		{method: "DELETE", hasKey: false, wantOp: "DeleteBucket"},
		{method: "GET", hasKey: false, wantOp: "ListObjects"},
		{method: "GET", hasKey: false, query: "policy", wantOp: "GetBucketPolicy", wantSub: "policy"},
		{method: "PUT", hasKey: false, query: "policy", wantOp: "PutBucketPolicy", wantSub: "policy"},
		{method: "GET", hasKey: false, query: "uploads", wantOp: "ListMultipartUploads", wantSub: "uploads"},
		{method: "GET", hasKey: true, wantOp: "GetObject"},
		{method: "HEAD", hasKey: true, wantOp: "HeadObject"},
		{method: "PUT", hasKey: true, wantOp: "PutObject"},
		{method: "PUT", hasKey: true, query: "uploadId=u&partNumber=1", wantOp: "UploadPart", wantSub: "uploadId"},
		{method: "POST", hasKey: true, query: "uploads", wantOp: "CreateMultipartUpload", wantSub: "uploads"},
		{method: "POST", hasKey: true, query: "uploadId=u", wantOp: "CompleteMultipartUpload", wantSub: "uploadId"},
		{method: "DELETE", hasKey: true, query: "uploadId=u", wantOp: "AbortMultipartUpload", wantSub: "uploadId"},
	}
	for _, tt := range tests {
		got := audit.ClassifyS3Operation(tt.method, tt.hasKey, tt.query, tt.header)
		require.Equal(t, tt.wantOp, got.Operation)
		require.Equal(t, tt.wantSub, got.Subresource)
	}
}

func TestClassifyS3OperationCopyObject(t *testing.T) {
	h := http.Header{}
	h.Set("x-amz-copy-source", "/src/k.txt")
	got := audit.ClassifyS3Operation("PUT", true, "", h)
	require.Equal(t, "CopyObject", got.Operation)
}
