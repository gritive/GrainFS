package server

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/gritive/GrainFS/internal/s3auth"
)

func TestS3ActionEnum(t *testing.T) {
	tests := []struct {
		method string
		path   string
		hasKey bool
		want   s3auth.S3Action
	}{
		{"GET", "/bucket/key", true, s3auth.GetObject},
		{"GET", "/bucket", false, s3auth.ListBucket},
		{"HEAD", "/bucket/key", true, s3auth.HeadObject},
		{"HEAD", "/bucket", false, s3auth.ListBucket},
		{"PUT", "/bucket/key", true, s3auth.PutObject},
		{"PUT", "/bucket", false, s3auth.CreateBucket},
		{"DELETE", "/bucket/key", true, s3auth.DeleteObject},
		{"DELETE", "/bucket", false, s3auth.DeleteBucket},
		{"POST", "/bucket/key", true, s3auth.PutObject}, // multipart
		{"UNKNOWN", "/bucket", false, s3auth.UnknownAction},
	}

	for _, tt := range tests {
		t.Run(tt.method+"_hasKey="+boolStr(tt.hasKey), func(t *testing.T) {
			got := s3ActionEnum(tt.method, tt.path, tt.hasKey)
			assert.Equal(t, tt.want, got)
		})
	}
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
