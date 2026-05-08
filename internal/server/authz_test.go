package server

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/gritive/GrainFS/internal/s3auth"
)

func TestS3ActionEnum(t *testing.T) {
	tests := []struct {
		method    string
		path      string
		hasKey    bool
		hasPolicy bool
		want      s3auth.S3Action
	}{
		{"GET", "/bucket/key", true, false, s3auth.GetObject},
		{"GET", "/bucket", false, false, s3auth.ListBucket},
		{"HEAD", "/bucket/key", true, false, s3auth.HeadObject},
		{"HEAD", "/bucket", false, false, s3auth.ListBucket},
		{"PUT", "/bucket/key", true, false, s3auth.PutObject},
		{"PUT", "/bucket", false, false, s3auth.CreateBucket},
		{"DELETE", "/bucket/key", true, false, s3auth.DeleteObject},
		{"DELETE", "/bucket", false, false, s3auth.DeleteBucket},
		{"POST", "/bucket/key", true, false, s3auth.PutObject}, // multipart
		{"UNKNOWN", "/bucket", false, false, s3auth.UnknownAction},
		// Phase 5d #4: ?policy CRUD maps to dedicated S3Actions.
		{"GET", "/bucket", false, true, s3auth.GetBucketPolicy},
		{"PUT", "/bucket", false, true, s3auth.PutBucketPolicy},
		{"DELETE", "/bucket", false, true, s3auth.DeleteBucketPolicy},
	}

	for _, tt := range tests {
		t.Run(tt.method+"_hasKey="+boolStr(tt.hasKey)+"_hasPolicy="+boolStr(tt.hasPolicy), func(t *testing.T) {
			got := s3ActionEnum(tt.method, tt.path, tt.hasKey, tt.hasPolicy)
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
