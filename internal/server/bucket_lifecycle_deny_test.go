package server

import (
	"encoding/xml"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBucketLifecycleDeny verifies that S3 data-plane bucket-lifecycle actions
// return 403 AccessDenied unconditionally (D#8). The admin UDS path is unaffected.
// Server is started without SigV4 auth (verifier==nil) to simulate the worst
// case: an anonymous request that reaches the handler without being intercepted
// by authMiddleware.
func TestBucketLifecycleDeny(t *testing.T) {
	base := setupTestServer(t) // no WithAuth → verifier nil → no authMiddleware

	tests := []struct {
		name   string
		method string
		url    string
	}{
		{"CreateBucket", http.MethodPut, base + "/newbucket"},
		{"DeleteBucket", http.MethodDelete, base + "/newbucket"},
		{"PutBucketPolicy", http.MethodPut, base + "/newbucket?policy"},
		{"DeleteBucketPolicy", http.MethodDelete, base + "/newbucket?policy"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(tt.method, tt.url, nil)
			require.NoError(t, err)

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			body, _ := io.ReadAll(resp.Body)

			assert.Equal(t, http.StatusForbidden, resp.StatusCode,
				"%s: expected 403, body: %s", tt.name, string(body))

			var s3Err s3Error
			require.NoError(t, xml.Unmarshal(body, &s3Err),
				"%s: response is not valid S3 XML: %s", tt.name, string(body))
			assert.Equal(t, "AccessDenied", s3Err.Code,
				"%s: expected AccessDenied code", tt.name)
		})
	}
}
