package s3auth

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVerifyValidSignature(t *testing.T) {
	v := NewVerifier([]Credentials{
		{AccessKey: "AKID", SecretKey: "SECRET"},
	})

	req, _ := http.NewRequest(http.MethodGet, "http://localhost:9000/test-bucket", nil)
	req.Host = "localhost:9000"
	SignRequest(req, "AKID", "SECRET", "us-east-1")

	accessKey, err := v.Verify(req)
	require.NoError(t, err, "Verify")
	assert.Equal(t, "AKID", accessKey)
}

func TestVerifyInvalidSignature(t *testing.T) {
	v := NewVerifier([]Credentials{
		{AccessKey: "AKID", SecretKey: "SECRET"},
	})

	req, _ := http.NewRequest(http.MethodGet, "http://localhost:9000/test-bucket", nil)
	req.Host = "localhost:9000"
	SignRequest(req, "AKID", "WRONG_SECRET", "us-east-1")

	_, err := v.Verify(req)
	assert.Error(t, err, "expected error for wrong secret")
}

func TestVerifyMissingAuth(t *testing.T) {
	v := NewVerifier([]Credentials{
		{AccessKey: "AKID", SecretKey: "SECRET"},
	})

	req, _ := http.NewRequest(http.MethodGet, "http://localhost:9000/test-bucket", nil)

	_, err := v.Verify(req)
	assert.Error(t, err, "expected error for missing auth")
}

func TestVerifyUnknownAccessKey(t *testing.T) {
	v := NewVerifier([]Credentials{
		{AccessKey: "AKID", SecretKey: "SECRET"},
	})

	req, _ := http.NewRequest(http.MethodGet, "http://localhost:9000/test-bucket", nil)
	req.Host = "localhost:9000"
	SignRequest(req, "UNKNOWN", "SECRET", "us-east-1")

	_, err := v.Verify(req)
	assert.Error(t, err, "expected error for unknown access key")
}

func TestVerifyPutRequest(t *testing.T) {
	v := NewVerifier([]Credentials{
		{AccessKey: "mykey", SecretKey: "mysecret"},
	})

	req, _ := http.NewRequest(http.MethodPut, "http://localhost:9000/bucket/object.txt", nil)
	req.Host = "localhost:9000"
	SignRequest(req, "mykey", "mysecret", "us-east-1")

	accessKey, err := v.Verify(req)
	require.NoError(t, err, "Verify PUT")
	assert.Equal(t, "mykey", accessKey)
}

func TestCanonicalRequestUsesEscapedPath(t *testing.T) {
	req, err := http.NewRequest(http.MethodPut, "http://localhost:9000/bucket/prefix/1.name%29.rnd", nil)
	require.NoError(t, err)
	req.Host = "localhost:9000"
	req.Header.Set("X-Amz-Date", "20260515T160252Z")
	req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")

	canonical := buildCanonicalRequest(req, "host;x-amz-content-sha256;x-amz-date")
	assert.Contains(t, canonical, "/bucket/prefix/1.name%29.rnd")
	assert.NotContains(t, canonical, "/bucket/prefix/1.name).rnd")
}

func TestPresignURL(t *testing.T) {
	tests := []struct {
		name      string
		method    string
		rawURL    string
		accessKey string
		secretKey string
		region    string
		expires   int
	}{
		{"get_object", http.MethodGet, "http://localhost:9000/mybucket/file.txt", "AKID", "SECRET", "us-east-1", 3600},
		{"put_object", http.MethodPut, "http://localhost:9000/mybucket/upload.txt", "AKID", "SECRET", "us-east-1", 600},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			presigned, err := PresignURL(tt.method, tt.rawURL, tt.accessKey, tt.secretKey, tt.region, tt.expires)
			require.NoError(t, err)
			assert.Contains(t, presigned, "X-Amz-Algorithm=AWS4-HMAC-SHA256")
			assert.Contains(t, presigned, "X-Amz-Credential=")
			assert.Contains(t, presigned, "X-Amz-Signature=")
			assert.Contains(t, presigned, "X-Amz-Expires=")
		})
	}
}

func TestVerifyPresignedURL(t *testing.T) {
	v := NewVerifier([]Credentials{
		{AccessKey: "AKID", SecretKey: "SECRET"},
	})

	presigned, err := PresignURL(http.MethodGet, "http://localhost:9000/mybucket/file.txt", "AKID", "SECRET", "us-east-1", 3600)
	require.NoError(t, err)

	req, _ := http.NewRequest(http.MethodGet, presigned, nil)
	req.Host = "localhost:9000"

	accessKey, err := v.Verify(req)
	require.NoError(t, err, "verify presigned URL")
	assert.Equal(t, "AKID", accessKey)
}

func TestVerifyPresignedURLExpired(t *testing.T) {
	v := NewVerifier([]Credentials{
		{AccessKey: "AKID", SecretKey: "SECRET"},
	})

	presigned, err := PresignURLAt(http.MethodGet, "http://localhost:9000/mybucket/file.txt",
		"AKID", "SECRET", "us-east-1", 1, time.Now().Add(-10*time.Second))
	require.NoError(t, err)

	req, _ := http.NewRequest(http.MethodGet, presigned, nil)
	req.Host = "localhost:9000"

	_, err = v.Verify(req)
	assert.Error(t, err, "expected error for expired presigned URL")
	assert.Contains(t, err.Error(), "expired")
}

func TestVerifyPresignedURLWrongSignature(t *testing.T) {
	v := NewVerifier([]Credentials{
		{AccessKey: "AKID", SecretKey: "SECRET"},
	})

	presigned, err := PresignURL(http.MethodGet, "http://localhost:9000/mybucket/file.txt", "AKID", "WRONG", "us-east-1", 3600)
	require.NoError(t, err)

	req, _ := http.NewRequest(http.MethodGet, presigned, nil)
	req.Host = "localhost:9000"

	_, err = v.Verify(req)
	assert.Error(t, err, "expected error for wrong signature")
}

// TestVerify_SecretLookupFallback exercises the IAM-backed path: empty
// static creds + a SecretLookup function that resolves the access_key
// must authenticate. Regression guard for the bug where Verify and
// verifyPresigned read v.creds directly and ignored SecretLookup,
// breaking every IAM-issued SA key.
func TestVerify_SecretLookupFallback(t *testing.T) {
	v := NewVerifier(nil)
	v.SecretLookup = func(ak string) (string, bool) {
		if ak == "AKID-IAM" {
			return "SECRET-IAM", true
		}
		return "", false
	}

	t.Run("header_signed", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodGet, "http://localhost:9000/b", nil)
		req.Host = "localhost:9000"
		SignRequest(req, "AKID-IAM", "SECRET-IAM", "us-east-1")
		got, err := v.Verify(req)
		require.NoError(t, err)
		assert.Equal(t, "AKID-IAM", got)
	})

	t.Run("presigned", func(t *testing.T) {
		presigned, err := PresignURL(http.MethodGet, "http://localhost:9000/b/k",
			"AKID-IAM", "SECRET-IAM", "us-east-1", 3600)
		require.NoError(t, err)
		req, _ := http.NewRequest(http.MethodGet, presigned, nil)
		req.Host = "localhost:9000"
		got, err := v.Verify(req)
		require.NoError(t, err)
		assert.Equal(t, "AKID-IAM", got)
	})

	t.Run("unknown_key_still_rejected", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodGet, "http://localhost:9000/b", nil)
		req.Host = "localhost:9000"
		SignRequest(req, "AKID-OTHER", "SECRET-OTHER", "us-east-1")
		_, err := v.Verify(req)
		assert.Error(t, err)
	})
}
