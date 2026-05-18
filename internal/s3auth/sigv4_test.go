package s3auth

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
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

// signWithCanonicalQuery rebuilds a SigV4 Authorization header for r using the
// supplied canonicalQuery instead of r.URL.RawQuery, so tests can simulate
// clients (botocore, AWS SDKs) whose wire-side query string differs from the
// AWS-strict canonical they signed against (bare-key vs `key=`, `+` vs `%20`).
func signWithCanonicalQuery(r *http.Request, accessKey, secretKey, region, canonicalQuery string) {
	now := time.Now().UTC()
	date := now.Format("20060102")
	amzDate := now.Format("20060102T150405Z")
	r.Header.Set("X-Amz-Date", amzDate)
	r.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	signedHeaders := "host;x-amz-content-sha256;x-amz-date"
	uri := r.URL.EscapedPath()
	if uri == "" {
		uri = "/"
	}
	canonicalHeaders := "host:" + r.Host + "\n" +
		"x-amz-content-sha256:UNSIGNED-PAYLOAD\n" +
		"x-amz-date:" + amzDate + "\n"
	canonical := r.Method + "\n" + uri + "\n" + canonicalQuery + "\n" +
		canonicalHeaders + "\n" + signedHeaders + "\nUNSIGNED-PAYLOAD"
	h := sha256.Sum256([]byte(canonical))
	scope := fmt.Sprintf("%s/%s/s3/aws4_request", date, region)
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hex.EncodeToString(h[:])
	sig := calculateSignature(secretKey, date, region, "s3", stringToSign)
	credential := fmt.Sprintf("%s/%s/%s/s3/aws4_request", accessKey, date, region)
	r.Header.Set("Authorization", fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s, SignedHeaders=%s, Signature=%s",
		credential, signedHeaders, sig))
}

// TestVerifyAcceptsBareKeyQuery covers botocore's PutBucketVersioning wire
// shape: aws-cli sends `PUT /bucket?versioning HTTP/1.1` (no `=`) but signs
// against the AWS-strict canonical `versioning=`. Server-side verification
// must normalise the wire query to the same strict canonical, otherwise
// every PutBucketVersioning/GetBucketVersioning is rejected with
// "signature mismatch".
func TestVerifyAcceptsBareKeyQuery(t *testing.T) {
	v := NewVerifier([]Credentials{{AccessKey: "AKID", SecretKey: "SECRET"}})
	req, _ := http.NewRequest(http.MethodPut, "http://localhost:9000/bucket?versioning", nil)
	req.Host = "localhost:9000"
	signWithCanonicalQuery(req, "AKID", "SECRET", "us-east-1", "versioning=")
	_, err := v.Verify(req)
	require.NoError(t, err, "bare-key query must verify against AWS-strict canonical")
}

// TestVerifyAcceptsSpaceAsPercent20 covers any client that URL-encodes spaces
// in query values as `%20` (AWS-strict canonical) while net/url's QueryEscape
// renders them as `+`. The server canonical builder must match `%20`.
func TestVerifyAcceptsSpaceAsPercent20(t *testing.T) {
	v := NewVerifier([]Credentials{{AccessKey: "AKID", SecretKey: "SECRET"}})
	req, _ := http.NewRequest(http.MethodGet, "http://localhost:9000/bucket?prefix=hello%20world", nil)
	req.Host = "localhost:9000"
	signWithCanonicalQuery(req, "AKID", "SECRET", "us-east-1", "prefix=hello%20world")
	_, err := v.Verify(req)
	require.NoError(t, err, "AWS-strict %20-encoded space must verify")
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
