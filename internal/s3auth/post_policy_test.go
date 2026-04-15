package s3auth

import (
	"encoding/base64"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPostPolicy_ValidSignature(t *testing.T) {
	creds := Credentials{AccessKey: "AKIAIOSFODNN7EXAMPLE", SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"}
	date := "20260416"
	region := "us-east-1"

	policy := `{"expiration":"2026-04-17T00:00:00Z","conditions":[{"bucket":"mybucket"},["starts-with","$key","uploads/"]]}`
	policyB64 := base64.StdEncoding.EncodeToString([]byte(policy))

	sig := SignPostPolicy(policyB64, creds.SecretKey, date, region, "s3")

	err := VerifyPostPolicy(policyB64, sig, creds.SecretKey, date, region, "s3")
	require.NoError(t, err)
}

func TestPostPolicy_InvalidSignature(t *testing.T) {
	date := "20260416"
	region := "us-east-1"

	policy := `{"expiration":"2026-04-17T00:00:00Z","conditions":[]}`
	policyB64 := base64.StdEncoding.EncodeToString([]byte(policy))

	err := VerifyPostPolicy(policyB64, "bad-signature", "secret", date, region, "s3")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "signature mismatch")
}

func TestPostPolicy_Expired(t *testing.T) {
	policy := `{"expiration":"2020-01-01T00:00:00Z","conditions":[]}`
	policyB64 := base64.StdEncoding.EncodeToString([]byte(policy))

	err := ValidatePostPolicyExpiration(policyB64)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expired")
}

func TestPostPolicy_NotExpired(t *testing.T) {
	exp := time.Now().Add(1 * time.Hour).UTC().Format("2006-01-02T15:04:05Z")
	policy := `{"expiration":"` + exp + `","conditions":[]}`
	policyB64 := base64.StdEncoding.EncodeToString([]byte(policy))

	err := ValidatePostPolicyExpiration(policyB64)
	require.NoError(t, err)
}

func TestPostPolicy_ConditionBucketMatch(t *testing.T) {
	policy := `{"expiration":"2030-01-01T00:00:00Z","conditions":[{"bucket":"mybucket"}]}`
	policyB64 := base64.StdEncoding.EncodeToString([]byte(policy))

	err := ValidatePostPolicyConditions(policyB64, map[string]string{
		"bucket": "mybucket",
		"key":    "file.txt",
	})
	require.NoError(t, err)
}

func TestPostPolicy_ConditionBucketMismatch(t *testing.T) {
	policy := `{"expiration":"2030-01-01T00:00:00Z","conditions":[{"bucket":"mybucket"}]}`
	policyB64 := base64.StdEncoding.EncodeToString([]byte(policy))

	err := ValidatePostPolicyConditions(policyB64, map[string]string{
		"bucket": "wrongbucket",
		"key":    "file.txt",
	})
	assert.Error(t, err)
}

func TestPostPolicy_ConditionStartsWith(t *testing.T) {
	policy := `{"expiration":"2030-01-01T00:00:00Z","conditions":[["starts-with","$key","uploads/"]]}`
	policyB64 := base64.StdEncoding.EncodeToString([]byte(policy))

	err := ValidatePostPolicyConditions(policyB64, map[string]string{
		"key": "uploads/photo.jpg",
	})
	require.NoError(t, err)

	err = ValidatePostPolicyConditions(policyB64, map[string]string{
		"key": "other/file.txt",
	})
	assert.Error(t, err)
}

func TestPostPolicy_ConditionContentLengthRange(t *testing.T) {
	policy := `{"expiration":"2030-01-01T00:00:00Z","conditions":[["content-length-range",100,10485760]]}`
	policyB64 := base64.StdEncoding.EncodeToString([]byte(policy))

	// content-length-range is validated separately with actual size, skip here
	err := ValidatePostPolicyConditions(policyB64, map[string]string{})
	require.NoError(t, err)
}
