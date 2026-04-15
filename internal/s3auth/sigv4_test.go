package s3auth

import (
	"net/http"
	"testing"

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
