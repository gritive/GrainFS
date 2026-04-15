package e2e

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestE2E_BucketPolicy_SetAndGet(t *testing.T) {
	createBucket(t, "policy-test")

	policy := `{
		"Version": "2012-10-17",
		"Statement": [{
			"Effect": "Allow",
			"Principal": "*",
			"Action": ["s3:GetObject"],
			"Resource": ["arn:aws:s3:::policy-test/*"]
		}]
	}`

	// PUT bucket policy
	req, _ := http.NewRequest(http.MethodPut, testServerURL+"/policy-test?policy", bytes.NewReader([]byte(policy)))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	// GET bucket policy
	req, _ = http.NewRequest(http.MethodGet, testServerURL+"/policy-test?policy", nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), "s3:GetObject")

	// DELETE bucket policy
	req, _ = http.NewRequest(http.MethodDelete, testServerURL+"/policy-test?policy", nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
}

func TestE2E_BucketPolicy_InvalidJSON(t *testing.T) {
	createBucket(t, "policy-invalid")

	req, _ := http.NewRequest(http.MethodPut, testServerURL+"/policy-invalid?policy", bytes.NewReader([]byte(`{invalid`)))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestE2E_BucketPolicy_DenyAction(t *testing.T) {
	createBucket(t, "policy-deny")

	// Upload an object first
	_, err := testS3Client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String("policy-deny"),
		Key:    aws.String("secret.txt"),
		Body:   strings.NewReader("secret data"),
	})
	require.NoError(t, err)

	// Set policy that denies all access for any user
	policy := `{
		"Version": "2012-10-17",
		"Statement": [{
			"Effect": "Deny",
			"Principal": "*",
			"Action": ["s3:*"],
			"Resource": ["arn:aws:s3:::policy-deny/*"]
		}]
	}`

	req, _ := http.NewRequest(http.MethodPut, testServerURL+"/policy-deny?policy", bytes.NewReader([]byte(policy)))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	// Try to get the object — should be denied
	req, _ = http.NewRequest(http.MethodGet, testServerURL+"/policy-deny/secret.txt", nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
}
