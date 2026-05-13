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
	"github.com/gritive/GrainFS/internal/s3auth"
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

	_, err := testS3Client.PutBucketPolicy(context.Background(), &s3.PutBucketPolicyInput{
		Bucket: aws.String("policy-test"),
		Policy: aws.String(policy),
	})
	require.NoError(t, err)

	got, err := testS3Client.GetBucketPolicy(context.Background(), &s3.GetBucketPolicyInput{
		Bucket: aws.String("policy-test"),
	})
	require.NoError(t, err)
	require.NotNil(t, got.Policy)
	assert.Contains(t, *got.Policy, "s3:GetObject")

	_, err = testS3Client.DeleteBucketPolicy(context.Background(), &s3.DeleteBucketPolicyInput{
		Bucket: aws.String("policy-test"),
	})
	require.NoError(t, err)
}

func TestE2E_BucketPolicy_InvalidJSON(t *testing.T) {
	createBucket(t, "policy-invalid")

	req := signedPolicyRequest(t, http.MethodPut, "policy-invalid", bytes.NewReader([]byte(`{invalid`)))
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

	req := signedPolicyRequest(t, http.MethodPut, "policy-deny", bytes.NewReader([]byte(policy)))
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

func signedPolicyRequest(t *testing.T, method, bucket string, body io.Reader) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, testServerURL+"/"+bucket+"?policy", body)
	require.NoError(t, err)
	req.Host = req.URL.Host
	s3auth.SignRequest(req, testAccessKey, testSecretKey, "us-east-1")
	return req
}
