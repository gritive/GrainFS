package e2e

import (
	"bytes"
	"context"
	"fmt"
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

// BucketPolicy test set probes the PutBucketPolicy / GetBucketPolicy /
// DeleteBucketPolicy / policy-enforcement surface. A single set of cases
// runs against both single-node and 4-node cluster fixtures to prove the
// policy plane is at parity across topologies.

func TestBucketPolicyE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runBucketPolicyCases(t, newSingleNodeS3Target())
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		runBucketPolicyCases(t, newSharedClusterS3Target(t))
	})
}

func runBucketPolicyCases(t *testing.T, tgt s3Target) {
	t.Helper()

	t.Run("SetAndGet", func(t *testing.T) {
		bucket := tgt.uniqueBucket(t, "polset")
		cli := tgt.pickNode(0)
		ctx := context.Background()

		policy := fmt.Sprintf(`{
			"Version": "2012-10-17",
			"Statement": [{
				"Effect": "Allow",
				"Principal": "*",
				"Action": ["s3:GetObject"],
				"Resource": ["arn:aws:s3:::%s/*"]
			}]
		}`, bucket)

		_, err := cli.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{
			Bucket: aws.String(bucket),
			Policy: aws.String(policy),
		})
		require.NoError(t, err)

		got, err := cli.GetBucketPolicy(ctx, &s3.GetBucketPolicyInput{
			Bucket: aws.String(bucket),
		})
		require.NoError(t, err)
		require.NotNil(t, got.Policy)
		assert.Contains(t, *got.Policy, "s3:GetObject")

		_, err = cli.DeleteBucketPolicy(ctx, &s3.DeleteBucketPolicyInput{
			Bucket: aws.String(bucket),
		})
		require.NoError(t, err)
	})

	t.Run("InvalidJSON", func(t *testing.T) {
		bucket := tgt.uniqueBucket(t, "polinval")

		req := signedPolicyRequest(t, tgt, http.MethodPut, bucket, bytes.NewReader([]byte(`{invalid`)))
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})

	t.Run("DenyAction", func(t *testing.T) {
		bucket := tgt.uniqueBucket(t, "poldeny")
		cli := tgt.pickNode(0)
		ctx := context.Background()

		_, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("secret.txt"),
			Body:   strings.NewReader("secret data"),
		})
		require.NoError(t, err)

		policy := fmt.Sprintf(`{
			"Version": "2012-10-17",
			"Statement": [{
				"Effect": "Deny",
				"Principal": "*",
				"Action": ["s3:*"],
				"Resource": ["arn:aws:s3:::%s/*"]
			}]
		}`, bucket)

		req := signedPolicyRequest(t, tgt, http.MethodPut, bucket, bytes.NewReader([]byte(policy)))
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, http.StatusNoContent, resp.StatusCode)

		req, _ = http.NewRequest(http.MethodGet, tgt.endpoint(0)+"/"+bucket+"/secret.txt", nil)
		resp, err = http.DefaultClient.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	})
}

func signedPolicyRequest(t *testing.T, tgt s3Target, method, bucket string, body io.Reader) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, tgt.endpoint(0)+"/"+bucket+"?policy", body)
	require.NoError(t, err)
	req.Host = req.URL.Host
	s3auth.SignRequest(req, tgt.accessKey, tgt.secretKey, "us-east-1")
	return req
}
