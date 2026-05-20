package e2e

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gritive/GrainFS/internal/bucketadmin"
	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// BucketPolicy test set probes admin-UDS policy CRUD, data-plane policy read,
// data-plane mutation denial, and policy enforcement. A single set of cases
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

		adminPolicySet(t, tgt, bucket, []byte(policy))

		raw := adminPolicyGet(t, tgt, bucket)
		assert.Contains(t, string(raw), "s3:GetObject")

		got, err := cli.GetBucketPolicy(ctx, &s3.GetBucketPolicyInput{
			Bucket: aws.String(bucket),
		})
		require.NoError(t, err)
		require.NotNil(t, got.Policy)
		assert.Contains(t, *got.Policy, "s3:GetObject")

		adminPolicyDelete(t, tgt, bucket)
	})

	t.Run("InvalidJSON", func(t *testing.T) {
		bucket := tgt.uniqueBucket(t, "polinval")
		status, body := adminPolicySetRaw(t, tgt, bucket, []byte(`{invalid`))
		assert.Equalf(t, http.StatusBadRequest, status, "body=%s", body)
	})

	t.Run("DataPlaneMutationDenied", func(t *testing.T) {
		bucket := tgt.uniqueBucket(t, "poldenymutate")

		_, err := tgt.pickNode(0).PutBucketPolicy(context.Background(), &s3.PutBucketPolicyInput{
			Bucket: aws.String(bucket),
			Policy: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
		})
		require.Error(t, err)
		require.Containsf(t, []int{http.StatusForbidden, http.StatusUnauthorized}, httpStatusFrom(err), "PutBucketPolicy err=%v", err)

		_, err = tgt.pickNode(0).DeleteBucketPolicy(context.Background(), &s3.DeleteBucketPolicyInput{
			Bucket: aws.String(bucket),
		})
		require.Error(t, err)
		require.Containsf(t, []int{http.StatusForbidden, http.StatusUnauthorized}, httpStatusFrom(err), "DeleteBucketPolicy err=%v", err)
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

		adminPolicySet(t, tgt, bucket, []byte(policy))

		req, err := http.NewRequest(http.MethodGet, tgt.endpoint(0)+"/"+bucket+"/secret.txt", nil)
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	})
}

func adminPolicyClient(t *testing.T, tgt s3Target) *bucketadmin.Client {
	t.Helper()
	c, err := bucketadmin.NewClient(tgt.adminSockPath())
	require.NoError(t, err)
	return c
}

func adminPolicySet(t *testing.T, tgt s3Target, bucket string, policy []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, adminPolicyClient(t, tgt).PolicySet(ctx, bucket, policy))
}

func adminPolicyGet(t *testing.T, tgt s3Target, bucket string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	raw, err := adminPolicyClient(t, tgt).PolicyGetRaw(ctx, bucket)
	require.NoError(t, err)
	return raw
}

func adminPolicyDelete(t *testing.T, tgt s3Target, bucket string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, adminPolicyClient(t, tgt).PolicyDelete(ctx, bucket))
}

func adminPolicySetRaw(t *testing.T, tgt s3Target, bucket string, body []byte) (int, []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPut,
		"http://unix/v1/buckets/"+url.PathEscape(bucket)+"/policy", bytes.NewReader(body))
	require.NoError(t, err)
	resp, err := iamUDSClient(tgt.adminSockPath()).Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, respBody
}

func signedPolicyRequest(t *testing.T, tgt s3Target, method, bucket string, body io.Reader) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, tgt.endpoint(0)+"/"+bucket+"?policy", body)
	require.NoError(t, err)
	req.Host = req.URL.Host
	s3auth.SignRequest(req, tgt.accessKey, tgt.secretKey, "us-east-1")
	return req
}
