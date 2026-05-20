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
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/stretchr/testify/require"
)

// BucketPolicy test set probes admin-UDS policy CRUD, data-plane policy read,
// data-plane mutation denial, and policy enforcement. A single set of cases
// runs against both single-node and 4-node cluster fixtures to prove the
// policy plane is at parity across topologies.

var _ = ginkgo.Describe("Bucket policy", ginkgo.Label("bucket"), func() {
	describeBucketPolicyContext("SingleNode", func() s3Target {
		return newSingleNodeS3Target()
	})
	describeBucketPolicyContext("Cluster4Node", func() s3Target {
		return newSharedClusterS3Target(ginkgo.GinkgoTB())
	})
})

func describeBucketPolicyContext(name string, factory func() s3Target) {
	ginkgo.Context(name, func() {
		var tgt s3Target

		ginkgo.BeforeEach(func() {
			tgt = factory()
		})

		runBucketPolicyCases(func() s3Target { return tgt })
	})
}

func runBucketPolicyCases(getTgt func() s3Target) {
	ginkgo.It("sets and reads bucket policy through admin and data planes (SetAndGet)", func() {
		tb := ginkgo.GinkgoTB()
		tgt := getTgt()
		bucket := createSpecBucket(tgt, "polset")
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

		adminPolicySet(tb, tgt, bucket, []byte(policy))
		ginkgo.DeferCleanup(func() {
			adminPolicyDelete(ginkgo.GinkgoTB(), tgt, bucket)
		})

		raw := adminPolicyGet(tb, tgt, bucket)
		gomega.Expect(string(raw)).To(gomega.ContainSubstring("s3:GetObject"))

		got, err := cli.GetBucketPolicy(ctx, &s3.GetBucketPolicyInput{
			Bucket: aws.String(bucket),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(got.Policy).NotTo(gomega.BeNil())
		gomega.Expect(*got.Policy).To(gomega.ContainSubstring("s3:GetObject"))
	})

	ginkgo.It("rejects invalid policy JSON (InvalidJSON)", func() {
		tb := ginkgo.GinkgoTB()
		tgt := getTgt()
		bucket := createSpecBucket(tgt, "polinval")
		status, body := adminPolicySetRaw(tb, tgt, bucket, []byte(`{invalid`))
		gomega.Expect(status).To(gomega.Equal(http.StatusBadRequest), "body=%s", body)
	})

	ginkgo.It("denies data-plane policy mutation (DataPlaneMutationDenied)", func() {
		tgt := getTgt()
		bucket := createSpecBucket(tgt, "poldenymutate")

		_, err := tgt.pickNode(0).PutBucketPolicy(context.Background(), &s3.PutBucketPolicyInput{
			Bucket: aws.String(bucket),
			Policy: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
		})
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect([]int{http.StatusForbidden, http.StatusUnauthorized}).To(gomega.ContainElement(httpStatusFrom(err)), "PutBucketPolicy err=%v", err)

		_, err = tgt.pickNode(0).DeleteBucketPolicy(context.Background(), &s3.DeleteBucketPolicyInput{
			Bucket: aws.String(bucket),
		})
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect([]int{http.StatusForbidden, http.StatusUnauthorized}).To(gomega.ContainElement(httpStatusFrom(err)), "DeleteBucketPolicy err=%v", err)
	})

	ginkgo.It("enforces deny policies on object reads (DenyAction)", func() {
		tb := ginkgo.GinkgoTB()
		tgt := getTgt()
		bucket := createSpecBucket(tgt, "poldeny")
		cli := tgt.pickNode(0)
		ctx := context.Background()

		_, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("secret.txt"),
			Body:   strings.NewReader("secret data"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		policy := fmt.Sprintf(`{
			"Version": "2012-10-17",
			"Statement": [{
				"Effect": "Deny",
				"Principal": "*",
				"Action": ["s3:*"],
				"Resource": ["arn:aws:s3:::%s/*"]
			}]
		}`, bucket)

		adminPolicySet(tb, tgt, bucket, []byte(policy))
		ginkgo.DeferCleanup(func() {
			adminPolicyDelete(ginkgo.GinkgoTB(), tgt, bucket)
		})

		req, err := http.NewRequest(http.MethodGet, tgt.endpoint(0)+"/"+bucket+"/secret.txt", nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		resp, err := http.DefaultClient.Do(req)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		resp.Body.Close()
		gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusForbidden))
	})
}

func adminPolicyClient(t testing.TB, tgt s3Target) *bucketadmin.Client {
	t.Helper()
	c, err := bucketadmin.NewClient(tgt.adminSockPath())
	require.NoError(t, err)
	return c
}

func adminPolicySet(t testing.TB, tgt s3Target, bucket string, policy []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, adminPolicyClient(t, tgt).PolicySet(ctx, bucket, policy))
}

func adminPolicyGet(t testing.TB, tgt s3Target, bucket string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	raw, err := adminPolicyClient(t, tgt).PolicyGetRaw(ctx, bucket)
	require.NoError(t, err)
	return raw
}

func adminPolicyDelete(t testing.TB, tgt s3Target, bucket string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, adminPolicyClient(t, tgt).PolicyDelete(ctx, bucket))
}

func adminPolicySetRaw(t testing.TB, tgt s3Target, bucket string, body []byte) (int, []byte) {
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
