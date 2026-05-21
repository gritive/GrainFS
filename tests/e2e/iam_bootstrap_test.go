package e2e

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithy "github.com/aws/smithy-go"
	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("IAM bootstrap", func() {
	for _, tc := range []struct {
		name string
		mk   func(t testing.TB) iamBootstrapTarget
	}{
		{name: "SingleNode", mk: newSingleNodeBootstrapTarget},
		{name: "Cluster4Node", mk: newClusterBootstrapTarget},
	} {
		tc := tc
		ginkgo.Context(tc.name, func() {
			var tgt iamBootstrapTarget

			ginkgo.BeforeEach(func() {
				tgt = tc.mk(ginkgo.GinkgoTB())
			})

			// Each branch spawns exactly one fixture and runs the lifecycle
			// stages sequentially. State accumulates across stages, matching how
			// operators experience the bootstrap flow.
			ginkgo.It("runs the bootstrap service-account lifecycle", func() {
				runIAMBootstrapCases(ginkgo.GinkgoTB(), tgt)
			})
		})
	}
})

// runIAMBootstrapCases drives one bootstrap-lifecycle fixture through the
// full sequence of pre-bootstrap, first-SA, second-SA, and post-bootstrap
// assertions. State accumulates: each sub-case relies on the side-effects
// of the previous one. This is intentional — it matches how operators
// experience the bootstrap dance in practice.
func runIAMBootstrapCases(t testing.TB, tgt iamBootstrapTarget) {
	t.Helper()

	// Shared state captured across sub-cases.
	var bootstrapAK, bootstrapSK string
	var bootstrapSAID string

	{
		cli := s3ClientFor(tgt.s3URL(), "AKIA-fake-bootstrap-test", "fake-secret-bootstrap-test")

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		ginkgo.DeferCleanup(cancel)
		_, err := cli.ListBuckets(ctx, &s3.ListBucketsInput{})
		gomega.Expect(err).To(gomega.HaveOccurred(), "ListBuckets with fabricated key must fail before bootstrap")

		var apiErr smithy.APIError
		gomega.Expect(errors.As(err, &apiErr)).To(gomega.BeTrue(), "expected smithy APIError, got %T: %v", err, err)
		// Acceptable codes from the S3 auth path.
		code := apiErr.ErrorCode()
		gomega.Expect([]string{"AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch"}).To(gomega.ContainElement(code),
			"unexpected error code %q from pre-bootstrap sigv4: %v", code, err,
		)
	}

	{
		admin, _ := bootstrapAdminViaUDSAnyResult(t, tgt.dataDirs(), 30*time.Second)
		gomega.Expect(admin.AccessKey).NotTo(gomega.BeEmpty(), "first SA bootstrap must return non-empty access_key")
		gomega.Expect(admin.SecretKey).NotTo(gomega.BeEmpty(), "first SA bootstrap must return non-empty secret_key")

		bootstrapAK = admin.AccessKey
		bootstrapSK = admin.SecretKey
		bootstrapSAID = admin.SAID

		// Exactly one SA must be present after bootstrap.
		var saList []map[string]any
		iamDo(t, tgt.adminSock(), "GET", "/v1/iam/sa", nil, &saList)
		gomega.Expect(saList).To(gomega.HaveLen(1), "exactly one SA must exist after first-bootstrap; got %v", saList)

		// The SA ID must be a non-empty string (UUIDv7, not legacy "sa-default").
		gomega.Expect(bootstrapSAID).NotTo(gomega.BeEmpty(), "first SA must have a non-empty sa_id")
	}

	{
		gomega.Expect(bootstrapSAID).NotTo(gomega.BeEmpty(), "prerequisite FirstSAWildcardGrant must run first")

		sock := tgt.adminSock()

		var out struct {
			SAID string `json:"sa_id"`
			Name string `json:"name"`
		}
		iamDo(t, sock, "POST", "/v1/iam/sa", map[string]string{"name": "user1"}, &out)
		gomega.Expect(out.SAID).NotTo(gomega.BeEmpty(), "second SA must have non-empty sa_id")
		gomega.Expect(out.SAID).NotTo(gomega.Equal(bootstrapSAID), "second SA must have a different sa_id from the first")
	}

	{
		gomega.Expect(bootstrapAK).NotTo(gomega.BeEmpty(), "prerequisite FirstSAWildcardGrant must run first")

		cli := s3ClientFor(tgt.s3URL(), bootstrapAK, bootstrapSK)
		gomega.Expect(waitForIAMReady(cli, 30*time.Second)).To(gomega.Succeed())

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		ginkgo.DeferCleanup(cancel)

		// ListBuckets — admin SA must succeed.
		_, err := cli.ListBuckets(ctx, &s3.ListBucketsInput{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "ListBuckets")

		// CreateBucket with bucket-admin policy attached to the bootstrap SA.
		bucket := "f4-bootstrap-bucket-" + tgt.name
		createBucketWithAdminPolicyAttachViaUDSAny(t, tgt.dataDirs(), bootstrapSAID, bucket, cli)

		// PutObject.
		const payload = "hello-bootstrap-f4"
		_, err = cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("obj1"),
			Body:   strings.NewReader(payload),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "PutObject")

		// GetObject — body must round-trip.
		getOut, err := cli.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("obj1"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "GetObject")
		ginkgo.DeferCleanup(getOut.Body.Close)
		body, err := io.ReadAll(getOut.Body)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "read GetObject body")
		gomega.Expect(string(body)).To(gomega.Equal(payload), "object body round-trip mismatch")
	}
}
