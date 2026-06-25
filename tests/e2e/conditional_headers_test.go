package e2e

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// S3 Conditional Headers characterizes the RFC 7232 read-path conditional
// request headers (If-Match / If-None-Match / If-Modified-Since /
// If-Unmodified-Since) on GET, HEAD, and Range-GET. The feature is already
// implemented and correct (internal/server/object_conditionals.go ->
// checkConditionals); the s3-compatibility matrix declares it "Supported",
// which under the doc policy requires e2e coverage. These cases are therefore
// characterization tests (green on first run), not RED-first.
//
// 412 PreconditionFailed and 304 NotModified are returned as bare HTTP status
// codes with no XML error body, so assertions extract the status via
// smithyhttp.ResponseError rather than the modeled S3 error code.
var _ = ginkgo.Describe("S3 Conditional Headers", ginkgo.Label("s3"), func() {
	describeConditionalContext("SingleNode", func() s3Target {
		return newSingleNodeS3Target()
	})
	describeConditionalContext("Cluster4Node", func() s3Target {
		return newSharedClusterS3Target(ginkgo.GinkgoTB())
	})
})

func describeConditionalContext(name string, factory func() s3Target) {
	ginkgo.Context(name, ginkgo.Ordered, func() {
		var (
			tgt s3Target
			cli *s3.Client
		)

		ginkgo.BeforeAll(func() {
			tgt = factory()
			cli = tgt.pickNode(0)
		})

		runConditionalHeaderCases(func() s3Target { return tgt }, func() *s3.Client { return cli })
	})
}

func runConditionalHeaderCases(getTgt func() s3Target, getClient func() *s3.Client) {
	const key = "obj.txt"

	// putAndCaptureETag PUTs the object (cluster-tolerant) and returns the
	// server-issued ETag exactly as HeadObject reports it (quoted). The
	// conditional headers must be sent verbatim against this value: the SDK
	// forwards If-Match/If-None-Match unchanged and the server compares against
	// the quoted form.
	putAndCaptureETag := func(ctx context.Context, bucket string) string {
		ginkgo.GinkgoHelper()
		putPlainObjectEventually(ctx, getTgt(), bucket, key, "conditional-body")
		var etag string
		gomega.Eventually(func() error {
			head, err := getClient().HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			if err != nil {
				return err
			}
			etag = aws.ToString(head.ETag)
			return nil
		}, 30*time.Second, 500*time.Millisecond).Should(gomega.Succeed())
		gomega.Expect(etag).NotTo(gomega.BeEmpty(), "server must report an ETag")
		return etag
	}

	ginkgo.It("honors If-Match on GET and HEAD", func(ctx context.Context) {
		bucket := createSpecBucket(getTgt(), "ifmatch")
		etag := putAndCaptureETag(ctx, bucket)

		// matching ETag -> 200
		expectConditionalGet(ctx, getClient(), bucket, key, http.StatusOK, &s3.GetObjectInput{IfMatch: aws.String(etag)})
		expectConditionalHead(ctx, getClient(), bucket, key, http.StatusOK, &s3.HeadObjectInput{IfMatch: aws.String(etag)})
		// bogus ETag -> 412
		expectConditionalGet(ctx, getClient(), bucket, key, http.StatusPreconditionFailed, &s3.GetObjectInput{IfMatch: aws.String(`"bogus"`)})
		expectConditionalHead(ctx, getClient(), bucket, key, http.StatusPreconditionFailed, &s3.HeadObjectInput{IfMatch: aws.String(`"bogus"`)})
	}, ginkgo.NodeTimeout(90*time.Second))

	ginkgo.It("honors If-None-Match on GET and HEAD", func(ctx context.Context) {
		bucket := createSpecBucket(getTgt(), "ifnonematch")
		etag := putAndCaptureETag(ctx, bucket)

		// matching ETag -> 304
		expectConditionalGet(ctx, getClient(), bucket, key, http.StatusNotModified, &s3.GetObjectInput{IfNoneMatch: aws.String(etag)})
		expectConditionalHead(ctx, getClient(), bucket, key, http.StatusNotModified, &s3.HeadObjectInput{IfNoneMatch: aws.String(etag)})
		// bogus ETag -> 200
		expectConditionalGet(ctx, getClient(), bucket, key, http.StatusOK, &s3.GetObjectInput{IfNoneMatch: aws.String(`"bogus"`)})
		expectConditionalHead(ctx, getClient(), bucket, key, http.StatusOK, &s3.HeadObjectInput{IfNoneMatch: aws.String(`"bogus"`)})
	}, ginkgo.NodeTimeout(90*time.Second))

	ginkgo.It("honors If-Modified-Since on GET and HEAD", func(ctx context.Context) {
		bucket := createSpecBucket(getTgt(), "ifmodsince")
		_ = putAndCaptureETag(ctx, bucket)
		future := time.Now().Add(time.Hour)
		past := time.Now().Add(-time.Hour)

		// not modified since a future instant -> 304
		expectConditionalGet(ctx, getClient(), bucket, key, http.StatusNotModified, &s3.GetObjectInput{IfModifiedSince: aws.Time(future)})
		expectConditionalHead(ctx, getClient(), bucket, key, http.StatusNotModified, &s3.HeadObjectInput{IfModifiedSince: aws.Time(future)})
		// modified since a past instant -> 200
		expectConditionalGet(ctx, getClient(), bucket, key, http.StatusOK, &s3.GetObjectInput{IfModifiedSince: aws.Time(past)})
		expectConditionalHead(ctx, getClient(), bucket, key, http.StatusOK, &s3.HeadObjectInput{IfModifiedSince: aws.Time(past)})
	}, ginkgo.NodeTimeout(90*time.Second))

	ginkgo.It("honors If-Unmodified-Since on GET and HEAD", func(ctx context.Context) {
		bucket := createSpecBucket(getTgt(), "ifunmodsince")
		_ = putAndCaptureETag(ctx, bucket)
		future := time.Now().Add(time.Hour)
		past := time.Now().Add(-time.Hour)

		// modified since a past instant -> 412
		expectConditionalGet(ctx, getClient(), bucket, key, http.StatusPreconditionFailed, &s3.GetObjectInput{IfUnmodifiedSince: aws.Time(past)})
		expectConditionalHead(ctx, getClient(), bucket, key, http.StatusPreconditionFailed, &s3.HeadObjectInput{IfUnmodifiedSince: aws.Time(past)})
		// unmodified since a future instant -> 200
		expectConditionalGet(ctx, getClient(), bucket, key, http.StatusOK, &s3.GetObjectInput{IfUnmodifiedSince: aws.Time(future)})
		expectConditionalHead(ctx, getClient(), bucket, key, http.StatusOK, &s3.HeadObjectInput{IfUnmodifiedSince: aws.Time(future)})
	}, ginkgo.NodeTimeout(90*time.Second))

	ginkgo.It("applies If-Match before Range on Range-GET", func(ctx context.Context) {
		bucket := createSpecBucket(getTgt(), "rangeifmatch")
		_ = putAndCaptureETag(ctx, bucket)

		// failed precondition wins over the Range request -> 412
		expectConditionalGet(ctx, getClient(), bucket, key, http.StatusPreconditionFailed, &s3.GetObjectInput{
			Range:   aws.String("bytes=0-3"),
			IfMatch: aws.String(`"bogus"`),
		})
	}, ginkgo.NodeTimeout(90*time.Second))
}

// expectConditionalGet issues a conditional GetObject and asserts the resulting
// HTTP status. It retries until the observed status matches want, which both
// tolerates cluster read-after-write replication lag (a peer may briefly 404
// before replication) and is a harmless no-op for the single-node target.
func expectConditionalGet(ctx context.Context, cli *s3.Client, bucket, key string, want int, in *s3.GetObjectInput) {
	ginkgo.GinkgoHelper()
	in.Bucket = aws.String(bucket)
	in.Key = aws.String(key)
	gomega.Eventually(func() int {
		out, err := cli.GetObject(ctx, in)
		if err == nil {
			_ = out.Body.Close()
			return http.StatusOK
		}
		return s3ErrorHTTPStatus(err)
	}, 30*time.Second, 500*time.Millisecond).Should(gomega.Equal(want))
}

// expectConditionalHead issues a conditional HeadObject and asserts the
// resulting HTTP status, with the same retry rationale as expectConditionalGet.
func expectConditionalHead(ctx context.Context, cli *s3.Client, bucket, key string, want int, in *s3.HeadObjectInput) {
	ginkgo.GinkgoHelper()
	in.Bucket = aws.String(bucket)
	in.Key = aws.String(key)
	gomega.Eventually(func() int {
		_, err := cli.HeadObject(ctx, in)
		if err == nil {
			return http.StatusOK
		}
		return s3ErrorHTTPStatus(err)
	}, 30*time.Second, 500*time.Millisecond).Should(gomega.Equal(want))
}

// s3ErrorHTTPStatus extracts the HTTP status code from an SDK error. 304/412
// conditional responses carry a bare status with no XML body, so the modeled S3
// error code is unreliable; the transport-level smithyhttp.ResponseError is the
// authoritative source. Returns -1 when no HTTP response is attached.
func s3ErrorHTTPStatus(err error) int {
	var re *smithyhttp.ResponseError
	if errors.As(err, &re) {
		return re.HTTPStatusCode()
	}
	return -1
}
