package e2e

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// S3ACL exercises canned ACL behavior on PutObject and CopyObject.
//
// The honored subset {private, public-read, public-read-write} is observable
// only through anonymous read authorization (there is no GetObjectAcl/?acl
// read-back surface): public-read -> anonymous GET 200, private -> 403. The
// fail-closed branch covers recognized-but-unsupported canned values
// (authenticated-read, bucket-owner-*, aws-exec-read) and all x-amz-grant-*
// headers, which are rejected with NotImplemented (501) rather than silently
// downgraded to private.
var _ = ginkgo.Describe("S3 ACL", ginkgo.Label("s3"), func() {
	describeS3ACLContext("SingleNode", func() s3Target {
		return newSingleNodeS3Target()
	})
	describeS3ACLContext("Cluster4Node", func() s3Target {
		return newSharedClusterS3Target(ginkgo.GinkgoTB())
	})
})

func describeS3ACLContext(name string, factory func() s3Target) {
	ginkgo.Context(name, ginkgo.Ordered, func() {
		var (
			tgt s3Target
			cli *s3.Client
		)

		ginkgo.BeforeAll(func() {
			tgt = factory()
			cli = tgt.pickNode(0)
		})

		runS3ACLCases(func() s3Target { return tgt }, func() *s3.Client { return cli })
	})
}

func runS3ACLCases(getTgt func() s3Target, getClient func() *s3.Client) {
	// (A) Characterization — the honored subset works end-to-end today; these
	// pass on first run (NOT TDD-RED).
	ginkgo.It("allows anonymous GET for public-read objects", func(ctx context.Context) {
		bucket := createSpecBucket(getTgt(), "publicread")
		cli := getClient()
		body := "public-read payload"

		_, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("pub.txt"),
			Body:   strings.NewReader(body),
			ACL:    types.ObjectCannedACLPublicRead,
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		requireAnonGetEventually(getTgt(), bucket, "pub.txt", http.StatusOK, body)
	}, ginkgo.NodeTimeout(60*time.Second))

	ginkgo.It("denies anonymous GET for default (no ACL header) objects", func(ctx context.Context) {
		bucket := createSpecBucket(getTgt(), "default")
		cli := getClient()

		// Regression guard: the empty-header path must NOT be rejected by the
		// fail-closed canned-ACL guard; the PUT succeeds (200).
		_, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("priv.txt"),
			Body:   strings.NewReader("private payload"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		requireAnonGetStatusEventually(getTgt(), bucket, "priv.txt", http.StatusForbidden)
	}, ginkgo.NodeTimeout(60*time.Second))

	ginkgo.It("allows anonymous GET for public-read-write objects", func(ctx context.Context) {
		bucket := createSpecBucket(getTgt(), "publicreadwrite")
		cli := getClient()
		body := "public-read-write payload"

		_, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("prw.txt"),
			Body:   strings.NewReader(body),
			ACL:    types.ObjectCannedACLPublicReadWrite,
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		requireAnonGetEventually(getTgt(), bucket, "prw.txt", http.StatusOK, body)
	}, ginkgo.NodeTimeout(60*time.Second))

	ginkgo.It("propagates public-read on CopyObject from a private source", func(ctx context.Context) {
		bucket := createSpecBucket(getTgt(), "copyacl")
		cli := getClient()
		body := "copy source payload"

		// Private source (no ACL header).
		_, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("src.txt"),
			Body:   strings.NewReader(body),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		_, err = cli.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String("dst.txt"),
			CopySource: aws.String(bucket + "/src.txt"),
			ACL:        types.ObjectCannedACLPublicRead,
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Destination got the explicit public-read; source remains private.
		requireAnonGetEventually(getTgt(), bucket, "dst.txt", http.StatusOK, body)
		requireAnonGetStatusEventually(getTgt(), bucket, "src.txt", http.StatusForbidden)
	}, ginkgo.NodeTimeout(60*time.Second))

	ginkgo.It("does not copy ACL on CopyObject without an ACL header", func(ctx context.Context) {
		bucket := createSpecBucket(getTgt(), "copynoacl")
		cli := getClient()
		body := "public source payload"

		// Public-read source.
		_, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("src.txt"),
			Body:   strings.NewReader(body),
			ACL:    types.ObjectCannedACLPublicRead,
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Copy with no ACL header: canned ACL is not inherited (S3 default =
		// private). Destination is private.
		_, err = cli.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String("dst.txt"),
			CopySource: aws.String(bucket + "/src.txt"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		requireAnonGetStatusEventually(getTgt(), bucket, "dst.txt", http.StatusForbidden)
	}, ginkgo.NodeTimeout(60*time.Second))

	// (B) Fail-closed reconciliation — RED before the fix (server returns 200
	// today, must become 501 NotImplemented).
	ginkgo.It("rejects PutObject with authenticated-read canned ACL", func(ctx context.Context) {
		bucket := createSpecBucket(getTgt(), "putauthread")
		cli := getClient()

		_, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("obj.txt"),
			Body:   strings.NewReader("x"),
			ACL:    types.ObjectCannedACLAuthenticatedRead,
		})
		requireS3ErrorCode(err, "NotImplemented")
	}, ginkgo.NodeTimeout(60*time.Second))

	ginkgo.It("rejects PutObject with bucket-owner-full-control canned ACL", func(ctx context.Context) {
		bucket := createSpecBucket(getTgt(), "putbofc")
		cli := getClient()

		_, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("obj.txt"),
			Body:   strings.NewReader("x"),
			ACL:    types.ObjectCannedACLBucketOwnerFullControl,
		})
		requireS3ErrorCode(err, "NotImplemented")
	}, ginkgo.NodeTimeout(60*time.Second))

	ginkgo.It("rejects PutObject carrying an x-amz-grant-read header", func(ctx context.Context) {
		bucket := createSpecBucket(getTgt(), "putgrantread")
		cli := getClient()

		_, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket:    aws.String(bucket),
			Key:       aws.String("obj.txt"),
			Body:      strings.NewReader("x"),
			GrantRead: aws.String("id=anybody"),
		})
		requireS3ErrorCode(err, "NotImplemented")
	}, ginkgo.NodeTimeout(60*time.Second))

	ginkgo.It("rejects CopyObject with authenticated-read canned ACL", func(ctx context.Context) {
		bucket := createSpecBucket(getTgt(), "copyauthread")
		cli := getClient()

		_, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("src.txt"),
			Body:   strings.NewReader("x"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		_, err = cli.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String("dst.txt"),
			CopySource: aws.String(bucket + "/src.txt"),
			ACL:        types.ObjectCannedACLAuthenticatedRead,
		})
		requireS3ErrorCode(err, "NotImplemented")
	}, ginkgo.NodeTimeout(60*time.Second))
}

// requireAnonGetEventually asserts an unsigned HTTP GET returns wantStatus with
// the expected body. ACL metadata propagation across cluster nodes is
// eventually consistent, so the assertion is wrapped in Eventually and rotates
// across nodes (mirrors putSSEAESObjectEventually).
func requireAnonGetEventually(tgt s3Target, bucket, key string, wantStatus int, wantBody string) {
	ginkgo.GinkgoHelper()
	gomega.Eventually(func() error {
		return anonGetExpect(tgt, bucket, key, wantStatus, &wantBody)
	}, 30*time.Second, 500*time.Millisecond).Should(gomega.Succeed())
}

// requireAnonGetStatusEventually asserts an unsigned HTTP GET returns wantStatus
// (body unchecked) consistently across all nodes.
func requireAnonGetStatusEventually(tgt s3Target, bucket, key string, wantStatus int) {
	ginkgo.GinkgoHelper()
	gomega.Eventually(func() error {
		return anonGetExpect(tgt, bucket, key, wantStatus, nil)
	}, 30*time.Second, 500*time.Millisecond).Should(gomega.Succeed())
}

// anonGetExpect performs an unsigned GET against every node and fails if any
// node disagrees with wantStatus (or, when wantBody is non-nil, the body).
func anonGetExpect(tgt s3Target, bucket, key string, wantStatus int, wantBody *string) error {
	for i := 0; i < tgt.nodes; i++ {
		resp, err := http.Get(tgt.endpoint(i) + "/" + bucket + "/" + key)
		if err != nil {
			return err
		}
		data, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != wantStatus {
			return fmt.Errorf("node %d: status %d != want %d", i, resp.StatusCode, wantStatus)
		}
		if wantBody != nil && string(data) != *wantBody {
			return fmt.Errorf("node %d: body %q != want %q", i, string(data), *wantBody)
		}
	}
	return nil
}
