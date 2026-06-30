// S3 Content-MD5 PUT validation e2e (target table-driven).
//
// A client may send the S3 Content-MD5 header (base64(md5(body))) on PutObject;
// the server must commit the object only when the digest matches the received
// body and reject a mismatch with 400 BadDigest before anything is committed.
// Validation now happens in a beforeCommit hook on the streaming PUT path
// (replacing a post-spool check), and BadDigest is a documented public-API
// behavior, so it requires e2e coverage. The same case set runs against a
// single-node fixture and a 4-node cluster fixture.
package e2e

import (
	"bytes"
	"context"
	"crypto/md5" //nolint:gosec // MD5 is the S3-mandated Content-MD5/ETag hash; no security use.
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("S3 Content-MD5", ginkgo.Label("s3"), func() {
	describeContentMD5Context("SingleNode", newSingleNodeS3Target)
	describeContentMD5Context("Cluster4Node", func() s3Target {
		return newSharedClusterS3Target(ginkgo.GinkgoTB())
	})
})

func describeContentMD5Context(name string, factory func() s3Target) {
	ginkgo.Context(name, ginkgo.Ordered, func() {
		var tgt s3Target

		ginkgo.BeforeAll(func() {
			tgt = factory()
		})

		runContentMD5Cases(func() s3Target { return tgt })
	})
}

func runContentMD5Cases(getTgt func() s3Target) {
	// 17 MiB > the 16 MiB segment size, so the large cases exercise the
	// multi-segment streaming PUT path (the path whose digest hook moved).
	const largeSize = 17 << 20

	ginkgo.It("accepts a correct Content-MD5 and round-trips the body", func(ctx context.Context) {
		tgt := getTgt()
		bucket := createSpecBucket(tgt, "md5-ok-small")
		body := []byte("content-md5 round trip")

		putWithContentMD5Eventually(ctx, tgt, bucket, "ok.txt", body, contentMD5(body))
		requireObjectBodyEventually(ctx, tgt, bucket, "ok.txt", body)
	}, ginkgo.NodeTimeout(90*time.Second))

	ginkgo.It("accepts a correct Content-MD5 for a multi-segment body", func(ctx context.Context) {
		tgt := getTgt()
		bucket := createSpecBucket(tgt, "md5-ok-large")
		body := patternedBytes(largeSize)

		putWithContentMD5Eventually(ctx, tgt, bucket, "ok-large.bin", body, contentMD5(body))
		requireObjectBodyEventually(ctx, tgt, bucket, "ok-large.bin", body)
	}, ginkgo.NodeTimeout(120*time.Second))

	ginkgo.It("rejects a wrong Content-MD5 with BadDigest and commits nothing", func(ctx context.Context) {
		tgt := getTgt()
		bucket := createSpecBucket(tgt, "md5-bad-small")
		body := []byte("this single-segment body must be rejected")
		// Well-formed base64 of a *different* body's digest: valid-but-wrong
		// → BadDigest (a malformed value would instead be InvalidDigest).
		wrong := contentMD5([]byte("totally different bytes"))

		expectBadDigest(ctx, tgt, bucket, "bad.txt", body, wrong)
		requireObjectNotFound(ctx, tgt, bucket, "bad.txt")
	}, ginkgo.NodeTimeout(90*time.Second))

	ginkgo.It("rejects a wrong Content-MD5 on a multi-segment body and commits nothing", func(ctx context.Context) {
		tgt := getTgt()
		bucket := createSpecBucket(tgt, "md5-bad-large")
		body := patternedBytes(largeSize)
		wrong := contentMD5([]byte("totally different bytes"))

		expectBadDigest(ctx, tgt, bucket, "bad-large.bin", body, wrong)
		requireObjectNotFound(ctx, tgt, bucket, "bad-large.bin")
	}, ginkgo.NodeTimeout(120*time.Second))
}

// contentMD5 returns the value of the S3 Content-MD5 header for body:
// base64(md5(body)). The aws-sdk-go-v2 PutObjectInput.ContentMD5 field takes
// exactly this base64 form (NOT hex).
func contentMD5(body []byte) string {
	sum := md5.Sum(body) //nolint:gosec // S3-mandated Content-MD5 hash.
	return base64.StdEncoding.EncodeToString(sum[:])
}

// patternedBytes returns n deterministic bytes (cheap to generate, stable
// digest) for exercising the multi-segment path.
func patternedBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i * 31)
	}
	return b
}

// putWithContentMD5Eventually PUTs body with the given Content-MD5 and expects
// success, retrying across nodes to tolerate cluster write-readiness lag. A
// fresh bytes.Reader is created per attempt so retries re-read from the start.
func putWithContentMD5Eventually(ctx context.Context, tgt s3Target, bucket, key string, body []byte, md5b64 string) {
	ginkgo.GinkgoHelper()
	gomega.Eventually(func() error {
		var err error
		for i := 0; i < tgt.nodes; i++ {
			_, err = tgt.pickNode(i).PutObject(ctx, &s3.PutObjectInput{
				Bucket:     aws.String(bucket),
				Key:        aws.String(key),
				Body:       bytes.NewReader(body),
				ContentMD5: aws.String(md5b64),
			})
			if err == nil {
				return nil
			}
		}
		return err
	}, 60*time.Second, 500*time.Millisecond).Should(gomega.Succeed())
}

// requireObjectBodyEventually GETs the object and asserts the returned bytes
// match want (compared by length + digest to keep failure output sane),
// retrying across nodes to tolerate read-after-write replication lag.
func requireObjectBodyEventually(ctx context.Context, tgt s3Target, bucket, key string, want []byte) {
	ginkgo.GinkgoHelper()
	wantMD5 := contentMD5(want)
	gomega.Eventually(func() error {
		var lastErr error
		for i := 0; i < tgt.nodes; i++ {
			out, err := tgt.pickNode(i).GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			if err != nil {
				lastErr = err
				continue
			}
			data, rerr := io.ReadAll(out.Body)
			_ = out.Body.Close()
			if rerr != nil {
				lastErr = rerr
				continue
			}
			if len(data) != len(want) || contentMD5(data) != wantMD5 {
				lastErr = fmt.Errorf("body mismatch: got len=%d md5=%s, want len=%d md5=%s",
					len(data), contentMD5(data), len(want), wantMD5)
				continue
			}
			return nil
		}
		if lastErr == nil {
			lastErr = errors.New("no node returned the object")
		}
		return lastErr
	}, 60*time.Second, 500*time.Millisecond).Should(gomega.Succeed())
}

// expectBadDigest PUTs body with a wrong Content-MD5 and asserts the server
// rejects it with the modeled S3 error code "BadDigest" (HTTP 400). Retrying
// is safe: a wrong digest always fails, so the loop only absorbs transient
// non-BadDigest errors while the path warms, then settles on BadDigest.
func expectBadDigest(ctx context.Context, tgt s3Target, bucket, key string, body []byte, wrongMD5 string) {
	ginkgo.GinkgoHelper()
	cli := tgt.pickNode(0)
	gomega.Eventually(func() string {
		_, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(key),
			Body:       bytes.NewReader(body),
			ContentMD5: aws.String(wrongMD5),
		})
		if err == nil {
			return "<nil: PUT with wrong Content-MD5 unexpectedly succeeded>"
		}
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			return apiErr.ErrorCode()
		}
		return err.Error()
	}, 30*time.Second, 500*time.Millisecond).Should(gomega.Equal("BadDigest"))
}

// requireObjectNotFound asserts the object was never committed: a GET returns
// 404 NoSuchKey. The object was rejected on every node, so a single read on
// node 0 is authoritative (no replication lag for a never-written key).
func requireObjectNotFound(ctx context.Context, tgt s3Target, bucket, key string) {
	ginkgo.GinkgoHelper()
	_, err := tgt.pickNode(0).GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	requireS3ErrorCode(err, "NoSuchKey")
}
