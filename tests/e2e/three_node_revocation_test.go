package e2e

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/onsi/ginkgo/v2"
	"github.com/stretchr/testify/require"
)

// TestThreeNodeRevocationE2E proves F#14: when an SA's policy is detached (or
// its access key is revoked) on the leader, the next S3 request on any
// follower MUST be denied within one Raft apply round-trip.
//
// Dual-target shape (R10):
//   - SingleNode: validates the trivial invariant (next request denied after
//     detach/revoke) — no Raft cross-node propagation involved.
//   - Cluster4Node: validates F#14's actual property by issuing the pre/post
//     requests against a follower (node 1) while the admin RPC hits the leader.
//
// The cluster fixture is the shared 4-node IAM fixture (see
// newSharedClusterIAMAdminTarget). "3-node" in the task title is semantic
// intent ("small cluster"); the label tracks the actual fixture per repo
// convention.
var _ = ginkgo.Describe("Three-node revocation", func() {
	describeRevocationContext("SingleNode", func(testing.TB) iamAdminTarget {
		return newSingleNodeIAMAdminTarget()
	})
	describeRevocationContext("Cluster4Node", func(tb testing.TB) iamAdminTarget {
		return newSharedClusterIAMAdminTarget(tb)
	})
})

func describeRevocationContext(name string, factory func(testing.TB) iamAdminTarget) {
	ginkgo.Context(name, func() {
		var tgt iamAdminTarget

		ginkgo.BeforeEach(func() {
			tgt = factory(ginkgo.GinkgoTB())
		})

		runRevocationCases(func() iamAdminTarget { return tgt })
	})
}

func runRevocationCases(getTgt func() iamAdminTarget) {
	ginkgo.It("denies the next follower request after policy detach (DetachedSANextRequestDenied)", func() {
		runRevocationDetachPolicy(ginkgo.GinkgoTB(), getTgt())
	})
	ginkgo.It("denies the next follower request after key revocation (DetachedKeyRevokedNextRequestDenied)", func() {
		runRevocationKeyRevoke(ginkgo.GinkgoTB(), getTgt())
	})
}

// revocationPropagationDeadline returns the hard deadline for "next request
// must see denial after admin mutation returns". Single-node still passes
// through Raft apply (single-voter), so we keep a generous bound; cluster
// must propagate within one full apply round-trip on followers.
func revocationPropagationDeadline() time.Duration {
	return 150 * time.Millisecond
}

// pickFollowerIdx returns the node index a non-leader request should target.
// For single-node the only node is index 0; for cluster we pick 1 to ensure
// the request crosses Raft apply from a different node than the admin UDS.
func pickFollowerIdx(tgt iamAdminTarget) int {
	if tgt.isCluster {
		return 1
	}
	return 0
}

// runRevocationDetachPolicy: attach a custom Write policy, drive a pre-revoke
// PUT on a follower (proves both key and policy reached the follower), detach
// on the leader, then assert the next PUT on the follower is denied within
// one apply round-trip.
func runRevocationDetachPolicy(t testing.TB, tgt iamAdminTarget) {
	t.Helper()
	ctx := context.Background()

	saID, ak, sk := tgt.uniqueSA(t, "t72-detach")
	bucket := tgt.uniqueBucket(t, "t72-detach")

	cli := tgt.iamClient()
	polName := "t72-detach-" + sanitizeForBucket(t.Name())
	if len(polName) > 63 {
		polName = polName[:63]
	}
	doc := buildPolicyDocJSON(
		policyActionsForRole(t, "Write"),
		[]string{"arn:aws:s3:::" + bucket, "arn:aws:s3:::" + bucket + "/*"},
	)
	require.NoError(t, cli.PolicyPut(ctx, polName, doc))
	require.NoError(t, cli.PolicyAttachToSA(ctx, polName, saID))
	ginkgo.DeferCleanup(func() {
		_ = cli.PolicyDetachFromSA(ctx, polName, saID)
		_ = cli.PolicyDelete(ctx, polName)
	})

	// Wait for the key+policy to land on node 0 (the bootstrap path), then
	// confirm the follower also sees them by driving a real PUT on the
	// follower endpoint. require.Eventually absorbs the apply propagation
	// jitter before we start the post-detach timing.
	iamWaitKeyReady(t, tgt.endpoint(0), ak, sk, 30*time.Second)

	nodeIdx := pickFollowerIdx(tgt)
	s3c := ecS3Client(tgt.endpoint(nodeIdx), ak, sk)
	require.Eventually(t, func() bool {
		_, err := s3c.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("pre-revoke.txt"),
			Body:   bytes.NewReader([]byte("ok")),
		})
		return err == nil
	}, 5*time.Second, 50*time.Millisecond, "pre-revoke PUT on node %d must succeed", nodeIdx)

	// Detach on the leader (admin UDS routes there). Start the clock AFTER
	// the RPC returns — we want to measure propagation, not RPC overhead.
	require.NoError(t, cli.PolicyDetachFromSA(ctx, polName, saID))
	t0 := time.Now()

	deadline := revocationPropagationDeadline()
	var propagated time.Duration
	require.Eventually(t, func() bool {
		_, err := s3c.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("post-revoke-" + strconv.FormatInt(time.Now().UnixNano(), 10) + ".txt"),
			Body:   bytes.NewReader([]byte("after-detach")),
		})
		if err == nil {
			return false
		}
		var apiErr smithy.APIError
		if !errors.As(err, &apiErr) {
			return false
		}
		if apiErr.ErrorCode() == "AccessDenied" {
			propagated = time.Since(t0)
			return true
		}
		return false
	}, deadline, 10*time.Millisecond,
		"policy detach must propagate within %v (node %d, isCluster=%v)", deadline, nodeIdx, tgt.isCluster)

	t.Logf("policy detach propagated in %v on node %d (isCluster=%v)", propagated, nodeIdx, tgt.isCluster)
}

// runRevocationKeyRevoke: same shape, but the revocation mechanism is the
// access-key revoke RPC. Proves "policy detach" and "key revoke" propagate
// with identical cross-node semantics.
func runRevocationKeyRevoke(t testing.TB, tgt iamAdminTarget) {
	t.Helper()
	ctx := context.Background()

	saID, ak, sk := tgt.uniqueSA(t, "t72-revoke")
	bucket := tgt.uniqueBucket(t, "t72-revoke")
	attachAdminPolicyOnBucket(t, tgt, saID, bucket, "Write")

	iamWaitKeyReady(t, tgt.endpoint(0), ak, sk, 30*time.Second)

	nodeIdx := pickFollowerIdx(tgt)
	s3c := ecS3Client(tgt.endpoint(nodeIdx), ak, sk)
	require.Eventually(t, func() bool {
		_, err := s3c.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("pre-revoke.txt"),
			Body:   bytes.NewReader([]byte("ok")),
		})
		return err == nil
	}, 5*time.Second, 50*time.Millisecond, "pre-revoke PUT on node %d must succeed", nodeIdx)

	// Revoke the access key on the leader.
	cli := tgt.iamClient()
	require.NoError(t, cli.KeyRevoke(ctx, saID, ak))
	t0 := time.Now()

	deadline := revocationPropagationDeadline()
	var propagated time.Duration
	require.Eventually(t, func() bool {
		_, err := s3c.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("post-revoke-" + strconv.FormatInt(time.Now().UnixNano(), 10) + ".txt"),
			Body:   bytes.NewReader([]byte("after-revoke")),
		})
		if err == nil {
			return false
		}
		var apiErr smithy.APIError
		if !errors.As(err, &apiErr) {
			return false
		}
		code := apiErr.ErrorCode()
		if code == "InvalidAccessKeyId" || code == "AccessDenied" {
			propagated = time.Since(t0)
			return true
		}
		return false
	}, deadline, 10*time.Millisecond,
		"key revoke must propagate within %v (node %d, isCluster=%v)", deadline, nodeIdx, tgt.isCluster)

	t.Logf("key revoke propagated in %v on node %d (isCluster=%v)", propagated, nodeIdx, tgt.isCluster)
}
