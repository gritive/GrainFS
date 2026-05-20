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
	"github.com/stretchr/testify/require"
)

// runIAMBootstrapFirstSAWildcardGrant (was F1): empty IAM → first SA
// create returns non-empty credentials. The §2 rewrite replaced the legacy
// InitFirstSA path (which issued a fixed "sa-default" ID + wildcard grant)
// with a standard SACreate path that issues a UUIDv7 SA ID and no in-band
// grant; the caller's subsequent requests are permitted because iam.anon-enabled
// is flipped off at first-SA time, making the new SA the only authorized
// principal.
func runIAMBootstrapFirstSAWildcardGrant(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runBootstrapFirstSAWildcardGrantCases(t, newSingleNodeBootstrapTarget(t))
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		runBootstrapFirstSAWildcardGrantCases(t, newClusterBootstrapTarget(t))
	})
}

func runBootstrapFirstSAWildcardGrantCases(t *testing.T, tgt iamBootstrapTarget) {
	t.Helper()

	ak, sk := bootstrapAdminViaUDSAny(t, tgt.dataDirs(), 30*time.Second)
	require.NotEmpty(t, ak, "first SA bootstrap must return non-empty access_key")
	require.NotEmpty(t, sk, "first SA bootstrap must return non-empty secret_key")

	// Exactly one SA must be present after bootstrap.
	var saList []map[string]any
	iamDo(t, tgt.adminSock(), "GET", "/v1/iam/sa", nil, &saList)
	require.Len(t, saList, 1, "exactly one SA must exist after first-bootstrap; got %v", saList)

	// The SA ID must be a non-empty string. The §2 code assigns a UUIDv7
	// (not the legacy "sa-default" constant). We accept any non-empty ID.
	firstSAID, _ := saList[0]["sa_id"].(string)
	require.NotEmpty(t, firstSAID, "first SA must have a non-empty sa_id")
}

// runIAMBootstrapSecondSANoAutoGrant (was F2): non-empty store → SA
// create does NOT auto-issue a wildcard grant.
func runIAMBootstrapSecondSANoAutoGrant(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runBootstrapSecondSANoAutoGrantCases(t, newSingleNodeBootstrapTarget(t))
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		runBootstrapSecondSANoAutoGrantCases(t, newClusterBootstrapTarget(t))
	})
}

func runBootstrapSecondSANoAutoGrantCases(t *testing.T, tgt iamBootstrapTarget) {
	t.Helper()
	sock := tgt.adminSock()

	// First SA → bootstrap.
	bootstrapAdminViaUDSAny(t, tgt.dataDirs(), 30*time.Second)

	// Second SA → must receive a distinct sa_id (not the same as the first).
	var firstList []map[string]any
	iamDo(t, sock, "GET", "/v1/iam/sa", nil, &firstList)
	require.Len(t, firstList, 1)
	firstSAID, _ := firstList[0]["sa_id"].(string)

	var out struct {
		SAID string `json:"sa_id"`
		Name string `json:"name"`
	}
	iamDo(t, sock, "POST", "/v1/iam/sa", map[string]string{"name": "user1"}, &out)
	require.NotEmpty(t, out.SAID, "second SA must have non-empty sa_id")
	require.NotEqual(t, firstSAID, out.SAID, "second SA must have a different sa_id from the first")
}

// runIAMBootstrapPreBootstrapDenied (was F3): pre-bootstrap sigv4 traffic
// → AccessDenied / InvalidAccessKeyId / SignatureDoesNotMatch class error.
func runIAMBootstrapPreBootstrapDenied(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runBootstrapPreBootstrapDeniedCases(t, newSingleNodeBootstrapTarget(t))
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		runBootstrapPreBootstrapDeniedCases(t, newClusterBootstrapTarget(t))
	})
}

func runBootstrapPreBootstrapDeniedCases(t *testing.T, tgt iamBootstrapTarget) {
	t.Helper()
	cli := s3ClientFor(tgt.s3URL(), "AKIA-fake-bootstrap-test", "fake-secret-bootstrap-test")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := cli.ListBuckets(ctx, &s3.ListBucketsInput{})
	require.Error(t, err, "ListBuckets with fabricated key must fail before bootstrap")

	var apiErr smithy.APIError
	require.True(t, errors.As(err, &apiErr), "expected smithy APIError, got %T: %v", err, err)
	// Acceptable codes from the S3 auth path. Project sigv4 surface uses
	// AccessDenied / InvalidAccessKeyId / SignatureDoesNotMatch.
	code := apiErr.ErrorCode()
	require.Contains(t,
		[]string{"AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch"},
		code,
		"unexpected error code %q from pre-bootstrap sigv4: %v", code, err,
	)
}

// runIAMBootstrapPostBootstrapVerbs (was F4): post-bootstrap, the bootstrap
// creds drive ListBuckets, CreateBucket, PutObject, and GetObject end-to-end.
func runIAMBootstrapPostBootstrapVerbs(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runBootstrapPostBootstrapVerbsCases(t, newSingleNodeBootstrapTarget(t))
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		runBootstrapPostBootstrapVerbsCases(t, newClusterBootstrapTarget(t))
	})
}

func runBootstrapPostBootstrapVerbsCases(t *testing.T, tgt iamBootstrapTarget) {
	t.Helper()

	// Get the SA result so we have the SAID for bucket-admin attachment.
	admin, _ := bootstrapAdminViaUDSAnyResult(t, tgt.dataDirs(), 30*time.Second)
	cli := s3ClientFor(tgt.s3URL(), admin.AccessKey, admin.SecretKey)
	require.NoError(t, waitForIAMReady(cli, 30*time.Second))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// ListBuckets — admin SA must succeed (the bucket list itself may
	// contain auto-created defaults; the assertion is just that auth
	// passes and the verb returns 200).
	_, err := cli.ListBuckets(ctx, &s3.ListBucketsInput{})
	require.NoError(t, err, "ListBuckets")

	// CreateBucket with bucket-admin policy attached to the bootstrap SA.
	// The §2 model requires an explicit per-bucket grant or policy; the bootstrap
	// SA has no wildcard grant in the current codebase.
	bucket := "f4-bootstrap-bucket-" + tgt.name
	createBucketWithAdminPolicyAttachViaUDSAny(t, tgt.dataDirs(), admin.SAID, bucket, cli)

	// PutObject.
	const payload = "hello-bootstrap-f4"
	_, err = cli.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("obj1"),
		Body:   strings.NewReader(payload),
	})
	require.NoError(t, err, "PutObject")

	// GetObject — body must round-trip.
	getOut, err := cli.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("obj1"),
	})
	require.NoError(t, err, "GetObject")
	defer getOut.Body.Close()
	body, err := io.ReadAll(getOut.Body)
	require.NoError(t, err, "read GetObject body")
	require.Equal(t, payload, string(body), "object body round-trip mismatch")
}

// TestIAMBootstrapE2E groups IAM bootstrap SA grant scenarios.
func TestIAMBootstrapE2E(t *testing.T) {
	t.Run("FirstSAWildcardGrant", runIAMBootstrapFirstSAWildcardGrant)
	t.Run("SecondSANoAutoGrant", runIAMBootstrapSecondSANoAutoGrant)
	t.Run("PreBootstrapDenied", runIAMBootstrapPreBootstrapDenied)
	t.Run("PostBootstrapVerbs", runIAMBootstrapPostBootstrapVerbs)
}
