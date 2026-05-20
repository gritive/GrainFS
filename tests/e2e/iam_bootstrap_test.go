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

// TestIAMBootstrapE2E groups IAM bootstrap SA grant scenarios.
// Each branch (single-node, cluster) spawns exactly one fixture and runs all
// four lifecycle stages sequentially. State accumulates across sub-cases —
// each relies on the side-effects of the previous one, mirroring how operators
// experience the bootstrap dance in practice.
func TestIAMBootstrapE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runIAMBootstrapCases(t, newSingleNodeBootstrapTarget(t))
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		runIAMBootstrapCases(t, newClusterBootstrapTarget(t))
	})
}

// runIAMBootstrapCases drives one bootstrap-lifecycle fixture through the
// full sequence of pre-bootstrap, first-SA, second-SA, and post-bootstrap
// assertions. State accumulates: each sub-case relies on the side-effects
// of the previous one. This is intentional — it matches how operators
// experience the bootstrap dance in practice.
func runIAMBootstrapCases(t *testing.T, tgt iamBootstrapTarget) {
	t.Helper()

	// Shared state captured across sub-cases.
	var bootstrapAK, bootstrapSK string
	var bootstrapSAID string

	// PreBootstrapDenied must run before bootstrap so the IAM store is empty.
	t.Run("PreBootstrapDenied", func(t *testing.T) {
		cli := s3ClientFor(tgt.s3URL(), "AKIA-fake-bootstrap-test", "fake-secret-bootstrap-test")

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := cli.ListBuckets(ctx, &s3.ListBucketsInput{})
		require.Error(t, err, "ListBuckets with fabricated key must fail before bootstrap")

		var apiErr smithy.APIError
		require.True(t, errors.As(err, &apiErr), "expected smithy APIError, got %T: %v", err, err)
		// Acceptable codes from the S3 auth path.
		code := apiErr.ErrorCode()
		require.Contains(t,
			[]string{"AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch"},
			code,
			"unexpected error code %q from pre-bootstrap sigv4: %v", code, err,
		)
	})

	// FirstSAWildcardGrant: empty IAM store → first SA create returns non-empty
	// credentials. The §2 rewrite assigns a UUIDv7 SA ID (not "sa-default") and
	// no in-band wildcard grant; the new SA becomes the only authorized principal.
	t.Run("FirstSAWildcardGrant", func(t *testing.T) {
		admin, _ := bootstrapAdminViaUDSAnyResult(t, tgt.dataDirs(), 30*time.Second)
		require.NotEmpty(t, admin.AccessKey, "first SA bootstrap must return non-empty access_key")
		require.NotEmpty(t, admin.SecretKey, "first SA bootstrap must return non-empty secret_key")

		bootstrapAK = admin.AccessKey
		bootstrapSK = admin.SecretKey
		bootstrapSAID = admin.SAID

		// Exactly one SA must be present after bootstrap.
		var saList []map[string]any
		iamDo(t, tgt.adminSock(), "GET", "/v1/iam/sa", nil, &saList)
		require.Len(t, saList, 1, "exactly one SA must exist after first-bootstrap; got %v", saList)

		// The SA ID must be a non-empty string (UUIDv7, not legacy "sa-default").
		require.NotEmpty(t, bootstrapSAID, "first SA must have a non-empty sa_id")
	})

	// SecondSANoAutoGrant: non-empty store → a second SA create does NOT
	// auto-issue a wildcard grant. It must receive a distinct sa_id.
	t.Run("SecondSANoAutoGrant", func(t *testing.T) {
		require.NotEmpty(t, bootstrapSAID, "prerequisite FirstSAWildcardGrant must run first")

		sock := tgt.adminSock()

		var out struct {
			SAID string `json:"sa_id"`
			Name string `json:"name"`
		}
		iamDo(t, sock, "POST", "/v1/iam/sa", map[string]string{"name": "user1"}, &out)
		require.NotEmpty(t, out.SAID, "second SA must have non-empty sa_id")
		require.NotEqual(t, bootstrapSAID, out.SAID, "second SA must have a different sa_id from the first")
	})

	// PostBootstrapVerbs: bootstrap creds drive ListBuckets, CreateBucket,
	// PutObject, and GetObject end-to-end.
	t.Run("PostBootstrapVerbs", func(t *testing.T) {
		require.NotEmpty(t, bootstrapAK, "prerequisite FirstSAWildcardGrant must run first")

		cli := s3ClientFor(tgt.s3URL(), bootstrapAK, bootstrapSK)
		require.NoError(t, waitForIAMReady(cli, 30*time.Second))

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// ListBuckets — admin SA must succeed.
		_, err := cli.ListBuckets(ctx, &s3.ListBucketsInput{})
		require.NoError(t, err, "ListBuckets")

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
	})
}
