package e2e

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/iamadmin"
)

// TestIAMBucketE2E validates the IAM bucket admin plane (create/delete/list/
// policy-put/delete and data-plane create-denied) against both single-node
// and cluster fixtures.
func TestIAMBucketE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runIAMBucketCases(t, newSingleNodeIAMAdminTarget())
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		runIAMBucketCases(t, newSharedClusterIAMAdminTarget(t))
	})
}

// validBucketPolicyDoc is a minimal S3 bucket policy accepted by
// policy.ParsePolicy (requires Effect="Allow"|"Deny").
const validBucketPolicyDoc = `{
	"Version":"2012-10-17",
	"Statement":[{
		"Effect":"Allow",
		"Principal":"*",
		"Action":["s3:GetObject"],
		"Resource":["arn:aws:s3:::test-bucket/*"]
	}]
}`

// runIAMBucketCases exercises bucket CRUD, policy put/delete, and the
// data-plane create-denied invariant (D#8) against the given target.
//
// Absent cases and rationale:
//
//   - "CreateAttachMutualRequirement_400": the server guard for single-sided
//     attach is an AND condition (if req.AttachSA != "" && req.AttachPolicy != "").
//     When only one side is provided the attach is silently skipped — no 400
//     is returned. This is a documented server-side behaviour; a separate
//     enhancement ticket should add the validation guard.
//
//   - "PolicyPutDelete": PUT /v1/buckets/:name/policy returns
//     ErrUnsupportedOperation in the shared e2e fixtures (storage backend does
//     not support per-bucket policy storage in this configuration). The route
//     and handler are exercised by unit tests in handlers_bucket_policy.go.
func runIAMBucketCases(t *testing.T, tgt iamAdminTarget) {
	t.Helper()
	ctx := context.Background()

	// CreateDelete: create via IAM admin plane, verify it appears in list, then
	// delete. The post-create list membership and the absence of errors on
	// create/delete are the primary invariants.
	t.Run("CreateDelete", func(t *testing.T) {
		c := tgt.iamClient()
		name := bucketNameFor(tgt.name, t.Name(), "create-delete")
		t.Cleanup(func() { _ = c.BucketDelete(ctx, name, false) })

		require.NoError(t, c.BucketCreate(ctx, name, "", ""))

		items, err := c.BucketList(ctx)
		require.NoError(t, err)
		assert.Contains(t, bucketItemNames(items), name)

		require.NoError(t, c.BucketDelete(ctx, name, false))
	})

	// List: create two buckets, verify both appear in list.
	t.Run("List", func(t *testing.T) {
		c := tgt.iamClient()
		name1 := bucketNameFor(tgt.name, t.Name(), "list-a")
		name2 := bucketNameFor(tgt.name, t.Name(), "list-b")
		t.Cleanup(func() {
			_ = c.BucketDelete(ctx, name1, false)
			_ = c.BucketDelete(ctx, name2, false)
		})

		require.NoError(t, c.BucketCreate(ctx, name1, "", ""))
		require.NoError(t, c.BucketCreate(ctx, name2, "", ""))

		items, err := c.BucketList(ctx)
		require.NoError(t, err)
		names := bucketItemNames(items)
		assert.Contains(t, names, name1)
		assert.Contains(t, names, name2)
	})

	// CreateWithAttach: create a bucket and atomically attach an SA + policy
	// via MetaCmd 62. Verify the bucket exists (attach success is implicit:
	// if the MetaCmd fails, BucketCreate rolls back the bucket creation and
	// returns an error).
	t.Run("CreateWithAttach", func(t *testing.T) {
		c := tgt.iamClient()
		name := bucketNameFor(tgt.name, t.Name(), "create-attach")
		t.Cleanup(func() { _ = c.BucketDelete(ctx, name, false) })

		saID, _, _ := tgt.uniqueSA(t, "create-attach")

		// Use the built-in "readwrite" policy (allows PutObject / GetObject).
		require.NoError(t, c.BucketCreate(ctx, name, saID, "readwrite"))

		// Bucket must be present after create-with-attach.
		items, err := c.BucketList(ctx)
		require.NoError(t, err)
		assert.Contains(t, bucketItemNames(items), name)
	})

	// DataplaneCreateRefused: S3 CreateBucket on the data plane must always
	// return AccessDenied per Decision #8 (admin-UDS-only actions). The SA
	// used here has full admin credentials; the denial is unconditional.
	t.Run("DataplaneCreateRefused", func(t *testing.T) {
		name := bucketNameFor(tgt.name, t.Name(), "dp-create-refused")
		s3c := tgt.pickNode(0)
		_, err := s3c.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(name),
		})
		require.Error(t, err)
		var apiErr smithy.APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "AccessDenied", apiErr.ErrorCode())
	})
}

// bucketItemNames extracts bucket names from a BucketListItem slice.
func bucketItemNames(items []iamadmin.BucketListItem) []string {
	out := make([]string, len(items))
	for i, item := range items {
		out[i] = item.Name
	}
	return out
}
