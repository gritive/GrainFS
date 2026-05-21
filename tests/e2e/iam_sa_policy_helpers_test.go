package e2e

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/iamadmin"
)

// attachAdminPolicyOnBucket creates a custom inline policy granting the named
// role on the named bucket, attaches it to saID, and registers cleanup to
// detach + delete the policy. Mirrors legacy iamGrantPut(sock, saID, bucket, role)
// semantics: "alice gets Read on bucket-X" → custom policy {s3 read actions} on
// arn:aws:s3:::bucket-X and arn:aws:s3:::bucket-X/*.
//
// role: "Read" → s3:GetObject/HeadObject/ListBucket/...
//
//	"Write" → s3:GetObject/PutObject/DeleteObject/...
//	"Admin" → s3:* on arn:aws:s3:::bucket-X[/*]
//
// bucket="*" uses Resource:"*" (wildcard across all buckets).
func attachAdminPolicyOnBucket(t testing.TB, tgt iamAdminTarget, saID, bucket, role string) {
	t.Helper()
	actions := policyActionsForRole(t, role)

	var resources []string
	if bucket == "*" {
		resources = []string{"*"}
	} else {
		resources = []string{
			"arn:aws:s3:::" + bucket,
			"arn:aws:s3:::" + bucket + "/*",
		}
	}

	doc := buildPolicyDocJSON(actions, resources)
	polName := "test-" + sanitizeForBucket(t.Name()) + "-" + sanitizeForBucket(saID) + "-" + bucket + "-" + strings.ToLower(role)
	// When the name exceeds 63 chars, use a content hash to avoid truncation
	// collisions (e.g. two attachAdminPolicyOnBucket calls with different
	// buckets but the same long t.Name()/saID prefix would otherwise produce
	// identical truncated names, causing the second PolicyPut to silently
	// overwrite the first).
	if len(polName) > 63 {
		sum := sha256.Sum256([]byte(polName))
		polName = "test-" + hex.EncodeToString(sum[:12])
	}

	cli := iamadmin.NewClientForURL(tgt.adminSockPath())
	ctx := context.Background()
	require.NoError(t, cli.PolicyPut(ctx, polName, doc), "PolicyPut %s", polName)
	require.NoError(t, cli.PolicyAttachToSA(ctx, polName, saID), "PolicyAttachToSA %s->%s", polName, saID)
	t.Cleanup(func() {
		_ = cli.PolicyDetachFromSA(ctx, polName, saID)
		_ = cli.PolicyDelete(ctx, polName)
	})
}

// policyActionsForRole returns the IAM action list for the given role name.
// Action lists are derived from internal/iam/builtin/policies.go equivalents.
func policyActionsForRole(t testing.TB, role string) []string {
	t.Helper()
	switch role {
	case "Read":
		return []string{
			"s3:GetObject", "s3:HeadObject", "s3:ListBucket",
			"s3:GetBucketLocation", "s3:GetBucketVersioning", "s3:ListBucketVersions",
			"s3:GetBucketObjectLockConfiguration", "s3:GetBucketLifecycleConfiguration",
			"s3:GetObjectRetention", "s3:GetObjectTagging",
		}
	case "Write":
		return []string{
			"s3:GetObject", "s3:HeadObject", "s3:ListBucket", "s3:GetBucketLocation",
			"s3:GetBucketVersioning", "s3:PutBucketVersioning", "s3:ListBucketVersions",
			"s3:GetBucketObjectLockConfiguration", "s3:GetBucketLifecycleConfiguration",
			"s3:GetObjectRetention", "s3:PutObjectRetention",
			"s3:PutObject", "s3:DeleteObject", "s3:CopyObject",
			"s3:AbortMultipartUpload", "s3:ListMultipartUploads",
			"s3:GetObjectTagging", "s3:PutObjectTagging", "s3:DeleteObjectTagging",
		}
	case "Admin":
		return []string{"s3:*"}
	default:
		t.Fatalf("policyActionsForRole: unknown role %q", role)
		return nil
	}
}

// buildPolicyDocJSON encodes a minimal single-statement Allow policy.
func buildPolicyDocJSON(actions, resources []string) []byte {
	type stmt struct {
		Effect   string   `json:"Effect"`
		Action   []string `json:"Action"`
		Resource []string `json:"Resource"`
	}
	type doc struct {
		Version   string `json:"Version"`
		Statement []stmt `json:"Statement"`
	}
	b, _ := json.Marshal(doc{
		Version: "2012-10-17",
		Statement: []stmt{{
			Effect:   "Allow",
			Action:   actions,
			Resource: resources,
		}},
	})
	return b
}

// s3ClientForSA constructs an aws-sdk-go-v2 *s3.Client targeting tgt's node-0
// endpoint, signed with (ak, sk). Used to assert that a freshly-created SA's
// credentials work end-to-end against the S3 plane.
func s3ClientForSA(tgt iamAdminTarget, ak, sk string) *s3.Client {
	ep := tgt.endpoint(0)
	return s3.New(s3.Options{
		BaseEndpoint: aws.String(ep),
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider(ak, sk, ""),
		UsePathStyle: true,
		HTTPClient:   e2eNoKeepAliveHTTPClient(0),
	})
}
