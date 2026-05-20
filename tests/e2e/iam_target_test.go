package e2e

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/iamadmin"
)

// iamAdminTarget abstracts a fixture (single-node or cluster) for e2e tests
// that exercise the IAM admin plane (UDS) directly. Parallel to s3Target for
// the data plane; callers use iamClient() to drive policy/SA/bucket ops and
// uniqueSA/uniqueBucket for per-case isolation.
type iamAdminTarget struct {
	name          string
	nodes         int
	adminSockPath func() string
	pickNode      func(i int) *s3.Client
	accessKey     string
	secretKey     string
	// uniqueSA creates a fresh ServiceAccount scoped to the calling sub-test.
	// Returns (saID, accessKey, secretKey). Registers t.Cleanup to delete the SA.
	uniqueSA func(t *testing.T, caseName string) (saID, ak, sk string)
	// uniqueBucket creates a bucket and registers t.Cleanup(DeleteBucket).
	// Returns the actual bucket name used.
	uniqueBucket func(t *testing.T, caseName string) string
	isCluster    bool
}

// iamClient returns an *iamadmin.Client wired to the admin UDS for this target.
func (tgt iamAdminTarget) iamClient() *iamadmin.Client {
	tp, _ := adminapi.NewTransport(tgt.adminSockPath())
	return &iamadmin.Client{Transport: tp}
}

// newSingleNodeIAMAdminTarget returns an iamAdminTarget backed by the
// package-global single-node grainfs fixture. No server is started — the
// global fixture from TestMain is reused.
func newSingleNodeIAMAdminTarget() iamAdminTarget {
	return iamAdminTarget{
		name:  "single",
		nodes: 1,
		adminSockPath: func() string {
			return testServerDataDir + "/admin.sock"
		},
		pickNode: func(i int) *s3.Client {
			return testS3Client
		},
		accessKey: testAccessKey,
		secretKey: testSecretKey,
		uniqueSA: func(t *testing.T, caseName string) (string, string, string) {
			t.Helper()
			sock := testServerDataDir + "/admin.sock"
			name := "sa-" + sanitizeForBucket(caseName)
			out := iamCreateSA(t, sock, name)
			t.Cleanup(func() {
				iamSADelete(t, sock, out.SAID)
			})
			return out.SAID, out.AccessKey, out.SecretKey
		},
		uniqueBucket: func(t *testing.T, caseName string) string {
			t.Helper()
			name := bucketNameFor("single", t.Name(), caseName)
			createBucket(t, name)
			t.Cleanup(func() {
				testS3Client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(name)})
			})
			return name
		},
		isCluster: false,
	}
}

// newSharedClusterIAMAdminTarget returns an iamAdminTarget backed by the
// shared 4-node cluster fixture (same fixture as newSharedClusterS3Target).
func newSharedClusterIAMAdminTarget(t *testing.T) iamAdminTarget {
	t.Helper()
	c := getOrInitSharedCluster(t)
	return iamAdminTarget{
		name:  "cluster4",
		nodes: 4,
		adminSockPath: func() string {
			return c.dataDirs[c.leaderIdx] + "/admin.sock"
		},
		pickNode: func(i int) *s3.Client {
			return c.S3Client(i % 4)
		},
		accessKey: c.accessKey,
		secretKey: c.secretKey,
		uniqueSA: func(t *testing.T, caseName string) (string, string, string) {
			t.Helper()
			sock := c.dataDirs[c.leaderIdx] + "/admin.sock"
			name := "sa-" + sanitizeForBucket(caseName)
			out := iamCreateSA(t, sock, name)
			t.Cleanup(func() {
				iamSADelete(t, sock, out.SAID)
			})
			return out.SAID, out.AccessKey, out.SecretKey
		},
		uniqueBucket: func(t *testing.T, caseName string) string {
			t.Helper()
			name := bucketNameFor("cluster4", t.Name(), caseName)
			if c.wildcardAdmin {
				createBucketWithClient(t, c.S3Client(c.leaderIdx), name)
			} else {
				createBucketWithAdminPolicyAttachViaUDSAny(t, c.dataDirs, c.saID, name, c.S3Client(c.leaderIdx))
			}
			t.Cleanup(func() {
				c.S3Client(c.leaderIdx).DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(name)})
			})
			return name
		},
		isCluster: true,
	}
}
