// Generic e2e target abstraction.
//
// s3Target lets a single case set run against both a single-node fixture and
// a multi-node cluster fixture. The same test bodies call tgt.pickNode(0) for
// the S3 client and tgt.createBkt(t, name) for bucket setup; cluster-specific
// wiring (admin grants, leader selection, IAM key propagation) is hidden
// inside the factory.
package e2e

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

// s3Target abstracts a fixture (single-node or cluster) for e2e tests that
// only exercise the public S3 surface. For HTTP-raw tests (form upload,
// presigned URLs) callers also need an endpoint URL and the access/secret
// pair used to sign requests.
type s3Target struct {
	name      string
	nodes     int
	pickNode  func(i int) *s3.Client
	endpoint  func(i int) string
	accessKey string
	secretKey string
	createBkt func(t *testing.T, bucket string)
	// uniqueBucket creates a bucket with a name derived from t.Name() + case,
	// sanitized to S3 spec (lowercase/hyphen, 3-63 chars). Auto-registers
	// t.Cleanup(DeleteBucket). Returns the actual bucket name used.
	uniqueBucket func(t *testing.T, caseName string) string
	isCluster    bool
}

func newSingleNodeS3Target() s3Target {
	return s3Target{
		name:  "single",
		nodes: 1,
		pickNode: func(i int) *s3.Client {
			return testS3Client
		},
		endpoint: func(i int) string {
			return testServerURL
		},
		accessKey: testAccessKey,
		secretKey: testSecretKey,
		createBkt: func(t *testing.T, bucket string) {
			createBucket(t, bucket)
		},
		uniqueBucket: func(t *testing.T, caseName string) string {
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

func newClusterS3Target(t *testing.T, nodes int) s3Target {
	t.Helper()
	c := startE2ECluster(t, e2eClusterOptions{
		Nodes:      nodes,
		Mode:       ClusterModeDynamicJoin,
		ClusterKey: "E2E-S3-OP-KEY",
		LogPrefix:  "grainfs-s3op",
		DisableNFS: true,
		DisableNBD: true,
	})

	// Wait for IAM key propagation across all nodes; otherwise non-leader
	// nodes 403 on first request.
	for i := range c.procs {
		iamWaitKeyReady(t, c.httpURLs[i], c.accessKey, c.secretKey, 30*time.Second)
	}

	return s3Target{
		name:  "cluster4",
		nodes: nodes,
		pickNode: func(i int) *s3.Client {
			return c.S3Client(i % nodes)
		},
		endpoint: func(i int) string {
			return c.httpURLs[i%nodes]
		},
		accessKey: c.accessKey,
		secretKey: c.secretKey,
		createBkt: func(t *testing.T, bucket string) {
			c.GrantAdminOnBuckets(bucket)
			createBucketWithClient(t, c.S3Client(c.leaderIdx), bucket)
		},
		uniqueBucket: func(t *testing.T, caseName string) string {
			name := bucketNameFor("cluster4", t.Name(), caseName)
			c.GrantAdminOnBuckets(name)
			createBucketWithClient(t, c.S3Client(c.leaderIdx), name)
			t.Cleanup(func() {
				c.S3Client(c.leaderIdx).DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(name)})
			})
			return name
		},
		isCluster: true,
	}
}

var bucketSanitizeRE = regexp.MustCompile(`[^a-z0-9-]`)

func sanitizeForBucket(s string) string {
	s = strings.ToLower(s)
	s = strings.ReplaceAll(s, "/", "-")
	s = bucketSanitizeRE.ReplaceAllString(s, "")
	return s
}

// bucketNameFor produces a S3-spec compliant bucket name (3-63 chars,
// lowercase, hyphens). When the t.Name()+case combination exceeds 50 chars
// it falls back to <tgt>-<case>-<sha8> to keep names stable per test.
func bucketNameFor(tgtName, testName, caseName string) string {
	full := fmt.Sprintf("%s-%s-%s", tgtName, sanitizeForBucket(testName), sanitizeForBucket(caseName))
	if len(full) > 50 {
		sum := sha256.Sum256([]byte(testName + "|" + caseName))
		full = fmt.Sprintf("%s-%s-%s", tgtName, sanitizeForBucket(caseName), hex.EncodeToString(sum[:4]))
	}
	if len(full) < 3 {
		full = full + "-x"
	}
	return full
}

func TestBucketNameFor(t *testing.T) {
	got := bucketNameFor("single", "TestS3FooE2E/SingleNode/Put", "basic")
	require.Equal(t, "single-tests3fooe2e-singlenode-put-basic", got)
	require.LessOrEqual(t, len(got), 63)

	long := bucketNameFor("cluster4", "TestS3VersioningE2E/Cluster4Node/ListObjectVersionsWithDeleteMarker", "basic")
	require.LessOrEqual(t, len(long), 63)
	require.GreaterOrEqual(t, len(long), 3)
	require.Regexp(t, `^cluster4-basic-[0-9a-f]{8}$`, long)
}
