// Generic e2e target abstraction.
//
// s3Target lets a single case set run against both a single-node fixture and
// a multi-node cluster fixture. The same test bodies call tgt.pickNode(0) for
// the S3 client and tgt.createBkt(t, name) for bucket setup; cluster-specific
// wiring (admin grants, leader selection, IAM key propagation) is hidden
// inside the factory.
package e2e

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
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
	isCluster bool
	cluster   *e2eCluster // non-nil for cluster fixtures
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
		isCluster: true,
		cluster:   c,
	}
}

// newClusterS3TargetWithExtraArgs mirrors newClusterS3Target but passes
// extraArgs verbatim to every node's grainfs serve command-line.
func newClusterS3TargetWithExtraArgs(t *testing.T, nodes int, extraArgs []string) s3Target {
	t.Helper()
	c := startE2ECluster(t, e2eClusterOptions{
		Nodes:      nodes,
		Mode:       ClusterModeDynamicJoin,
		ClusterKey: "E2E-S3-OP-KEY",
		LogPrefix:  "grainfs-s3op",
		DisableNFS: true,
		DisableNBD: true,
		ExtraArgs:  extraArgs,
	})

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
		isCluster: true,
		cluster:   c,
	}
}
