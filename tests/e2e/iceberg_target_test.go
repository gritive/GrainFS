package e2e

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/gritive/GrainFS/internal/iamadmin"
)

// icebergTarget abstracts a grainfs fixture used for audit-iceberg tests.
// It no longer carries iceberg REST/OAuth capabilities — only the S3 surface
// needed by audit and policy-decision e2e cases.
type icebergTarget struct {
	name      string
	endpoint  func(i int) string
	s3Client  func(i int) *s3.Client
	accessKey string
	secretKey string
	dataDirs  []string
	saID      string
	isCluster bool
	caseSeq   atomic.Int64
	cluster   *mrCluster // non-nil for cluster fixtures
}

func (tgt *icebergTarget) createBucketWithAdminPolicy(t testing.TB, bucket string) {
	t.Helper()
	// Idempotent: shared cluster fixture is reused across multiple iceberg
	// tests, each of which constructs a fresh icebergTarget. HeadBucket-skip
	// avoids 409 conflict on the second-and-later targets that share the same
	// underlying cluster.
	if _, err := tgt.s3Client(0).HeadBucket(context.Background(), &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	}); err == nil {
		return
	}
	createBucketWithAdminPolicyAttachViaUDSAny(t, tgt.dataDirs, tgt.saID, bucket, tgt.s3Client(0))
}

// adminSockPath returns the admin UDS path for the leader node. Cluster
// targets route through the elected leader; single-node uses dataDirs[0].
func (tgt *icebergTarget) adminSockPath() string {
	if tgt.isCluster && tgt.cluster != nil && tgt.cluster.leaderIdx >= 0 &&
		tgt.cluster.leaderIdx < len(tgt.cluster.dataDirs) {
		return tgt.cluster.dataDirs[tgt.cluster.leaderIdx] + "/admin.sock"
	}
	return tgt.dataDirs[0] + "/admin.sock"
}

// adminCreateSA creates a fresh ServiceAccount via the admin UDS and registers
// a t.Cleanup to delete it. Returns (saID, accessKey, secretKey).
func (tgt *icebergTarget) adminCreateSA(t testing.TB, namePrefix string) (saID, ak, sk string) {
	t.Helper()
	sock := tgt.adminSockPath()
	name := "sa-" + sanitizeForBucket(t.Name()) + "-" + sanitizeForBucket(namePrefix)
	out := iamCreateSA(t, sock, name)
	ginkgo.DeferCleanup(func() {
		iamSADelete(t, sock, out.SAID)
	})
	// Wait for Raft propagation on cluster targets.
	iamWaitKeyReady(t, tgt.endpoint(0), out.AccessKey, out.SecretKey, 30*time.Second)
	return out.SAID, out.AccessKey, out.SecretKey
}

// adminAttachPolicy attaches the named policy to saID via the admin UDS.
func (tgt *icebergTarget) adminAttachPolicy(t testing.TB, saID, policyName string) {
	t.Helper()
	cli := iamadmin.NewClientForURL(tgt.adminSockPath())
	ctx := context.Background()
	gomega.Expect(cli.PolicyAttachToSA(ctx, policyName, saID)).To(gomega.Succeed(),
		"PolicyAttachToSA %s -> %s", policyName, saID)
	ginkgo.DeferCleanup(func() {
		_ = cli.PolicyDetachFromSA(ctx, policyName, saID)
	})
}

// --- audit-enabled variants (for the audit matrix cases) ---

// newSingleNodeIcebergTargetWithAudit boots a single node with --audit-iceberg.
func newSingleNodeIcebergTargetWithAudit(t testing.TB, commitInterval time.Duration) *icebergTarget {
	t.Helper()
	dataDir := shortTempDir(t)
	raftPort := freePort()
	server := startIcebergE2EServerWithExtraArgs(t, dataDir, raftPort,
		"--audit-iceberg=true",
		"--audit-commit-interval", commitInterval.String(),
	)

	bootstrap, _ := bootstrapAdminViaUDSAnyResult(t, []string{dataDir}, 60*time.Second)
	ak, sk := bootstrap.AccessKey, bootstrap.SecretKey

	tgt := &icebergTarget{
		name:      "single-audit",
		endpoint:  func(i int) string { return server.endpoint },
		s3Client:  func(i int) *s3.Client { return ecS3Client(server.endpoint, ak, sk) },
		accessKey: ak,
		secretKey: sk,
		dataDirs:  []string{dataDir},
		saID:      bootstrap.SAID,
		isCluster: false,
	}
	tgt.createBucketWithAdminPolicy(t, "grainfs-tables")
	return tgt
}

// newSharedClusterIcebergTargetWithAudit boots a dedicated cluster (NOT shared)
// because audit flags can't be retrofitted onto a running cluster fixture.
func newSharedClusterIcebergTargetWithAudit(t testing.TB, commitInterval time.Duration) *icebergTarget {
	t.Helper()
	cluster := startStaticMRClusterWithOptions(t, 3, mrClusterOptions{
		ExtraArgs: []string{
			"--audit-iceberg=true",
			"--audit-commit-interval", commitInterval.String(),
		},
	})
	if cluster.nodeCount == 0 {
		cluster.nodeCount = len(cluster.httpURLs)
	}
	cluster.GrantAdminOnBuckets("grainfs-audit")
	tgt := &icebergTarget{
		name:     "cluster-audit",
		endpoint: func(i int) string { return cluster.httpURLs[i%cluster.nodeCount] },
		s3Client: func(i int) *s3.Client {
			return ecS3Client(cluster.httpURLs[i%cluster.nodeCount], cluster.accessKey, cluster.secretKey)
		},
		accessKey: cluster.accessKey,
		secretKey: cluster.secretKey,
		dataDirs:  cluster.dataDirs,
		saID:      cluster.saID,
		isCluster: true,
		cluster:   cluster,
	}
	tgt.createBucketWithAdminPolicy(t, "grainfs-tables")
	return tgt
}

// newSingleNodeIcebergTarget boots a single grainfs node.
// Used by protocol-credential smoke tests.
func newSingleNodeIcebergTarget(t testing.TB) *icebergTarget {
	t.Helper()
	dataDir := shortTempDir(t)
	raftPort := freePort()
	server := startIcebergE2EServer(t, dataDir, raftPort)

	bootstrap, _ := bootstrapAdminViaUDSAnyResult(t, []string{dataDir}, 60*time.Second)
	ak, sk := bootstrap.AccessKey, bootstrap.SecretKey

	tgt := &icebergTarget{
		name:      "single",
		endpoint:  func(i int) string { return server.endpoint },
		s3Client:  func(i int) *s3.Client { return ecS3Client(server.endpoint, ak, sk) },
		accessKey: ak,
		secretKey: sk,
		dataDirs:  []string{dataDir},
		saID:      bootstrap.SAID,
		isCluster: false,
	}
	tgt.createBucketWithAdminPolicy(t, "grainfs-tables")
	return tgt
}

// uniqueWarehouse creates a warehouse-backed bucket via the admin UDS.
func (tgt *icebergTarget) uniqueWarehouse(t testing.TB, suffix string) string {
	t.Helper()
	name := bucketNameFor(tgt.name, t.Name(), suffix)
	createBucketWithAdminPolicyAttachViaUDSAny(t, tgt.dataDirs, tgt.saID, name, tgt.s3Client(0))
	ginkgo.DeferCleanup(func() {
		_, _ = tgt.s3Client(0).DeleteBucket(context.Background(), &s3.DeleteBucketInput{
			Bucket: aws.String(name),
		})
	})
	return name
}
