package e2e

import (
	"context"
	"fmt"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// icebergTarget abstracts an iceberg-capable grainfs fixture. Per-case
// isolation: by namespace for DDL-shape cases, by per-case bucket for
// audit cases.
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

// uniqueNamespace returns a fresh namespace name and registers Cleanup that
// best-effort drops the schema. Use as per-case isolation unit on iceberg
// targets when the test creates iceberg DDL.
func (tgt *icebergTarget) uniqueNamespace(t *testing.T, caseName string) string {
	t.Helper()
	id := tgt.caseSeq.Add(1)
	ns := fmt.Sprintf("ns_%s_%s_%d", tgt.name, sanitizeForBucket(caseName), id)
	t.Cleanup(func() {
		tgt.runExecBestEffort(fmt.Sprintf(
			"DROP SCHEMA IF EXISTS grainfs_iceberg.%s CASCADE;", ns))
	})
	return ns
}

// runSQL executes a single-row SELECT and asserts the scanned column equals want.
func (tgt *icebergTarget) runSQL(t *testing.T, query, want string) {
	t.Helper()
	runDuckDBIcebergSQLWithCreds(t, tgt.endpoint(0), tgt.accessKey, tgt.secretKey, query, want)
}

// runExec executes a statement (no return) and asserts success.
func (tgt *icebergTarget) runExec(t *testing.T, query string) {
	t.Helper()
	runDuckDBIcebergExecWithCreds(t, tgt.endpoint(0), tgt.accessKey, tgt.secretKey, query)
}

// runExecBestEffort is runExec without testing.T fatal — swallows all errors.
// Used by t.Cleanup paths.
func (tgt *icebergTarget) runExecBestEffort(stmt string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "duckdb", "-csv", "-noheader", "-c",
		duckDBIcebergSQL(tgt.endpoint(0), tgt.accessKey, tgt.secretKey, stmt))
	_ = cmd.Run()
}

func (tgt *icebergTarget) createBucketWithAdminPolicy(t *testing.T, bucket string) {
	t.Helper()
	createBucketWithAdminPolicyAttachViaUDSAny(t, tgt.dataDirs, tgt.saID, bucket, tgt.s3Client(0))
}

// newSingleNodeIcebergTarget boots a single grainfs node with the iceberg
// catalog enabled. startIcebergE2EServer registers its own t.Cleanup for stop.
func newSingleNodeIcebergTarget(t *testing.T) *icebergTarget {
	t.Helper()
	dataDir := shortTempDir(t)
	raftPort := freePort()
	encKeyFile := makeSharedEncryptionKeyFile(t)
	server := startIcebergE2EServer(t, dataDir, raftPort, encKeyFile)

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

// newSharedClusterIcebergTarget reuses the shared mrCluster fixture.
func newSharedClusterIcebergTarget(t *testing.T) *icebergTarget {
	t.Helper()
	c := getOrInitSharedMRCluster(t)
	tgt := &icebergTarget{
		name:      "cluster3",
		endpoint:  func(i int) string { return c.httpURLs[i%c.nodeCount] },
		s3Client:  func(i int) *s3.Client { return ecS3Client(c.httpURLs[i%c.nodeCount], c.accessKey, c.secretKey) },
		accessKey: c.accessKey,
		secretKey: c.secretKey,
		dataDirs:  c.dataDirs,
		saID:      c.saID,
		isCluster: true,
		cluster:   c,
	}
	tgt.createBucketWithAdminPolicy(t, "grainfs-tables")
	requireIcebergClusterS3Ready(t, c, "grainfs-tables")
	return tgt
}

// --- audit-enabled variants (for the audit matrix cases) ---

// newSingleNodeIcebergTargetWithAudit boots a single node with --audit-iceberg.
func newSingleNodeIcebergTargetWithAudit(t *testing.T, commitInterval time.Duration) *icebergTarget {
	t.Helper()
	dataDir := shortTempDir(t)
	raftPort := freePort()
	encKeyFile := makeSharedEncryptionKeyFile(t)
	server := startIcebergE2EServerWithExtraArgs(t, dataDir, raftPort, encKeyFile,
		"--audit-iceberg=true",
		"--audit-commit-interval", commitInterval.String(),
	)

	bootstrap, _ := bootstrapAdminViaUDSAnyResult(t, []string{dataDir}, 60*time.Second)
	ak, sk := bootstrap.AccessKey, bootstrap.SecretKey
	// NOTE: grainfs-audit is an internal bucket — the audit committer creates
	// and owns it. Tests must not create it via the public S3 API (returns 403
	// AccessDenied). Matches the pre-matrix single-node audit test behavior.

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
func newSharedClusterIcebergTargetWithAudit(t *testing.T, commitInterval time.Duration) *icebergTarget {
	t.Helper()
	cluster := startStaticMRClusterWithOptions(t, 3, mrClusterOptions{
		disableNFS4: true,
		disableNBD:  true,
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
