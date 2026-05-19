package e2e

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

// nfsTarget abstracts an NFS4-capable grainfs fixture. Cluster variant
// shares the mrCluster fixture with icebergTarget via getOrInitSharedMRCluster.
type nfsTarget struct {
	name      string
	nfsAddr   func(i int) string
	dataDir   func(i int) string
	nodeCount int
	leaderIdx int
	caseSeq   atomic.Int64
	isCluster bool
	cluster   *mrCluster // non-nil for cluster fixtures

	// S3 access for tests that verify export effects via S3.
	s3Endpoint func(i int) string
	s3Client   func(i int) *s3.Client
	accessKey  string
	secretKey  string
}

// uniqueExport creates a per-case bucket + NFS export and registers cleanup.
// Returns (bucket, export-generation).
func (tgt *nfsTarget) uniqueExport(t *testing.T, caseName string) (string, uint64) {
	t.Helper()
	id := tgt.caseSeq.Add(1)
	bucket := fmt.Sprintf("nfs-%s-%s-%d", tgt.name, sanitizeForBucket(caseName), id)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if tgt.cluster != nil {
		tgt.cluster.GrantAdminOnBuckets(bucket)
		requireMRCreateBucketEventually(t, ctx, tgt.cluster, bucket)
	} else {
		createBucket(t, bucket)
	}

	created := runNfsExportJSONOnDataDir(t, tgt.dataDir(tgt.leaderIdx), "add", bucket)
	require.Equal(t, bucket, created.Bucket)
	require.NotZero(t, created.Generation)

	t.Cleanup(func() {
		_ = runNfsExportJSONOnDataDir(t, tgt.dataDir(tgt.leaderIdx), "remove", bucket)
	})
	return bucket, created.Generation
}

// newSingleNodeNFSTarget reuses the existing single-node fixture from TestMain.
func newSingleNodeNFSTarget(t *testing.T) *nfsTarget {
	t.Helper()
	return &nfsTarget{
		name:       "single",
		nfsAddr:    func(i int) string { return fmt.Sprintf("127.0.0.1:%d", testServerNFSPort) },
		dataDir:    func(i int) string { return testServerDataDir },
		nodeCount:  1,
		leaderIdx:  0,
		s3Endpoint: func(i int) string { return testServerURL },
		s3Client:   func(i int) *s3.Client { return testS3Client },
		accessKey:  testAccessKey,
		secretKey:  testSecretKey,
		isCluster:  false,
	}
}

// newSharedClusterNFSTarget reuses the shared mrCluster (NFS+iceberg).
func newSharedClusterNFSTarget(t *testing.T) *nfsTarget {
	t.Helper()
	c := getOrInitSharedMRCluster(t)
	return &nfsTarget{
		name: "cluster3",
		nfsAddr: func(i int) string {
			return fmt.Sprintf("127.0.0.1:%d", c.nfs4Ports[i%c.nodeCount])
		},
		dataDir:    func(i int) string { return c.dataDirs[i%c.nodeCount] },
		nodeCount:  c.nodeCount,
		leaderIdx:  c.leaderIdx,
		s3Endpoint: func(i int) string { return c.httpURLs[i%c.nodeCount] },
		s3Client: func(i int) *s3.Client {
			return ecS3Client(c.httpURLs[i%c.nodeCount], c.accessKey, c.secretKey)
		},
		accessKey: c.accessKey,
		secretKey: c.secretKey,
		isCluster: true,
		cluster:   c,
	}
}

// listNfsExportsOnDataDir is the dataDir-parameterized form of listNfsExports.
// Used by matrix cases that need to query exports on a specific cluster node's
// dataDir. The single-fixture listNfsExports remains for non-matrix callers.
func listNfsExportsOnDataDir(t *testing.T, dataDir string) []e2eNfsExport {
	t.Helper()
	out, code := runCLI(t, dataDir, "nfs", "export", "list", "--json")
	require.Equalf(t, 0, code, "%s", out)
	return parseNfsExportList(t, out)
}
