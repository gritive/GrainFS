package e2e

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// nfsTarget abstracts an NFS4-capable grainfs fixture.
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
	createBkt  func(testing.TB, string)
}

// uniqueExport creates a per-case bucket + NFS export and registers cleanup.
// Returns (bucket, export-generation).
func (tgt *nfsTarget) uniqueExport(t testing.TB, caseName string) (string, uint64) {
	t.Helper()
	id := tgt.caseSeq.Add(1)
	bucket := fmt.Sprintf("nfs-%s-%s-%d", tgt.name, sanitizeForBucket(caseName), id)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if tgt.cluster != nil {
		tgt.cluster.GrantAdminOnBuckets(bucket)
		requireMRCreateBucketEventually(t, ctx, tgt.cluster, bucket)
	} else {
		tgt.createBkt(t, bucket)
	}

	created := runNfsExportJSONOnDataDir(t, tgt.dataDir(tgt.leaderIdx), "add", bucket)
	gomega.Expect(created.Bucket).To(gomega.Equal(bucket))
	gomega.Expect(created.Generation).NotTo(gomega.BeZero())

	ginkgo.DeferCleanup(func() {
		// Best-effort: bucket may already be gone (e.g. delete-cascade case
		// removed the export). Use --quiet and ignore non-zero exit.
		_, _ = runCLI(t, tgt.dataDir(tgt.leaderIdx), "nfs", "export", "remove", bucket, "--quiet")
	})
	return bucket, created.Generation
}

// newSingleNodeNFSTarget starts a single-node fixture owned by the caller's
// Ginkgo context/spec cleanup.
func newSingleNodeNFSTarget(t testing.TB) *nfsTarget {
	t.Helper()
	tgt := newDedicatedSingleNodeS3Target(t, []string{
		"--nfs-write-buffer-idle", "1s",
	})
	return &nfsTarget{
		name:       "single",
		nfsAddr:    func(i int) string { return fmt.Sprintf("127.0.0.1:%d", tgt.nfsPort) },
		dataDir:    func(i int) string { return tgt.dataDir },
		nodeCount:  1,
		leaderIdx:  0,
		s3Endpoint: func(i int) string { return tgt.endpoint(i) },
		s3Client:   func(i int) *s3.Client { return tgt.pickNode(i) },
		accessKey:  tgt.accessKey,
		secretKey:  tgt.secretKey,
		createBkt:  tgt.createBkt,
		isCluster:  false,
	}
}

// newSharedClusterNFSTarget starts a cluster owned by the caller's Ginkgo
// context/spec cleanup.
func newSharedClusterNFSTarget(t testing.TB) *nfsTarget {
	t.Helper()
	c := startStaticMRClusterWithOptions(t, 3, mrClusterOptions{
		disableNBD:    true,
		FastBootstrap: true,
	})
	c.nodeCount = 3
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
		createBkt: func(t testing.TB, bucket string) {
			c.GrantAdminOnBuckets(bucket)
			requireMRCreateBucketEventually(t, context.Background(), c, bucket)
		},
		isCluster: true,
		cluster:   c,
	}
}

// listNfsExportsOnDataDir is the dataDir-parameterized form of listNfsExports.
// Used by matrix cases that need to query exports on a specific cluster node's
// dataDir. The single-fixture listNfsExports remains for non-matrix callers.
func listNfsExportsOnDataDir(t testing.TB, dataDir string) []e2eNfsExport {
	t.Helper()
	out, code := runCLI(t, dataDir, "nfs", "export", "list", "--json")
	gomega.Expect(code).To(gomega.Equal(0), "%s", out)
	return parseNfsExportList(t, out)
}
