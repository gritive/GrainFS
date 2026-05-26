package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/gritive/GrainFS/internal/iamadmin"
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
func (tgt *icebergTarget) uniqueNamespace(t testing.TB, caseName string) string {
	t.Helper()
	id := tgt.caseSeq.Add(1)
	ns := fmt.Sprintf("ns_%s_%s_%d", tgt.name, sanitizeForBucket(caseName), id)
	ginkgo.DeferCleanup(func() {
		tgt.runExecBestEffort(fmt.Sprintf(
			"DROP SCHEMA IF EXISTS grainfs_iceberg.%s CASCADE;", ns))
	})
	return ns
}

// runSQL executes a single-row SELECT and asserts the scanned column equals want.
func (tgt *icebergTarget) runSQL(t testing.TB, query, want string) {
	t.Helper()
	runDuckDBIcebergSQLWithCreds(t, tgt.endpoint(0), tgt.accessKey, tgt.secretKey, query, want)
}

// runExec executes a statement (no return) and asserts success.
func (tgt *icebergTarget) runExec(t testing.TB, query string) {
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
// a t.Cleanup to delete it. Returns (saID, accessKey, secretKey). Mirrors the
// shape of iamAdminTarget.uniqueSA but lives on icebergTarget so OAuth e2e
// cases can mint bearers without composing the iamAdminTarget abstraction.
func (tgt *icebergTarget) adminCreateSA(t testing.TB, namePrefix string) (saID, ak, sk string) {
	t.Helper()
	sock := tgt.adminSockPath()
	name := "sa-" + sanitizeForBucket(t.Name()) + "-" + sanitizeForBucket(namePrefix)
	out := iamCreateSA(t, sock, name)
	ginkgo.DeferCleanup(func() {
		iamSADelete(t, sock, out.SAID)
	})
	// Wait for Raft propagation on cluster targets — without this, MintToken
	// happy-path can race the key apply and return 401 (indistinguishable
	// from a real wrong-secret 401, masking both flakes and false passes).
	iamWaitKeyReady(t, tgt.endpoint(0), out.AccessKey, out.SecretKey, 30*time.Second)
	return out.SAID, out.AccessKey, out.SecretKey
}

// adminAttachPolicy attaches the named policy to saID via the admin UDS and
// registers a t.Cleanup that detaches on test exit. policyName must be a
// known builtin (e.g. "readwrite", "readonly", "bucket-admin") or a policy
// previously installed via PolicyPut.
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

// uniqueWarehouse creates a warehouse-backed bucket via the admin UDS (with
// admin policy attached to tgt.saID so the bootstrap SA can manage it) and
// registers a t.Cleanup that best-effort deletes the bucket. The bucket name
// is derived from t.Name() + suffix to stay stable per sub-test while keeping
// re-runs collision-free across t.Name() variants.
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

// mintToken POSTs grant_type=client_credentials to the iceberg OAuth token
// endpoint. Returns (jwt, 200) on success; ("", non-200) on auth failure.
// Transport/IO/decode errors fail the test via Gomega assertions.
func (tgt *icebergTarget) mintToken(t testing.TB, clientID, clientSecret, warehouse string) (string, int) {
	t.Helper()
	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("client_id", clientID)
	form.Set("client_secret", clientSecret)
	form.Set("scope", "PRINCIPAL_ROLE:"+warehouse)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		tgt.endpoint(0)+"/iceberg/v1/oauth/tokens", strings.NewReader(form.Encode()))
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "mintToken: build request")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "mintToken: HTTP do")
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "mintToken: read body")

	if resp.StatusCode != http.StatusOK {
		return "", resp.StatusCode
	}
	var out struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("mintToken: decode 200 body: %v (body=%s)", err, string(body))
	}
	return out.AccessToken, resp.StatusCode
}

// newSingleNodeIcebergTarget boots a single grainfs node with the iceberg
// catalog enabled. startIcebergE2EServer registers its own t.Cleanup for stop.
func newSingleNodeIcebergTarget(t testing.TB) *icebergTarget {
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

// newSharedClusterIcebergTarget starts a cluster owned by the caller's
// Ginkgo context/spec cleanup.
func newSharedClusterIcebergTarget(t testing.TB) *icebergTarget {
	t.Helper()
	c := startStaticMRClusterWithOptions(t, 3, mrClusterOptions{
		disableNBD:    true,
		FastBootstrap: true,
	})
	c.nodeCount = 3
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
func newSingleNodeIcebergTargetWithAudit(t testing.TB, commitInterval time.Duration) *icebergTarget {
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
func newSharedClusterIcebergTargetWithAudit(t testing.TB, commitInterval time.Duration) *icebergTarget {
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
