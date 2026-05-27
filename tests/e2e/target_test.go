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
	"os"
	"os/exec"
	"regexp"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
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
	dataDir   string
	nfsPort   int
	nbdPort   int
	createBkt func(t testing.TB, bucket string)
	// uniqueBucket creates a bucket with a name derived from t.Name() + case,
	// sanitized to S3 spec (lowercase/hyphen, 3-63 chars). Auto-registers
	// t.Cleanup(DeleteBucket). Returns the actual bucket name used.
	uniqueBucket func(t testing.TB, caseName string) string
	// adminSockPath returns the path to the admin UDS for the "writable"
	// node — node-0 on single, the elected leader on cluster. Tests that
	// drive per-bucket admin config (e.g., pull-through upstream
	// registration) use it; vanilla S3-surface tests can ignore.
	adminSockPath func() string
	isCluster     bool
	cluster       *e2eCluster // non-nil for cluster fixtures
}

var (
	sharedSingleS3Target  s3Target
	sharedClusterS3Target s3Target
)

func initSharedS3Targets(t testing.TB) {
	t.Helper()

	sharedSingleS3Target = newDedicatedSingleNodeS3Target(t, nil)
	sharedSingleS3Target.name = "single"
	sharedSingleCreateBkt := sharedSingleS3Target.createBkt
	sharedSingleClient := sharedSingleS3Target.pickNode(0)
	sharedSingleS3Target.uniqueBucket = func(t testing.TB, caseName string) string {
		name := bucketNameFor("single", t.Name(), caseName)
		sharedSingleCreateBkt(t, name)
		ginkgo.DeferCleanup(func() {
			sharedSingleClient.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(name)})
		})
		return name
	}

	c := startE2ECluster(t, e2eClusterOptions{
		Nodes:      4,
		Mode:       ClusterModeDynamicJoin,
		ClusterKey: "E2E-S3-OP-KEY",
		LogPrefix:  "grainfs-s3op-shared",
		DisableNFS: true,
		DisableNBD: true,
	})
	for i := range c.procs {
		iamWaitKeyReady(t, c.httpURLs[i], c.accessKey, c.secretKey, 30*time.Second)
	}
	sharedClusterS3Target = s3TargetFromCluster(c)
}

func newSingleNodeS3Target() s3Target {
	gomega.Expect(sharedSingleS3Target.pickNode).NotTo(gomega.BeNil(), "shared single-node fixture must be initialized")
	return sharedSingleS3Target
}

var bucketSanitizeRE = regexp.MustCompile(`[^a-z0-9-]`)

func currentE2EClusterLeaderIdx(t testing.TB, c *e2eCluster) int {
	t.Helper()
	start := c.leaderIdx
	if start < 0 || start >= len(c.httpURLs) {
		start = 0
	}
	status := getStatusJSON(t, c.httpURLs[start])
	leaderID, _ := status["leader_id"].(string)
	for i := range c.httpURLs {
		if c.nodeID(i) == leaderID {
			c.leaderIdx = i
			return i
		}
	}
	return start
}

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

func newSharedClusterS3Target(t testing.TB) s3Target {
	t.Helper()
	gomega.Expect(sharedClusterS3Target.pickNode).NotTo(gomega.BeNil(), "shared cluster fixture must be initialized")
	return sharedClusterS3Target
}

func s3TargetFromCluster(c *e2eCluster) s3Target {
	return s3Target{
		name:  "cluster4",
		nodes: 4,
		pickNode: func(i int) *s3.Client {
			return c.S3Client(i % 4)
		},
		endpoint: func(i int) string {
			return c.httpURLs[i%4]
		},
		accessKey: c.accessKey,
		secretKey: c.secretKey,
		createBkt: func(t testing.TB, bucket string) {
			createBucketWithAdminPolicyAttachViaUDSAny(t, c.dataDirs, c.saID, bucket, c.S3Client(c.leaderIdx))
		},
		uniqueBucket: func(t testing.TB, caseName string) string {
			name := bucketNameFor("cluster4", t.Name(), caseName)
			createBucketWithAdminPolicyAttachViaUDSAny(t, c.dataDirs, c.saID, name, c.S3Client(c.leaderIdx))
			ginkgo.DeferCleanup(func() {
				c.S3Client(c.leaderIdx).DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(name)})
			})
			return name
		},
		adminSockPath: func() string {
			return c.dataDirs[currentE2EClusterLeaderIdx(ginkgo.GinkgoTB(), c)] + "/admin.sock"
		},
		isCluster: true,
		cluster:   c,
	}
}

// newClusterS3Target returns a DEDICATED (non-shared) cluster fixture. Use
// newSharedClusterS3Target for tests that don't mutate cluster topology;
// reserve this for tests that kill nodes, change CLI flags, or otherwise
// need an isolated cluster.
func newClusterS3Target(t testing.TB, nodes int) s3Target {
	return newClusterS3TargetWithExtraArgs(t, nodes, nil)
}

// newClusterS3TargetWithExtraArgs mirrors newClusterS3Target but passes
// extraArgs verbatim to every node's grainfs serve command-line.
func newClusterS3TargetWithExtraArgs(t testing.TB, nodes int, extraArgs []string) s3Target {
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
		createBkt: func(t testing.TB, bucket string) {
			createBucketWithAdminPolicyAttachViaUDSAny(t, c.dataDirs, c.saID, bucket, c.S3Client(c.leaderIdx))
		},
		uniqueBucket: func(t testing.TB, caseName string) string {
			name := bucketNameFor("cluster4", t.Name(), caseName)
			createBucketWithAdminPolicyAttachViaUDSAny(t, c.dataDirs, c.saID, name, c.S3Client(c.leaderIdx))
			ginkgo.DeferCleanup(func() {
				c.S3Client(c.leaderIdx).DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(name)})
			})
			return name
		},
		adminSockPath: func() string {
			return c.dataDirs[currentE2EClusterLeaderIdx(t, c)] + "/admin.sock"
		},
		isCluster: true,
		cluster:   c,
	}
}

// newDedicatedSingleNodeS3Target boots a fresh single-node grainfs with the
// given extra args. Use it for specs that need isolated ports, data, IAM
// bootstrap state, server flags, or cleanup.
//
// Lifetime: process is launched on call, terminated + tmpdir removed via
// t.Cleanup. Each call gets its own port + data dir.
func newDedicatedSingleNodeS3Target(t testing.TB, extraArgs []string) s3Target {
	t.Helper()

	dir, err := os.MkdirTemp("", "grainfs-e2e-single-dedicated-")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.DeferCleanup(func() {
		_ = removeE2EDir(dir)
	})

	port := freePort()
	nfsPort := freePort()
	nbdPort := freePort()
	args := []string{
		"serve", "--data", dir,
		"--port", fmt.Sprintf("%d", port),
		"--nfs4-port", fmt.Sprintf("%d", nfsPort),
		"--nbd-port", fmt.Sprintf("%d", nbdPort),
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
		"--cluster-key", "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899",
	}
	args = append(args, extraArgs...)

	cmd := exec.Command(getBinary(), args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	gomega.Expect(cmd.Start()).To(gomega.Succeed(), "start single-node grainfs")
	ginkgo.DeferCleanup(func() {
		terminateProcess(cmd)
	})

	gomega.Expect(waitForPortM(port, 30*time.Second)).To(gomega.Succeed(), "wait for HTTP port")

	admin, err := bootstrapAdminResultViaUDSForTestMain(dir, 30*time.Second)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "bootstrap admin SA via UDS")
	ak, sk := admin.AccessKey, admin.SecretKey
	if !bootstrapResultHasWildcardAdmin(admin) && admin.SAID != "" {
		gomega.Expect(grantAdminOnBucketViaUDSForTestMain(dir, admin.SAID, "default", 30*time.Second)).
			To(gomega.Succeed(), "grant default bucket to bootstrap SA")
	}

	gomega.Expect(patchSnapshotIntervalM(dir, "0s")).To(gomega.Succeed(), "disable auto-snapshot")

	endpoint := fmt.Sprintf("http://127.0.0.1:%d", port)
	client := ecS3Client(endpoint, ak, sk)
	gomega.Expect(waitForIAMReady(client, 30*time.Second)).To(gomega.Succeed(), "wait for IAM ready")

	return s3Target{
		name:  "single-dedicated",
		nodes: 1,
		pickNode: func(int) *s3.Client {
			return client
		},
		endpoint: func(int) string {
			return endpoint
		},
		accessKey: ak,
		secretKey: sk,
		dataDir:   dir,
		nfsPort:   nfsPort,
		nbdPort:   nbdPort,
		createBkt: func(t testing.TB, bucket string) {
			createBucketWithAdminPolicyAttachViaUDSAny(t, []string{dir}, admin.SAID, bucket, client)
		},
		uniqueBucket: func(t testing.TB, caseName string) string {
			name := bucketNameFor("single-dedicated", t.Name(), caseName)
			createBucketWithAdminPolicyAttachViaUDSAny(t, []string{dir}, admin.SAID, name, client)
			ginkgo.DeferCleanup(func() {
				client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(name)})
			})
			return name
		},
		adminSockPath: func() string {
			return dir + "/admin.sock"
		},
		isCluster: false,
	}
}

// newDedicatedCluster4NodeS3Target boots a per-test 4-node cluster. All
// runtime flags are caller-controlled via extraArgs (lifecycle-interval,
// etc.) — mirrors newDedicatedSingleNodeS3Target for single/cluster parity.
// Boot cost is the trade-off for being able to exercise versioning-dependent
// lifecycle behavior (DM reclaim) in a real cluster topology. Vanilla cluster
// lifecycle tests should keep using the package-global shared cluster
// fixture where boot cost dominates.
//
// The fixture's tgt.name is "cluster-4-dedicated" — sub-tests use this
// to discriminate cluster-only flows (e.g., DM sub-test).
func newDedicatedCluster4NodeS3Target(t testing.TB, extraArgs []string) s3Target {
	t.Helper()
	tgt := newClusterS3TargetWithExtraArgs(t, 4, extraArgs)
	tgt.name = "cluster-4-dedicated"
	return tgt
}

// Bucket name helper verifies the bucketNameFor helper that derives an
// S3-spec-compliant bucket name from target + test + case.
var _ = ginkgo.Describe("Bucket name helper", ginkgo.Label("bucket"), func() {
	ginkgo.It("derives valid names", func() {
		runBucketNameForCases(ginkgo.GinkgoTB())
	})
})

func runBucketNameForCases(t testing.TB) {
	t.Helper()

	got := bucketNameFor("single", "TestS3FooE2E/SingleNode/Put", "basic")
	gomega.Expect(got).To(gomega.Equal("single-tests3fooe2e-singlenode-put-basic"))
	gomega.Expect(len(got)).To(gomega.BeNumerically("<=", 63))

	long := bucketNameFor("cluster4", "TestS3VersioningE2E/Cluster4Node/ListObjectVersionsWithDeleteMarker", "basic")
	gomega.Expect(len(long)).To(gomega.BeNumerically("<=", 63))
	gomega.Expect(len(long)).To(gomega.BeNumerically(">=", 3))
	gomega.Expect(long).To(gomega.MatchRegexp(`^cluster4-basic-[0-9a-f]{8}$`))
}
