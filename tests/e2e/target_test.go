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
	"sync"
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
	// adminSockPath returns the path to the admin UDS for the "writable"
	// node — node-0 on single, the elected leader on cluster. Tests that
	// drive per-bucket admin config (e.g., pull-through upstream
	// registration) use it; vanilla S3-surface tests can ignore.
	adminSockPath func() string
	isCluster     bool
	cluster       *e2eCluster // non-nil for cluster fixtures
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
		adminSockPath: func() string {
			return testServerDataDir + "/admin.sock"
		},
		isCluster: false,
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

// Shared cluster fixture — process-global, lazily booted on first cluster-
// target test (so -short skips boot automatically by skipping cluster tests).
// Lifetime managed by TestMain teardown via stopSharedCluster.
var (
	sharedClusterOnce sync.Once
	sharedCluster     *e2eCluster
)

func getOrInitSharedCluster(t *testing.T) *e2eCluster {
	t.Helper()
	sharedClusterOnce.Do(func() {
		c := startE2EClusterNoCleanup(t, e2eClusterOptions{
			Nodes:      4,
			Mode:       ClusterModeDynamicJoin,
			ClusterKey: "E2E-S3-OP-SHARED-KEY",
			LogPrefix:  "grainfs-s3op-shared",
			DisableNFS: true,
			// NBD stays enabled so newSharedClusterNBDTarget can reuse this fixture.
		})
		for i := range c.procs {
			iamWaitKeyReady(t, c.httpURLs[i], c.accessKey, c.secretKey, 30*time.Second)
		}
		sharedCluster = c
	})
	if sharedCluster == nil {
		t.Fatal("shared cluster initialization failed")
	}
	return sharedCluster
}

// stopSharedCluster is invoked from TestMain teardown to release the shared
// cluster fixture. No-op when no cluster test triggered initialization.
func stopSharedCluster() {
	if sharedCluster != nil {
		sharedCluster.Stop()
	}
}

func newSharedClusterS3Target(t *testing.T) s3Target {
	t.Helper()
	c := getOrInitSharedCluster(t)
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
		createBkt: func(t *testing.T, bucket string) {
			if c.wildcardAdmin {
				createBucketWithClient(t, c.S3Client(c.leaderIdx), bucket)
			} else {
				createBucketWithAdminPolicyAttachViaUDSAny(t, c.dataDirs, c.saID, bucket, c.S3Client(c.leaderIdx))
			}
		},
		uniqueBucket: func(t *testing.T, caseName string) string {
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
		adminSockPath: func() string {
			return c.dataDirs[c.leaderIdx] + "/admin.sock"
		},
		isCluster: true,
		cluster:   c,
	}
}

// newClusterS3Target returns a DEDICATED (non-shared) cluster fixture. Use
// newSharedClusterS3Target for tests that don't mutate cluster topology;
// reserve this for tests that kill nodes, change CLI flags, or otherwise
// need an isolated cluster.
func newClusterS3Target(t *testing.T, nodes int) s3Target {
	return newClusterS3TargetWithExtraArgs(t, nodes, nil)
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
			if c.wildcardAdmin {
				createBucketWithClient(t, c.S3Client(c.leaderIdx), bucket)
			} else {
				createBucketWithAdminPolicyAttachViaUDSAny(t, c.dataDirs, c.saID, bucket, c.S3Client(c.leaderIdx))
			}
		},
		uniqueBucket: func(t *testing.T, caseName string) string {
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
		adminSockPath: func() string {
			return c.dataDirs[c.leaderIdx] + "/admin.sock"
		},
		isCluster: true,
		cluster:   c,
	}
}

// newDedicatedSingleNodeS3Target boots a per-test single-node grainfs with
// the given extra args. Use this only for tests that need non-default flags
// (e.g. --append-size-cap-bytes, alternate EC profile) — vanilla single-node
// tests should keep using the package-global newSingleNodeS3Target() fixture
// since it amortises one boot across the whole package. Cluster has the same
// dedicated/shared split via newClusterS3Target vs newSharedClusterS3Target;
// this completes the mirror for single.
//
// Lifetime: process is launched on call, terminated + tmpdir removed via
// t.Cleanup. Each call gets its own port + data dir.
func newDedicatedSingleNodeS3Target(t *testing.T, extraArgs []string) s3Target {
	t.Helper()

	dir, err := os.MkdirTemp("", "grainfs-e2e-single-dedicated-")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = os.RemoveAll(dir)
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
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start(), "start single-node grainfs")
	t.Cleanup(func() {
		terminateProcess(cmd)
	})

	require.NoError(t, waitForPortM(port, 30*time.Second), "wait for HTTP port")

	admin, err := bootstrapAdminResultViaUDSForTestMain(dir, 30*time.Second)
	require.NoError(t, err, "bootstrap admin SA via UDS")
	ak, sk := admin.AccessKey, admin.SecretKey
	wildcardAdmin := bootstrapResultHasWildcardAdmin(admin)

	require.NoError(t, patchSnapshotIntervalM(dir, "0s"), "disable auto-snapshot")

	endpoint := fmt.Sprintf("http://127.0.0.1:%d", port)
	client := ecS3Client(endpoint, ak, sk)
	require.NoError(t, waitForIAMReady(client, 30*time.Second), "wait for IAM ready")

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
		createBkt: func(t *testing.T, bucket string) {
			if !wildcardAdmin {
				createBucketWithAdminPolicyAttachViaUDSAny(t, []string{dir}, admin.SAID, bucket, client)
				return
			}
			createBucketWithClient(t, client, bucket)
		},
		uniqueBucket: func(t *testing.T, caseName string) string {
			name := bucketNameFor("single-dedicated", t.Name(), caseName)
			if !wildcardAdmin {
				createBucketWithAdminPolicyAttachViaUDSAny(t, []string{dir}, admin.SAID, name, client)
			} else {
				createBucketWithClient(t, client, name)
			}
			t.Cleanup(func() {
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

// TestBucketNameForE2E verifies the bucketNameFor helper that derives an
// S3-spec-compliant bucket name from target + test + case. Pure helper unit
// check — fixture is not used — but wrapped in the canonical
// SingleNode/Cluster4Node shape for grep/inventory consistency.
func TestBucketNameForE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		_ = newSingleNodeS3Target()
		runBucketNameForCases(t)
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		_ = newSharedClusterS3Target(t)
		runBucketNameForCases(t)
	})
}

func runBucketNameForCases(t *testing.T) {
	t.Helper()

	t.Run("ShortNameRoundtrip", func(t *testing.T) {
		got := bucketNameFor("single", "TestS3FooE2E/SingleNode/Put", "basic")
		require.Equal(t, "single-tests3fooe2e-singlenode-put-basic", got)
		require.LessOrEqual(t, len(got), 63)
	})

	t.Run("LongNameTruncatedWithHash", func(t *testing.T) {
		long := bucketNameFor("cluster4", "TestS3VersioningE2E/Cluster4Node/ListObjectVersionsWithDeleteMarker", "basic")
		require.LessOrEqual(t, len(long), 63)
		require.GreaterOrEqual(t, len(long), 3)
		require.Regexp(t, `^cluster4-basic-[0-9a-f]{8}$`, long)
	})
}
