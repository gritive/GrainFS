package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/onsi/ginkgo/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Pull-through cache exercises the pull-through cache layer end-to-end
// against both deployment shapes. The local grainfs (single or 4-node
// cluster, depending on subtest) is the e2e target; an in-test throwaway
// single-node grainfs plays the upstream S3 source. Cache miss on the
// local target pulls bytes from the upstream and serves them as if local;
// the second GET is satisfied from the local cache.
//
// Two cases — both run on both targets:
//   - FetchesFromUpstream: small text payload — basic round-trip.
//   - LargeObject: 5 MiB random payload — exercises the 2-pass streaming
//     fetch path (regression for the original io.ReadAll OOM bug).
//
// Cluster4Node/LargeObject currently fails: cluster pull-through truncates
// or corrupts large payloads. The failure is intentional and surfaces a
// real parity gap with single (single passes the identical case). Tracked
// in TODOS.md → Pull-through Parity Follow-Ups; the failing assertion is
// the regression signal that unblocks closing the gap.
var _ = ginkgo.Describe("Pull-through cache", func() {
	registerTarget := func(name string, newTarget func(testing.TB) s3Target) {
		ginkgo.Context(name, func() {
			var tgt s3Target
			var upstream *pullthroughUpstream

			ginkgo.BeforeEach(func() {
				t := ginkgo.GinkgoTB()
				tgt = newTarget(t)
				upstream = startPullthroughUpstream(t)
			})

			ginkgo.It("fetches from upstream and then serves the cache", func() {
				runPullthroughFetchesFromUpstream(ginkgo.GinkgoTB(), tgt, upstream)
			})

			ginkgo.It("streams and caches a large object", func() {
				runPullthroughLargeObject(ginkgo.GinkgoTB(), tgt, upstream)
			})
		})
	}

	registerTarget("SingleNode", func(t testing.TB) s3Target {
		return newDedicatedSingleNodeS3Target(t, nil)
	})
	registerTarget("Cluster4Node", newSharedClusterS3Target)
})

// pullthroughUpstream is a throwaway single-node grainfs that acts as the
// pull-through source.
type pullthroughUpstream struct {
	endpoint  string
	dataDir   string
	saID      string
	accessKey string
	secretKey string
	client    *s3.Client
}

// startPullthroughUpstream boots a throwaway single-node grainfs and
// returns a handle to it.
func startPullthroughUpstream(t testing.TB) *pullthroughUpstream {
	t.Helper()

	dir, err := os.MkdirTemp("", "grainfs-pullthrough-upstream-")
	require.NoError(t, err)
	ginkgo.DeferCleanup(os.RemoveAll, dir)

	port := freePort()
	cmd := exec.Command(getBinary(), "serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", port),
		"--nfs4-port", fmt.Sprintf("%d", freePort()),
		"--nbd-port", fmt.Sprintf("%d", freePort()),
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
		"--cluster-key", "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899",
	)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start(), "start upstream grainfs")
	ginkgo.DeferCleanup(terminateProcess, cmd)

	require.NoError(t, waitForPortM(port, 30*time.Second))
	endpoint := fmt.Sprintf("http://127.0.0.1:%d", port)
	bootstrap, _ := bootstrapAdminViaUDSAnyResult(t, []string{dir}, 30*time.Second)
	client := s3ClientFor(endpoint, bootstrap.AccessKey, bootstrap.SecretKey)
	require.NoError(t, waitForIAMReady(client, 30*time.Second))

	return &pullthroughUpstream{
		endpoint:  endpoint,
		dataDir:   dir,
		saID:      bootstrap.SAID,
		accessKey: bootstrap.AccessKey,
		secretKey: bootstrap.SecretKey,
		client:    client,
	}
}

// prepareBucket creates the bucket on upstream (matching name to the local
// bucket the case will configure pull-through for) and registers cleanup.
func (u *pullthroughUpstream) prepareBucket(t testing.TB, bucket string) {
	t.Helper()
	createBucketWithAdminPolicyAttachViaUDSAny(t, []string{u.dataDir}, u.saID, bucket, u.client)
	ginkgo.DeferCleanup(func() {
		u.client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	})
}

func runPullthroughFetchesFromUpstream(t testing.TB, tgt s3Target, upstream *pullthroughUpstream) {
	t.Helper()

	bucket := tgt.uniqueBucket(t, "fetch")
	upstream.prepareBucket(t, bucket)

	// Put canonical payload on upstream.
	ctx := context.Background()
	_, err := upstream.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("data/file.txt"),
		Body:   strings.NewReader("upstream-content"),
	})
	require.NoError(t, err)

	// Wire the local bucket to the upstream via admin UDS.
	iamPutBucketUpstream(t, tgt.adminSockPath(), bucket, upstream.endpoint, upstream.accessKey, upstream.secretKey)

	local := tgt.pickNode(0)

	// First GET on the local target — cache miss, must pull from upstream.
	getResp, err := local.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("data/file.txt"),
	})
	require.NoError(t, err, "pull-through GET must succeed")
	ginkgo.DeferCleanup(getResp.Body.Close)
	body, _ := io.ReadAll(getResp.Body)
	assert.Equal(t, "upstream-content", string(body), "pull-through must return upstream content")

	// Second GET — local cache hit, no upstream needed.
	getResp2, err := local.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("data/file.txt"),
	})
	require.NoError(t, err, "cached GET must succeed")
	ginkgo.DeferCleanup(getResp2.Body.Close)
	body2, _ := io.ReadAll(getResp2.Body)
	assert.Equal(t, "upstream-content", string(body2))
}

func runPullthroughLargeObject(t testing.TB, tgt s3Target, upstream *pullthroughUpstream) {
	t.Helper()

	bucket := tgt.uniqueBucket(t, "large")
	upstream.prepareBucket(t, bucket)

	// 5 MiB random payload — exercises the 2-pass streaming fetch
	// path on the local target. Regression for the original
	// io.ReadAll OOM bug. Cluster4Node currently fails this case
	// (parity gap with single tracked in TODOS.md).
	payload := make([]byte, 5*1024*1024)
	_, err := rand.Read(payload)
	require.NoError(t, err)

	ctx := context.Background()
	_, err = upstream.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String("bigfile.bin"),
		Body:          bytes.NewReader(payload),
		ContentLength: aws.Int64(int64(len(payload))),
	})
	require.NoError(t, err)

	iamPutBucketUpstream(t, tgt.adminSockPath(), bucket, upstream.endpoint, upstream.accessKey, upstream.secretKey)

	local := tgt.pickNode(0)

	// First GET: cache miss → streaming pull-through.
	getResp, err := local.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("bigfile.bin"),
	})
	require.NoError(t, err, "pull-through GET must succeed for large object")
	ginkgo.DeferCleanup(getResp.Body.Close)
	got, err := io.ReadAll(getResp.Body)
	require.NoError(t, err)
	assert.Equal(t, payload, got, "pull-through must return bytes-identical content for large object")

	// Second GET: cache hit.
	getResp2, err := local.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("bigfile.bin"),
	})
	require.NoError(t, err, "cached GET must succeed")
	ginkgo.DeferCleanup(getResp2.Body.Close)
	got2, err := io.ReadAll(getResp2.Body)
	require.NoError(t, err)
	assert.Equal(t, payload, got2, "cached object must be bytes-identical to original")
}
