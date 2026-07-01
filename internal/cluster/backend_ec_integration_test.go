package cluster

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Backend EC object integration", func() {
	var (
		b   *DistributedBackend
		ctx context.Context
	)

	BeforeEach(func() {
		b = newTestDistributedBackend(GinkgoT())
		ctx = context.Background()
		Expect(b.CreateBucket(ctx, "bucket")).To(Succeed())
	})

	configureEC := func(cfg ECConfig) {
		GinkgoHelper()
		nodes := make([]string, cfg.NumShards())
		for i := range nodes {
			nodes[i] = b.selfAddr
		}
		b.SetECConfig(cfg)
		// Reuse the backend's existing keeper so FSM-sealed meta written before
		// this reconfigure (e.g. legacy-convert tests) stays decryptable.
		keeper, clusterID := b.shardSvc.DEKKeeper(), b.shardSvc.ClusterID()
		b.SetShardService(NewShardService(b.root, nil, WithShardDEKKeeper(keeper, clusterID)), nodes)
		wireTestShardGroup(b)
	}

	It("rejects an object key that escapes the shard data root", func() {
		configureEC(ECConfig{DataShards: 2, ParityShards: 1})

		// The shared EC write path (single + cluster) maps key→shard path via
		// getShardDir, which now rejects a key whose ".." segments escape
		// {dataDir}/{bucket}. The physical no-escape guarantee is asserted
		// deterministically in TestWriteLocalShard_RejectsKeyEscapingShardRoot;
		// here we assert the user-facing PutObject surfaces the rejection.
		_, err := b.PutObject(ctx, "bucket", "../../../escape", bytes.NewReader([]byte("malicious")), "application/octet-stream")
		Expect(err).To(HaveOccurred())

		escaped := filepath.Join(filepath.Dir(b.root), "escape")
		_, statErr := os.Stat(escaped)
		Expect(os.IsNotExist(statErr)).To(BeTrue(), "no shard dir may escape the data dir: %s", escaped)
	})

	It("round-trips large parity EC objects via the streaming chunked path", func() {
		configureEC(ECConfig{DataShards: 2, ParityShards: 1})

		payload := bytes.Repeat([]byte("b"), 2<<20)
		body := bytes.NewReader(payload)
		obj, err := b.PutObject(ctx, "bucket", "large-spooled.bin", body, "application/octet-stream")
		Expect(err).NotTo(HaveOccurred())
		Expect(obj.Size).To(Equal(int64(len(payload))))

		rc, gotObj, err := b.GetObject(ctx, "bucket", "large-spooled.bin")
		Expect(err).NotTo(HaveOccurred())
		got, readErr := io.ReadAll(rc)
		closeErr := rc.Close()
		Expect(readErr).NotTo(HaveOccurred())
		Expect(closeErr).NotTo(HaveOccurred())
		Expect(got).To(Equal(payload))
		Expect(gotObj.ETag).To(Equal(obj.ETag))
	})

	It("preserves user metadata for EC objects on the streaming chunked path", func() {
		configureEC(ECConfig{DataShards: 2, ParityShards: 1})

		payload := bytes.Repeat([]byte("a"), 64<<10)
		body := bytes.NewReader(payload)
		obj, err := b.PutObjectWithUserMetadata(
			ctx,
			"bucket",
			"small-meta.bin",
			body,
			"application/octet-stream",
			map[string]string{"x-amz-meta-owner": "me"},
		)
		Expect(err).NotTo(HaveOccurred())
		Expect(obj.UserMetadata).To(Equal(map[string]string{"x-amz-meta-owner": "me"}))

		gotObj, err := b.HeadObject(ctx, "bucket", "small-meta.bin")
		Expect(err).NotTo(HaveOccurred())
		Expect(gotObj.UserMetadata).To(Equal(map[string]string{"x-amz-meta-owner": "me"}))
	})

	// "rejects stale PutObjectMeta expected ETag updates" was removed: the
	// conditional-PUT (ExpectedETag) FSM CAS it exercised is retired under
	// blob-primary — object metadata writes have no raft propose and the only
	// ExpectedETag caller (object relocation) relies on the blob LWW
	// (preserve-old-ModTime), not an FSM CAS (quorum_meta.go / relocate_object.go).
	// The test-only checkPutObjectExpectedETag helper is still unit-tested in
	// isolation in put_object_meta_test.go; there is no blob analogue to assert
	// end-to-end via headObjectMeta (which now reads only blobs).

	// "cleans written shards when EC commit aborts before metadata" was removed
	// with the EC-spooled whole-object write path. The streaming chunked PUT is
	// now the only write path; its commit-abort shard reclamation (Content-MD5
	// mismatch → beforeCommit abort → staged-shard cleanup) is covered by
	// object_put_md5_test.go (TestPutObject_SizedContentMD5_Mismatch_NoLeak via
	// finalShardFileCount, and TestRunChunkedPut_BeforeCommitError_ReclaimsStagedShards).
})
