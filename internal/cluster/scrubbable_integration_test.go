package cluster

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/storage"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ scrubber.Scrubbable = (*DistributedBackend)(nil)
var _ scrubber.ShardIntegrityReader = (*DistributedBackend)(nil)

var _ = Describe("Scrubbable integration", func() {
	var (
		b  *DistributedBackend
		db *badger.DB
	)

	BeforeEach(func() {
		b, db = newTestDistributedBackendWithDB(GinkgoT())
	})

	Describe("object scanning", func() {
		BeforeEach(func() {
			enableECForSpec(b, 2, 1)
		})

		It("emits no records for an empty bucket", func() {
			Expect(b.CreateBucket(context.Background(), "bkt")).To(Succeed())

			ch, err := b.ScanObjects("bkt")
			Expect(err).NotTo(HaveOccurred())
			Expect(scrubDrainObjectRecords(ch)).To(BeEmpty())
		})

		It("rejects a missing bucket", func() {
			_, err := b.ScanObjects("no-such-bucket")
			Expect(err).To(HaveOccurred())
		})

		It("emits a single object record with EC configuration", func() {
			Expect(b.CreateBucket(context.Background(), "bkt")).To(Succeed())
			writeScrubVersionedObjectMeta(b, db, "bkt", "hello.txt", "01HZXYZABC", "etag-hello", 11, nil)

			ch, err := b.ScanObjects("bkt")
			Expect(err).NotTo(HaveOccurred())
			recs := scrubDrainObjectRecords(ch)

			Expect(recs).To(HaveLen(1))
			Expect(recs[0].Bucket).To(Equal("bkt"))
			Expect(recs[0].Key).To(Equal("hello.txt"))
			Expect(recs[0].VersionID).To(Equal("01HZXYZABC"))
			Expect(recs[0].DataShards).To(Equal(2))
			Expect(recs[0].ParityShards).To(Equal(1))
			Expect(recs[0].ETag).To(Equal("etag-hello"))
		})

		It("reports a 1+0 object's own non-redundant EC profile (not the cluster config)", func() {
			// Regression guard: a genesis single-node object is written 1+0; after the
			// cluster grows to 2+1, ScanObjects must still emit ParityShards==0 so the
			// EC-redundancy-upgrade sweep can detect it (the scrub record reports the
			// object's OWN EC profile from its quorum-meta blob, not the cluster config).
			Expect(b.CreateBucket(context.Background(), "bkt")).To(Succeed())
			writeScrubLatestBlob(b, "bkt", "single.txt", PutObjectMetaCmd{
				VersionID: "01SINGLE", Size: 5, ContentType: "application/octet-stream",
				ETag: "etag-single", ModTime: time.Now().Unix(), ECData: 1, ECParity: 0,
				NodeIDs: []string{b.currentSelfAddr()},
			})

			ch, err := b.ScanObjects("bkt")
			Expect(err).NotTo(HaveOccurred())
			recs := scrubDrainObjectRecords(ch)
			Expect(recs).To(HaveLen(1))
			Expect(recs[0].DataShards).To(Equal(1))
			Expect(recs[0].ParityShards).To(Equal(0))
		})

		It("preserves slash-containing keys while scanning", func() {
			Expect(b.CreateBucket(context.Background(), "bkt")).To(Succeed())
			writeScrubVersionedObjectMeta(b, db, "bkt", "folder/nested/file.bin", "01A", "etag-a", 100, nil)
			writeScrubVersionedObjectMeta(b, db, "bkt", "top.txt", "01B", "etag-b", 7, nil)

			ch, err := b.ScanObjects("bkt")
			Expect(err).NotTo(HaveOccurred())
			recs := scrubDrainObjectRecords(ch)

			Expect(recs).To(HaveLen(2))
			keys := map[string]bool{}
			for _, r := range recs {
				keys[r.Key] = true
			}
			Expect(keys).To(HaveKey("folder/nested/file.bin"))
			Expect(keys).To(HaveKey("top.txt"))
		})

		It("skips tombstones while scanning", func() {
			Expect(b.CreateBucket(context.Background(), "bkt")).To(Succeed())
			writeScrubVersionedObjectMeta(b, db, "bkt", "alive", "01A", "etag-alive", 5, nil)
			writeScrubVersionedObjectMeta(b, db, "bkt", "dead", "01D", deleteMarkerETag, 0, nil)

			ch, err := b.ScanObjects("bkt")
			Expect(err).NotTo(HaveOccurred())
			recs := scrubDrainObjectRecords(ch)

			Expect(recs).To(HaveLen(1))
			Expect(recs[0].Key).To(Equal("alive"))
		})
	})

	Describe("object existence", func() {
		It("reports only live objects as existing", func() {
			Expect(b.CreateBucket(context.Background(), "bkt")).To(Succeed())
			// Non-versioned: latest-only blob is the HeadObject authority.
			writeScrubLatestBlob(b, "bkt", "present", PutObjectMetaCmd{
				ETag: "etag", Size: 10, NodeIDs: []string{b.currentSelfAddr()},
			})
			// A latest-only delete-marker tombstone → HeadObject 404 → not existing.
			writeScrubLatestBlob(b, "bkt", "tomb", PutObjectMetaCmd{
				ETag: deleteMarkerETag, IsDeleteMarker: true, NodeIDs: []string{b.currentSelfAddr()},
			})

			ok, err := b.ObjectExists("bkt", "present")
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeTrue())

			ok, err = b.ObjectExists("bkt", "missing")
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeFalse())

			ok, err = b.ObjectExists("bkt", "tomb")
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeFalse())

			ok, err = b.ObjectExists("no-such-bucket", "k")
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeFalse())
		})
	})

	Describe("shard paths and integrity", func() {
		It("returns versioned shard paths", func() {
			paths := b.ShardPaths("bkt", "objects/big.bin", "01HZXYZ", 6)

			Expect(paths).To(HaveLen(6))
			for i, p := range paths {
				Expect(p).To(Equal(filepath.Join(b.root, "shards", "bkt", "objects/big.bin", "01HZXYZ", fmt.Sprintf("shard_%d", i))))
			}
		})

		It("matches the shard service layout", func() {
			keeper, clusterID := testDEKKeeper(GinkgoT())
			svc := NewShardService(b.root, nil, WithShardDEKKeeper(keeper, clusterID))
			Expect(svc.WriteLocalShard("bkt", "key/01VID", 3, []byte("payload"))).To(Succeed())

			paths := b.ShardPaths("bkt", "key", "01VID", 4)
			_, err := os.Stat(paths[3])
			Expect(err).NotTo(HaveOccurred())
			decoded, err := svc.ReadLocalShard("bkt", "key/01VID", 3)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(decoded)).To(Equal("payload"))
		})

		It("uses the shared shard service root", func() {
			shardRoot := GinkgoT().TempDir()
			keeper, clusterID := testDEKKeeper(GinkgoT())
			svc := NewShardService(shardRoot, nil, WithShardDEKKeeper(keeper, clusterID))
			b.SetShardService(svc, []string{"test-node"})
			Expect(svc.WriteLocalShard("bkt", "key/01VID", 0, []byte("payload"))).To(Succeed())

			paths := b.ShardPaths("bkt", "key", "01VID", 1)
			_, err := os.Stat(paths[0])
			Expect(err).NotTo(HaveOccurred())
			decoded, err := svc.ReadLocalShard("bkt", "key/01VID", 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(decoded)).To(Equal("payload"))
		})

		It("round-trips shard writes and reads", func() {
			path := b.ShardPaths("bkt", "k", "01VID", 1)[0]
			payload := []byte("ec-shard-payload-0x42")

			Expect(b.WriteShard("bkt", "k", "01VID", 0, path, payload)).To(Succeed())
			got, err := b.ReadShard("bkt", "k", "01VID", 0, path)
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(payload))
		})

		It("verifies encoded shard integrity", func() {
			path := b.ShardPaths("bkt", "k", "01VID", 1)[0]
			payload := []byte("encoded-payload")
			Expect(b.WriteShard("bkt", "k", "01VID", 0, path, payload)).To(Succeed())

			got, err := b.ReadShardIntegrity("bkt", "k", "01VID", 0, path)
			Expect(err).NotTo(HaveOccurred())
			Expect(got.Status).To(Equal(scrubber.ShardIntegrityVerified))
			Expect(got.Payload).To(Equal(payload))
		})

		It("verifies encrypted shard-service shards", func() {
			keeper, clusterID := testDEKKeeper(GinkgoT())
			svc := NewShardService(GinkgoT().TempDir(), nil, WithShardDEKKeeper(keeper, clusterID))
			b.SetShardService(svc, []string{"test-node"})
			Expect(svc.WriteLocalShard("bkt", "key/01VID", 0, []byte("payload"))).To(Succeed())

			path := b.ShardPaths("bkt", "key", "01VID", 1)[0]
			got, err := b.ReadShardIntegrity("bkt", "key", "01VID", 0, path)
			Expect(err).NotTo(HaveOccurred())
			Expect(got.Status).To(Equal(scrubber.ShardIntegrityVerified))
			Expect(string(got.Payload)).To(Equal("payload"))
		})

		It("rejects shards outside a data dir (non-data-dir read fail-closed)", func() {
			// A shard reaching the non-data-dir fallback can carry no canonical key
			// binding, so it must be rejected fail-closed rather than handed back
			// as plaintext/unverified-legacy. Repaired shards now always live under
			// a shard data dir and are GFSENC3.
			path := filepath.Join(GinkgoT().TempDir(), "shard_0")
			Expect(os.WriteFile(path, []byte("legacy-raw-payload"), 0o600)).To(Succeed())

			_, err := b.ReadShardIntegrity("bkt", "k", "01VID", 0, path)
			Expect(err).To(HaveOccurred())

			_, err = b.ReadShard("bkt", "k", "01VID", 0, path)
			Expect(err).To(HaveOccurred())
		})

		It("overwrites shards atomically without leftover temp files", func() {
			path := b.ShardPaths("bkt", "k", "01VID", 1)[0]
			dir := filepath.Dir(path)

			Expect(b.WriteShard("bkt", "k", "01VID", 0, path, []byte("v1"))).To(Succeed())
			Expect(b.WriteShard("bkt", "k", "01VID", 0, path, []byte("v2"))).To(Succeed())

			got, err := b.ReadShard("bkt", "k", "01VID", 0, path)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(got)).To(Equal("v2"))

			entries, err := os.ReadDir(dir)
			Expect(err).NotTo(HaveOccurred())
			for _, entry := range entries {
				Expect(strings.HasSuffix(entry.Name(), ".tmp")).To(BeFalse(), "leftover tmp: %s", entry.Name())
			}
		})

		It("errors for missing shard files", func() {
			_, err := b.ReadShard("bkt", "k", "01VID", 0, filepath.Join(GinkgoT().TempDir(), "nope"))
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("grouped object scanning", func() {
		It("emits no groups for an empty bucket", func() {
			Expect(b.CreateBucket(context.Background(), "bkt")).To(Succeed())

			ch, err := b.ScanObjectsGrouped("bkt")
			Expect(err).NotTo(HaveOccurred())
			Expect(scrubDrainObjectKeyGroups(ch)).To(BeEmpty())
		})

		It("rejects a missing bucket", func() {
			_, err := b.ScanObjectsGrouped("no-such-bucket")
			Expect(err).To(HaveOccurred())
		})

		It("groups versions by key in key order", func() {
			Expect(b.CreateBucket(context.Background(), "bkt")).To(Succeed())
			Expect(b.SetBucketVersioning("bkt", "Enabled")).To(Succeed())
			self := b.currentSelfAddr()
			// Per-version blobs (versioning-enabled authority). ModTime ascending
			// mirrors VID order so the ModTime-primary derive is deterministic.
			writeScrubVersionBlob(b, "bkt", "a", "01AAAA0001", PutObjectMetaCmd{ETag: "etag-a-v1", Size: 11, ModTime: 1, NodeIDs: []string{self}})
			writeScrubVersionBlob(b, "bkt", "a", "01AAAA0002", PutObjectMetaCmd{ETag: "etag-a-v2", Size: 22, ModTime: 2, NodeIDs: []string{self}})
			writeScrubVersionBlob(b, "bkt", "b", "01BBBB0001", PutObjectMetaCmd{ETag: "etag-b-v1", Size: 33, ModTime: 1, NodeIDs: []string{self}})

			ch, err := b.ScanObjectsGrouped("bkt")
			Expect(err).NotTo(HaveOccurred())
			groups := scrubDrainObjectKeyGroups(ch)

			Expect(groups).To(HaveLen(2))
			Expect(groups[0].Bucket).To(Equal("bkt"))
			Expect(groups[0].Key).To(Equal("a"))
			Expect(groups[0].Versions).To(HaveLen(2))
			Expect(groups[0].Versions[0].VersionID).To(Equal("01AAAA0002"))
			Expect(groups[0].Versions[0].ETag).To(Equal("etag-a-v2"))
			Expect(groups[0].Versions[0].IsLatest).To(BeTrue())
			Expect(groups[0].Versions[1].VersionID).To(Equal("01AAAA0001"))
			Expect(groups[0].Versions[1].IsLatest).To(BeFalse())

			Expect(groups[1].Key).To(Equal("b"))
			Expect(groups[1].Versions).To(HaveLen(1))
			Expect(groups[1].Versions[0].VersionID).To(Equal("01BBBB0001"))
		})

		It("passes delete markers through grouped scans", func() {
			Expect(b.CreateBucket(context.Background(), "bkt")).To(Succeed())
			Expect(b.SetBucketVersioning("bkt", "Enabled")).To(Succeed())
			self := b.currentSelfAddr()
			writeScrubVersionBlob(b, "bkt", "k", "01AAAA0001", PutObjectMetaCmd{ETag: "etag-live", Size: 10, ModTime: 1, NodeIDs: []string{self}})
			writeScrubVersionBlob(b, "bkt", "k", "01AAAA0002", PutObjectMetaCmd{ETag: deleteMarkerETag, IsDeleteMarker: true, ModTime: 2, NodeIDs: []string{self}})

			ch, err := b.ScanObjectsGrouped("bkt")
			Expect(err).NotTo(HaveOccurred())
			groups := scrubDrainObjectKeyGroups(ch)

			Expect(groups).To(HaveLen(1))
			Expect(groups[0].Versions).To(HaveLen(2))
			Expect(groups[0].Versions[0].IsDeleteMarker).To(BeTrue())
			Expect(groups[0].Versions[1].IsDeleteMarker).To(BeFalse())
		})

		It("preserves tags in grouped scans", func() {
			Expect(b.CreateBucket(context.Background(), "bkt")).To(Succeed())
			Expect(b.SetBucketVersioning("bkt", "Enabled")).To(Succeed())
			tags := []storage.Tag{{Key: "env", Value: "prod"}, {Key: "team", Value: "data"}}
			writeScrubVersionBlob(b, "bkt", "tagged", "01AAAA0001", PutObjectMetaCmd{
				ETag: "etag", Size: 5, Tags: tags, ModTime: 1, NodeIDs: []string{b.currentSelfAddr()},
			})

			ch, err := b.ScanObjectsGrouped("bkt")
			Expect(err).NotTo(HaveOccurred())
			groups := scrubDrainObjectKeyGroups(ch)

			Expect(groups).To(HaveLen(1))
			Expect(groups[0].Versions).To(HaveLen(1))
			Expect(groups[0].Versions[0].Tags).To(Equal(tags))
		})

		It("includes legacy unversioned objects", func() {
			Expect(b.CreateBucket(context.Background(), "bkt")).To(Succeed())
			tags := []storage.Tag{{Key: "expire", Value: "yes"}}
			// Non-versioned object: latest-only blob is the authority.
			writeScrubLatestBlob(b, "bkt", "folder/tagged", PutObjectMetaCmd{
				ETag: "etag", Size: 42, Tags: tags, NodeIDs: []string{b.currentSelfAddr()},
			})

			ch, err := b.ScanObjectsGrouped("bkt")
			Expect(err).NotTo(HaveOccurred())
			groups := scrubDrainObjectKeyGroups(ch)

			Expect(groups).To(HaveLen(1))
			Expect(groups[0].Key).To(Equal("folder/tagged"))
			Expect(groups[0].Versions).To(HaveLen(1))
			Expect(groups[0].Versions[0].VersionID).To(BeEmpty())
			Expect(groups[0].Versions[0].IsLatest).To(BeTrue())
			Expect(groups[0].Versions[0].Size).To(Equal(int64(42)))
			Expect(groups[0].Versions[0].Tags).To(Equal(tags))
		})
	})

	Describe("local multipart upload scanning", func() {
		It("emits no records for an empty bucket", func() {
			Expect(b.CreateBucket(context.Background(), "bkt")).To(Succeed())

			ch, err := b.ScanLocalMultipartUploads("bkt")
			Expect(err).NotTo(HaveOccurred())
			Expect(scrubDrainMultipartRecords(ch)).To(BeEmpty())
		})

		It("rejects a missing bucket", func() {
			_, err := b.ScanLocalMultipartUploads("no-such-bucket")
			Expect(err).To(HaveOccurred())
		})

		It("emits active uploads for the requested bucket", func() {
			Expect(b.CreateBucket(context.Background(), "bkt")).To(Succeed())
			Expect(b.CreateBucket(context.Background(), "other")).To(Succeed())

			writeScrubMultipartMeta(b, db, "upload-aaa", clusterMultipartMeta{
				Bucket: "bkt", Key: "a.bin", ContentType: "application/octet-stream", CreatedAt: 100,
			})
			writeScrubMultipartMeta(b, db, "upload-bbb", clusterMultipartMeta{
				Bucket: "bkt", Key: "b.bin", ContentType: "application/octet-stream", CreatedAt: 200,
			})
			writeScrubMultipartMeta(b, db, "upload-other", clusterMultipartMeta{
				Bucket: "other", Key: "x.bin", ContentType: "application/octet-stream", CreatedAt: 300,
			})

			ch, err := b.ScanLocalMultipartUploads("bkt")
			Expect(err).NotTo(HaveOccurred())
			recs := scrubDrainMultipartRecords(ch)

			Expect(recs).To(HaveLen(2))
			got := map[string]storage.MultipartUploadRecord{}
			for _, r := range recs {
				got[r.UploadID] = r
			}
			Expect(got).To(HaveKey("upload-aaa"))
			Expect(got["upload-aaa"].Bucket).To(Equal("bkt"))
			Expect(got["upload-aaa"].Key).To(Equal("a.bin"))
			Expect(got["upload-aaa"].InitiatedAt).To(Equal(int64(100)))
			Expect(got).To(HaveKey("upload-bbb"))
			Expect(got["upload-bbb"].Key).To(Equal("b.bin"))
			Expect(got["upload-bbb"].InitiatedAt).To(Equal(int64(200)))
		})
	})
})

func writeScrubVersionedObjectMeta(b *DistributedBackend, _ *badger.DB, bucket, key, versionID, etag string, size int64, tags []storage.Tag) {
	GinkgoHelper()
	// Object metadata lives in the off-raft quorum-meta blob store. These specs use
	// non-versioned buckets, so seed the latest-only blob (the off-path scrub
	// source). Record a real 2+1 EC profile so the object represents an EC object
	// (matches enableECForSpec(b, 2, 1)); a delete-marker keeps no shards, so leave
	// its EC profile zero.
	var ecData, ecParity uint8 = 2, 1
	if etag == deleteMarkerETag {
		ecData, ecParity = 0, 0
	}
	writeScrubLatestBlob(b, bucket, key, PutObjectMetaCmd{
		VersionID:      versionID,
		Size:           size,
		ContentType:    "application/octet-stream",
		ETag:           etag,
		ModTime:        time.Now().Unix(),
		Tags:           tags,
		ECData:         ecData,
		ECParity:       ecParity,
		IsDeleteMarker: etag == deleteMarkerETag,
		NodeIDs:        []string{b.currentSelfAddr()},
	})
}

// writeScrubVersionBlob seeds a per-version quorum-meta blob (the live
// versioning-enabled authority). Ginkgo-native twin of seedVersionBlob.
func writeScrubVersionBlob(b *DistributedBackend, bucket, key, versionID string, cmd PutObjectMetaCmd) {
	GinkgoHelper()
	cmd.Bucket = bucket
	cmd.Key = key
	cmd.VersionID = versionID
	blob, err := encodeQuorumMetaBlob(cmd)
	Expect(err).NotTo(HaveOccurred())
	Expect(b.shardSvc.writeQuorumMetaVersionLocal(bucket, filepath.Join(key, versionID), blob)).To(Succeed())
}

// writeScrubLatestBlob seeds a latest-only quorum-meta blob (the non-versioned
// object authority). Ginkgo-native twin of seedLatestBlob.
func writeScrubLatestBlob(b *DistributedBackend, bucket, key string, cmd PutObjectMetaCmd) {
	GinkgoHelper()
	cmd.Bucket = bucket
	cmd.Key = key
	blob, err := encodeQuorumMetaBlob(cmd)
	Expect(err).NotTo(HaveOccurred())
	Expect(b.shardSvc.writeQuorumMetaLocal(bucket, key, blob)).To(Succeed())
}

func writeScrubMultipartMeta(b *DistributedBackend, _ *badger.DB, uploadID string, meta clusterMultipartMeta) {
	GinkgoHelper()
	// M2b: ScanLocalMultipartUploads walks the local .qmeta_mpu manifest replicas,
	// so seed the manifest blob (not the FSM mpu: key).
	raw, err := marshalClusterMultipartMeta(meta)
	Expect(err).NotTo(HaveOccurred())
	Expect(b.shardSvc.writeManifestBlobLocal(meta.Bucket, uploadID, raw)).To(Succeed())
}

func scrubDrainObjectRecords(ch <-chan scrubber.ObjectRecord) []scrubber.ObjectRecord {
	GinkgoHelper()
	var out []scrubber.ObjectRecord
	timeout := time.After(2 * time.Second)
	for {
		select {
		case rec, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, rec)
		case <-timeout:
			Fail("ObjectRecord channel did not close within timeout")
		}
	}
}

func scrubDrainObjectKeyGroups(ch <-chan storage.ObjectKeyGroup) []storage.ObjectKeyGroup {
	GinkgoHelper()
	var out []storage.ObjectKeyGroup
	timeout := time.After(2 * time.Second)
	for {
		select {
		case group, ok := <-ch:
			if !ok {
				return out
			}
			versions := append([]storage.ObjectVersionRecord(nil), group.Versions...)
			out = append(out, storage.ObjectKeyGroup{Bucket: group.Bucket, Key: group.Key, Versions: versions})
		case <-timeout:
			Fail("ObjectKeyGroup channel did not close within timeout")
		}
	}
}

func drainObjectKeyGroups(t clusterTestTB, ch <-chan storage.ObjectKeyGroup) []storage.ObjectKeyGroup {
	t.Helper()
	var out []storage.ObjectKeyGroup
	timeout := time.After(2 * time.Second)
	for {
		select {
		case group, ok := <-ch:
			if !ok {
				return out
			}
			versions := append([]storage.ObjectVersionRecord(nil), group.Versions...)
			out = append(out, storage.ObjectKeyGroup{Bucket: group.Bucket, Key: group.Key, Versions: versions})
		case <-timeout:
			t.Fatalf("ObjectKeyGroup channel did not close within timeout")
		}
	}
}

func scrubDrainMultipartRecords(ch <-chan storage.MultipartUploadRecord) []storage.MultipartUploadRecord {
	GinkgoHelper()
	var out []storage.MultipartUploadRecord
	timeout := time.After(2 * time.Second)
	for {
		select {
		case rec, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, rec)
		case <-timeout:
			Fail("MultipartUploadRecord channel did not close within timeout")
		}
	}
}

func enableECForSpec(b *DistributedBackend, k, m int) {
	GinkgoHelper()
	b.SetECConfig(ECConfig{DataShards: k, ParityShards: m})
	nodes := make([]string, 0, k+m)
	for i := 0; i < k+m; i++ {
		nodes = append(nodes, fmt.Sprintf("node-%d", i))
	}
	b.allNodes = nodes
	Expect(b.ECActive()).To(BeTrue())
}
