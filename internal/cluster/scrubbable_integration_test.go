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
			// EC-redundancy-upgrade sweep can detect it. Previously the FSM lat: branch
			// reported the cluster config (2+1), silently hiding versioned 1+0 objects.
			Expect(b.CreateBucket(context.Background(), "bkt")).To(Succeed())
			meta, err := marshalObjectMeta(objectMeta{
				Key: "single.txt", Size: 5, ContentType: "application/octet-stream",
				ETag: "etag-single", LastModified: time.Now().Unix(), ECData: 1, ECParity: 0,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(db.Update(func(txn *badger.Txn) error {
				if err := txn.Set(objectMetaKeyV("bkt", "single.txt", "01SINGLE"), meta); err != nil {
					return err
				}
				return txn.Set(latestKey("bkt", "single.txt"), []byte("01SINGLE"))
			})).To(Succeed())

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
			writeScrubVersionedObjectMeta(b, db, "bkt", "present", "01A", "etag", 10, nil)
			writeScrubVersionedObjectMeta(b, db, "bkt", "tomb", "01B", deleteMarkerETag, 0, nil)

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
			svc := NewShardService(b.root, nil, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(GinkgoT(), keeper, clusterID))
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
			svc := NewShardService(shardRoot, nil, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(GinkgoT(), keeper, clusterID))
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
			svc := NewShardService(GinkgoT().TempDir(), nil, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(GinkgoT(), keeper, clusterID))
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
			writeScrubVersionedObjectMeta(b, db, "bkt", "a", "01AAAA0001", "etag-a-v1", 11, nil)
			writeScrubVersionedObjectMeta(b, db, "bkt", "a", "01AAAA0002", "etag-a-v2", 22, nil)
			writeScrubVersionedObjectMeta(b, db, "bkt", "b", "01BBBB0001", "etag-b-v1", 33, nil)

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
			writeScrubVersionedObjectMeta(b, db, "bkt", "k", "01AAAA0001", "etag-live", 10, nil)
			writeScrubVersionedObjectMeta(b, db, "bkt", "k", "01AAAA0002", deleteMarkerETag, 0, nil)

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
			tags := []storage.Tag{{Key: "env", Value: "prod"}, {Key: "team", Value: "data"}}
			writeScrubVersionedObjectMeta(b, db, "bkt", "tagged", "01AAAA0001", "etag", 5, tags)

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
			writeScrubLegacyObjectMeta(b, db, "bkt", "folder/tagged", "etag", 42, tags)

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

func writeScrubVersionedObjectMeta(b *DistributedBackend, db *badger.DB, bucket, key, versionID, etag string, size int64, tags []storage.Tag) {
	GinkgoHelper()
	// Record a real 2+1 EC profile on the meta: ScanObjects now reports the
	// object's OWN EC profile (meta.ECData/ECParity), not the cluster config, so a
	// synthetic fixture must carry it to represent an EC object (matches the
	// enableECForSpec(b, 2, 1) used across these specs). A delete-marker keeps no
	// shards, so leave its EC profile zero.
	var ecData, ecParity uint8 = 2, 1
	if etag == deleteMarkerETag {
		ecData, ecParity = 0, 0
	}
	meta, err := marshalObjectMeta(objectMeta{
		Key:          key,
		Size:         size,
		ContentType:  "application/octet-stream",
		ETag:         etag,
		LastModified: time.Now().Unix(),
		Tags:         tags,
		ECData:       ecData,
		ECParity:     ecParity,
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(objectMetaKey(bucket, key), meta); err != nil {
			return err
		}
		if err := txn.Set(objectMetaKeyV(bucket, key, versionID), meta); err != nil {
			return err
		}
		return txn.Set(latestKey(bucket, key), []byte(versionID))
	})).To(Succeed())
}

func writeScrubLegacyObjectMeta(b *DistributedBackend, db *badger.DB, bucket, key, etag string, size int64, tags []storage.Tag) {
	GinkgoHelper()
	meta, err := marshalObjectMeta(objectMeta{
		Key:          key,
		Size:         size,
		ContentType:  "application/octet-stream",
		ETag:         etag,
		LastModified: time.Now().Unix(),
		Tags:         tags,
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(db.Update(func(txn *badger.Txn) error {
		return txn.Set(objectMetaKey(bucket, key), meta)
	})).To(Succeed())
}

func writeScrubMultipartMeta(b *DistributedBackend, db *badger.DB, uploadID string, meta clusterMultipartMeta) {
	GinkgoHelper()
	raw, err := marshalClusterMultipartMeta(meta)
	Expect(err).NotTo(HaveOccurred())
	Expect(db.Update(func(txn *badger.Txn) error {
		return txn.Set(b.ks().MultipartKey(uploadID), raw)
	})).To(Succeed())
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
